# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, BooleanType, TimestampType
from pyspark.sql.window import Window
import json

# COMMAND ----------

# DBTITLE 1, Configuration
catalog = spark.conf.get("catalog", "ironclad_hr")
quality_rules_path = spark.conf.get(
    "quality_rules_path",
    "/Workspace/Users/benjaminstringer1994@gmail.com/ironclad-hr-notebooks/dev/config/quality_rules.json"
)

# COMMAND ----------

# DBTITLE 1, Load quality rules from config
# Config-driven expectations: business rules live in quality_rules.json.
# Pipeline code never changes when rules change — only the JSON file does.
with open(quality_rules_path, "r") as f:
    _quality_rules = json.load(f)

def get_entity_rules(entity):
    """Return list of {field, rule, severity} dicts for a given entity."""
    return _quality_rules.get("entities", {}).get(entity, [])

def get_critical_rules(entity):
    return [r for r in get_entity_rules(entity) if r["severity"] == "CRITICAL"]

def get_error_rules(entity):
    return [r for r in get_entity_rules(entity) if r["severity"] == "ERROR"]

def get_warning_rules(entity):
    return [r for r in get_entity_rules(entity) if r["severity"] == "WARNING"]

print(f"Quality rules loaded from: {quality_rules_path}")
for entity, rules in _quality_rules.get("entities", {}).items():
    print(f"  {entity}: {len(rules)} rules")

# COMMAND ----------

# DBTITLE 1, Helper — most-recent-period dedup
# In production, the pipeline runs once per period. Each run, Bronze has all
# historical periods. This helper returns only the most recent period's rows
# per natural key — so AUTO CDC FROM SNAPSHOT sees the current period's
# snapshot and correctly computes SCD diffs against its prior state.
def latest_period_per_key(df, key_cols):
    """
    Deduplicate to the most recent period per key.
    Uses _source_file_name to extract the period (YYYY-MM from file path)
    then takes row_number() over key ordered by period desc.
    Falls back to _ingested_at ordering when file paths are not structured.
    """
    w = Window.partitionBy(*key_cols).orderBy(
        F.regexp_extract(F.col("_source_file_name"), r"/(\d{4}-\d{2})/", 1).desc(),
        F.col("_ingested_at").desc()
    )
    return (
        df.withColumn("_row_num", F.row_number().over(w))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )

# COMMAND ----------

# DBTITLE 1, Helper — build data_quality_log entries for a violation
def build_dq_entries(df, entity, field, rule, severity, actual_value_col=None):
    """
    Produces rows for the data_quality_log table.
    Includes all columns specified in the master reference schema.
    """
    actual_val = (
        F.col(actual_value_col).cast(StringType())
        if actual_value_col else F.lit(None).cast(StringType())
    )
    return (
        df
        .withColumn("entity", F.lit(entity))
        .withColumn("employee_id",
            F.col("employee_id").cast(StringType())
            if "employee_id" in df.columns
            else F.lit(None).cast(StringType())
        )
        .withColumn("period",
            F.regexp_extract(F.col("_source_file_name"), r"/(\d{4}-\d{2})/", 1)
            if "_source_file_name" in df.columns
            else F.lit(None).cast(StringType())
        )
        .withColumn("field", F.lit(field))
        .withColumn("rule", F.lit(rule))
        .withColumn("severity", F.lit(severity))
        .withColumn("actual_value", actual_val)
        .withColumn("requires_review", F.lit(True))
        .withColumn("resolved", F.lit(False))
        .withColumn("resolved_by", F.lit(None).cast(StringType()))
        .withColumn("resolved_at", F.lit(None).cast(TimestampType()))
        .withColumn("resolution_type", F.lit(None).cast(StringType()))
        .select(
            "_ingested_at", "_source_file_name",
            "entity", "employee_id", "period",
            "field", "rule", "severity",
            "actual_value", "requires_review", "resolved",
            "resolved_by", "resolved_at", "resolution_type"
        )
    )

# COMMAND ----------

# DBTITLE 1, dim_date — static dimension, built once via append_flow
dp.create_streaming_table(
    name="dim_date",
    comment="Static date dimension covering 2018-2030. Built once.",
    table_properties={"quality": "silver"}
)

@dp.append_flow(target="dim_date", once=True, name="dim_date_load")
def load_dim_date():
    from pyspark.sql import Row
    from datetime import date, timedelta
    start = date(2018, 1, 1)
    end = date(2030, 12, 31)
    rows = []
    current = start
    while current <= end:
        fiscal_year = current.year if current.month <= 9 else current.year + 1
        fiscal_period = ((current.month + 2) % 12) + 1
        rows.append(Row(
            date_id=current.strftime("%Y%m%d"),
            calendar_date=current.strftime("%Y-%m-%d"),
            year=current.year,
            month=current.month,
            quarter=((current.month - 1) // 3) + 1,
            fiscal_year=fiscal_year,
            fiscal_period=fiscal_period,
            is_working_day=(current.weekday() < 5),
            day_of_week=current.strftime("%A")
        ))
        current += timedelta(days=1)
    return spark.createDataFrame(rows)

# COMMAND ----------

# DBTITLE 1, dim_department source view
@dp.view
def v_bronze_departments():
    """
    Current-period snapshot of departments.
    parent_department_id arrives in _rescued_data due to null values —
    exposed explicitly as null to maintain schema contract.
    """
    return (
        spark.table(f"{catalog}.bronze.bronze_departments")
        .filter(F.col("department_id").isNotNull())
        .transform(lambda df: latest_period_per_key(df, ["department_id"]))
        .select(
            "department_id", "name", "cost_centre",
            F.lit(None).cast(StringType()).alias("parent_department_id")
        )
    )

# COMMAND ----------

# DBTITLE 1, dim_department — SCD Type 2
dp.create_streaming_table(
    name="dim_department",
    comment="Department dimension. SCD Type 2.",
    table_properties={"quality": "silver"}
)
dp.create_auto_cdc_from_snapshot_flow(
    target="dim_department",
    source="v_bronze_departments",
    keys=["department_id"],
    stored_as_scd_type=2
)

# COMMAND ----------

# DBTITLE 1, dim_job_role source view
@dp.view
def v_bronze_job_roles():
    """Current-period snapshot of job roles."""
    return (
        spark.table(f"{catalog}.bronze.bronze_job_roles")
        .filter(F.col("role_id").isNotNull())
        .transform(lambda df: latest_period_per_key(df, ["role_id"]))
        .select("role_id", "title", "band", "function", "job_family")
    )

# COMMAND ----------

# DBTITLE 1, dim_job_role — SCD Type 2
dp.create_streaming_table(
    name="dim_job_role",
    comment="Job role dimension. SCD Type 2.",
    table_properties={"quality": "silver"}
)
dp.create_auto_cdc_from_snapshot_flow(
    target="dim_job_role",
    source="v_bronze_job_roles",
    keys=["role_id"],
    stored_as_scd_type=2
)

# COMMAND ----------

# DBTITLE 1, dim_employee source view
@dp.view
def v_bronze_employees():
    """
    Current-period employee snapshot.
    Config-driven expectations applied here:
    - CRITICAL: null employee_id rows are dropped (logged in data_quality_log)
    - ERROR: termination_date before hire_date → field nulled, row retained
    Dedup ensures one row per employee_id from the most recent period.
    """
    df = spark.table(f"{catalog}.bronze.bronze_employees")

    # Apply CRITICAL filter: drop null employee_id
    df = df.filter(F.col("employee_id").isNotNull())

    # Dedup to current period per employee
    df = latest_period_per_key(df, ["employee_id"])

    # Apply ERROR rule: null out invalid termination_date
    df = df.withColumn(
        "termination_date",
        F.when(
            F.col("termination_date").isNotNull() &
            (F.col("termination_date") < F.col("hire_date")),
            F.lit(None)
        ).otherwise(F.col("termination_date"))
    )

    return df.select(
        "employee_id", "full_name", "email", "gender",
        "hire_date", "termination_date",
        "department_id", "role_id", "manager_id", "nationality"
    )

# COMMAND ----------

# DBTITLE 1, dim_employee — SCD Type 2
dp.create_streaming_table(
    name="dim_employee",
    comment="Employee dimension. SCD Type 2.",
    table_properties={"quality": "silver"}
)
dp.create_auto_cdc_from_snapshot_flow(
    target="dim_employee",
    source="v_bronze_employees",
    keys=["employee_id"],
    stored_as_scd_type=2
)

# COMMAND ----------

# DBTITLE 1, dim_contract source view
@dp.view
def v_bronze_contracts():
    """
    Current-period contract snapshot.
    Composite natural key: employee_id + contract_type + start_date.
    end_date arrives in _rescued_data — exposed as null.
    """
    return (
        spark.table(f"{catalog}.bronze.bronze_contracts")
        .filter(
            F.col("employee_id").isNotNull() &
            F.col("contract_type").isNotNull() &
            F.col("start_date").isNotNull()
        )
        .transform(lambda df: latest_period_per_key(
            df, ["employee_id", "contract_type", "start_date"]
        ))
        .select(
            "contract_id", "employee_id", "contract_type", "start_date",
            F.lit(None).cast(StringType()).alias("end_date")
        )
    )

# COMMAND ----------

# DBTITLE 1, dim_contract — SCD Type 2, composite key
dp.create_streaming_table(
    name="dim_contract",
    comment="Contract dimension. SCD Type 2. Composite key: employee_id + contract_type + start_date.",
    table_properties={"quality": "silver"}
)
dp.create_auto_cdc_from_snapshot_flow(
    target="dim_contract",
    source="v_bronze_contracts",
    keys=["employee_id", "contract_type", "start_date"],
    stored_as_scd_type=2
)

# COMMAND ----------

# DBTITLE 1, fact_headcount — monthly snapshot with tenure
@dp.table(
    name="fact_headcount",
    comment="Monthly headcount snapshot. One row per employee per period. Includes tenure_days and tenure_band.",
    table_properties={"quality": "silver"}
)
def fact_headcount():
    return (
        spark.table(f"{catalog}.bronze.bronze_employees")
        .filter(F.col("employee_id").isNotNull())
        .withColumn("period",
            F.regexp_extract(F.col("_source_file_name"), r"/(\d{4}-\d{2})/", 1)
        )
        .withColumn("period_date",
            F.to_date(
                F.regexp_extract(F.col("_source_file_name"), r"/(\d{4}-\d{2})/", 1),
                "yyyy-MM"
            )
        )
        .withColumn("hire_date_parsed",
            F.to_date(F.substring(F.col("hire_date"), 1, 10), "yyyy-MM-dd")
        )
        .withColumn("tenure_days",
            F.when(
                F.col("hire_date_parsed").isNotNull() & F.col("period_date").isNotNull(),
                F.datediff(F.col("period_date"), F.col("hire_date_parsed"))
            ).otherwise(F.lit(None).cast(IntegerType()))
        )
        .withColumn("tenure_days",
            # Null out negative tenure (data error — hire_date after period_date)
            F.when(F.col("tenure_days") < 0, F.lit(None).cast(IntegerType()))
             .otherwise(F.col("tenure_days"))
        )
        .withColumn("tenure_band",
            F.when(F.col("tenure_days").isNull(), F.lit(None))
             .when(F.col("tenure_days") < 365, "0-1 year")
             .when(F.col("tenure_days") < 1095, "1-3 years")
             .when(F.col("tenure_days") < 1825, "3-5 years")
             .otherwise("5+ years")
        )
        .select(
            "employee_id", "department_id", "role_id",
            "period", "period_date",
            "hire_date", "termination_date",
            "tenure_days", "tenure_band",
            "_ingested_at", "_source_file_name"
        )
    )

# COMMAND ----------

# DBTITLE 1, fact_payroll — append with referential integrity
@dp.table(
    name="fact_payroll",
    comment="Payroll events per employee per period. Referential integrity enforced against bronze_employees.",
    table_properties={"quality": "silver"}
)
def fact_payroll():
    payroll = spark.table(f"{catalog}.bronze.bronze_payroll_events")
    valid_employees = (
        spark.table(f"{catalog}.bronze.bronze_employees")
        .filter(F.col("employee_id").isNotNull())
        .select("employee_id").distinct()
    )
    return (
        payroll
        .join(valid_employees, on="employee_id", how="inner")
        .filter(
            F.col("employee_id").isNotNull() &
            F.col("gross_pay").isNotNull() &
            (F.col("gross_pay") >= 0) &
            F.col("net_pay").isNotNull() &
            (F.col("net_pay") >= 0)
        )
        .select(
            "employee_id", "period",
            "gross_pay", "deductions", "net_pay",
            "_ingested_at", "_source_file_name"
        )
    )

# COMMAND ----------

# DBTITLE 1, fact_compensation — compensation change events
@dp.table(
    name="fact_compensation",
    comment="Compensation per employee. Schema evolves from period 3 with compensation_category.",
    table_properties={"quality": "silver"}
)
def fact_compensation():
    comp = spark.table(f"{catalog}.bronze.bronze_compensation")
    base_cols = ["employee_id", "salary", "currency", "effective_date",
                 "_ingested_at", "_source_file_name"]
    select_cols = base_cols if "compensation_category" not in comp.columns else [
        "employee_id", "salary", "currency", "effective_date",
        "compensation_category", "_ingested_at", "_source_file_name"
    ]
    return (
        comp
        .filter(
            F.col("employee_id").isNotNull() &
            F.col("salary").isNotNull() &
            (F.col("salary") > 0)
        )
        .select(*select_cols)
    )

# COMMAND ----------

# DBTITLE 1, fact_workforce_movements — derived from consecutive snapshots
# Detects HIRE, TRANSFER, and TERMINATION events by comparing each employee's
# state across consecutive periods in fact_headcount.
#
# HIRE:        first appearance in the headcount table
# TRANSFER:    department_id or role_id changed between consecutive periods
# TERMINATION: present in period N, absent in period N+1
@dp.table(
    name="fact_workforce_movements",
    comment="Workforce movement events derived by comparing consecutive monthly snapshots.",
    table_properties={"quality": "silver"}
)
def fact_workforce_movements():
    headcount = spark.table(f"{catalog}.silver.fact_headcount").select(
        "employee_id", "department_id", "role_id", "period"
    )

    w_emp = Window.partitionBy("employee_id").orderBy("period")

    with_prev = (
        headcount
        .withColumn("prev_period", F.lag("period").over(w_emp))
        .withColumn("prev_department_id", F.lag("department_id").over(w_emp))
        .withColumn("prev_role_id", F.lag("role_id").over(w_emp))
    )

    # HIRE: no previous period
    hires = (
        with_prev.filter(F.col("prev_period").isNull())
        .withColumn("event_type", F.lit("HIRE"))
        .withColumn("from_department_id", F.lit(None).cast(StringType()))
        .withColumn("from_role_id", F.lit(None).cast(StringType()))
        .select("employee_id","period","event_type",
                "from_department_id","department_id","from_role_id","role_id")
    )

    # TRANSFER: attribute changed
    transfers = (
        with_prev.filter(
            F.col("prev_period").isNotNull() &
            (
                (F.col("department_id") != F.col("prev_department_id")) |
                (F.col("role_id") != F.col("prev_role_id"))
            )
        )
        .withColumn("event_type", F.lit("TRANSFER"))
        .withColumnRenamed("prev_department_id","from_department_id")
        .withColumnRenamed("prev_role_id","from_role_id")
        .select("employee_id","period","event_type",
                "from_department_id","department_id","from_role_id","role_id")
    )

    # TERMINATION: present in period N, absent in N+1
    # Build period sequence to find next period for each row
    all_periods = headcount.select("period").distinct()
    w_periods = Window.orderBy("period")
    periods_with_next = (
        all_periods
        .withColumn("next_period", F.lead("period").over(w_periods))
        .filter(F.col("next_period").isNotNull())
    )
    terminations = (
        headcount.alias("curr")
        .join(periods_with_next.alias("p"),
              F.col("curr.period") == F.col("p.period"))
        .join(
            headcount.alias("nxt"),
            (F.col("curr.employee_id") == F.col("nxt.employee_id")) &
            (F.col("nxt.period") == F.col("p.next_period")),
            how="left_anti"
        )
        .withColumn("event_type", F.lit("TERMINATION"))
        .withColumn("from_department_id", F.col("curr.department_id"))
        .withColumn("from_role_id", F.col("curr.role_id"))
        .select(
            F.col("curr.employee_id").alias("employee_id"),
            F.col("p.next_period").alias("period"),
            "event_type",
            "from_department_id",
            F.lit(None).cast(StringType()).alias("department_id"),
            "from_role_id",
            F.lit(None).cast(StringType()).alias("role_id")
        )
    )

    return hires.union(transfers).union(terminations)

# COMMAND ----------

# DBTITLE 1, data_quality_log — config-driven dead letter table
# All rules are sourced from quality_rules.json via the loader above.
# Adding a new rule requires only a JSON change — no pipeline code change.
# Schema matches master reference exactly including resolution tracking columns.
@dp.table(
    name="data_quality_log",
    comment="Dead letter table. All records failing quality expectations. Config-driven from quality_rules.json.",
    table_properties={"quality": "silver"}
)
def data_quality_log():
    emp_bronze = spark.table(f"{catalog}.bronze.bronze_employees")
    payroll_bronze = spark.table(f"{catalog}.bronze.bronze_payroll_events")

    entries = []

    # ── employees: CRITICAL — null employee_id ───────────────────────────
    critical_rules = get_critical_rules("employees")
    for rule_def in critical_rules:
        if rule_def["field"] == "employee_id":
            entries.append(
                build_dq_entries(
                    emp_bronze.filter(F.col("employee_id").isNull()),
                    entity="employees",
                    field=rule_def["field"],
                    rule=rule_def["rule"],
                    severity=rule_def["severity"],
                    actual_value_col=None
                )
            )

    # ── employees: ERROR rules ───────────────────────────────────────────
    for rule_def in get_error_rules("employees"):
        if rule_def["field"] == "hire_date":
            entries.append(
                build_dq_entries(
                    emp_bronze.filter(
                        F.col("employee_id").isNotNull() &
                        F.col("hire_date").isNull()
                    ),
                    entity="employees",
                    field=rule_def["field"],
                    rule=rule_def["rule"],
                    severity=rule_def["severity"],
                    actual_value_col=None
                )
            )
        elif rule_def["field"] == "termination_date":
            entries.append(
                build_dq_entries(
                    emp_bronze.filter(
                        F.col("employee_id").isNotNull() &
                        F.col("termination_date").isNotNull() &
                        (F.col("termination_date") < F.col("hire_date"))
                    ),
                    entity="employees",
                    field=rule_def["field"],
                    rule=rule_def["rule"],
                    severity=rule_def["severity"],
                    actual_value_col="termination_date"
                )
            )
        elif rule_def["field"] == "tenure_days":
            # tenure_days is derived in fact_headcount — check it there
            if spark.catalog.tableExists(f"{catalog}.silver.fact_headcount"):
                entries.append(
                    build_dq_entries(
                        spark.table(f"{catalog}.silver.fact_headcount")
                        .filter(
                            F.col("tenure_days").isNotNull() &
                            (F.col("tenure_days") < 0)
                        ),
                        entity="employees",
                        field=rule_def["field"],
                        rule=rule_def["rule"],
                        severity=rule_def["severity"],
                        actual_value_col="tenure_days"
                    )
                )

    # ── payroll_events: ERROR — missing employee_id (referential integrity) ─
    valid_employees = (
        emp_bronze.filter(F.col("employee_id").isNotNull())
        .select("employee_id").distinct()
    )
    for rule_def in get_error_rules("payroll_events"):
        if rule_def["field"] == "employee_id":
            entries.append(
                build_dq_entries(
                    payroll_bronze
                    .join(valid_employees, on="employee_id", how="left_anti")
                    .filter(F.col("employee_id").isNotNull()),
                    entity="payroll_events",
                    field=rule_def["field"],
                    rule=rule_def["rule"],
                    severity=rule_def["severity"],
                    actual_value_col="employee_id"
                )
            )
        elif rule_def["field"] in ("net_pay",):
            entries.append(
                build_dq_entries(
                    payroll_bronze.filter(
                        F.col("net_pay").isNull() | (F.col("net_pay") < 0)
                    ),
                    entity="payroll_events",
                    field=rule_def["field"],
                    rule=rule_def["rule"],
                    severity=rule_def["severity"],
                    actual_value_col="net_pay"
                )
            )

    # ── payroll_events: WARNING — zero gross_pay ─────────────────────────
    for rule_def in get_warning_rules("payroll_events"):
        if rule_def["field"] == "gross_pay":
            entries.append(
                build_dq_entries(
                    payroll_bronze.filter(
                        F.col("gross_pay").isNotNull() &
                        (F.col("gross_pay") == 0)
                    ),
                    entity="payroll_events",
                    field=rule_def["field"],
                    rule=rule_def["rule"],
                    severity=rule_def["severity"],
                    actual_value_col="gross_pay"
                )
            )

    # Union all collected entries
    if not entries:
        # Return empty DataFrame with correct schema when no violations
        return spark.createDataFrame([], schema=(
            spark.table(f"{catalog}.bronze.bronze_employees")
            .filter(F.lit(False))
            .withColumn("entity", F.lit(None).cast(StringType()))
            .withColumn("employee_id_dq", F.lit(None).cast(StringType()))
            .withColumn("period", F.lit(None).cast(StringType()))
            .withColumn("field", F.lit(None).cast(StringType()))
            .withColumn("rule", F.lit(None).cast(StringType()))
            .withColumn("severity", F.lit(None).cast(StringType()))
            .withColumn("actual_value", F.lit(None).cast(StringType()))
            .withColumn("requires_review", F.lit(None).cast(BooleanType()))
            .withColumn("resolved", F.lit(None).cast(BooleanType()))
            .withColumn("resolved_by", F.lit(None).cast(StringType()))
            .withColumn("resolved_at", F.lit(None).cast(TimestampType()))
            .withColumn("resolution_type", F.lit(None).cast(StringType()))
            .select(
                "_ingested_at","_source_file_name","entity","employee_id_dq",
                "period","field","rule","severity","actual_value",
                "requires_review","resolved","resolved_by","resolved_at","resolution_type"
            )
        ).schema)

    result = entries[0]
    for entry in entries[1:]:
        result = result.union(entry)
    return result
