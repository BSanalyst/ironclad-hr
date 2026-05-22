# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import date, timedelta

# COMMAND ----------

# DBTITLE 1, Configuration
catalog = spark.conf.get("catalog", "ironclad_hr")

# COMMAND ----------

# DBTITLE 1, dim_date — static dimension, built once
dp.create_streaming_table(
    name="dim_date",
    comment="Static date dimension covering 2018-2030. Built once via append_flow.",
    table_properties={"quality": "silver"}
)

@dp.append_flow(target="dim_date", once=True, name="dim_date_load")
def load_dim_date():
    from pyspark.sql import Row
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

# DBTITLE 1, dim_department — SCD Type 2 from snapshot
dp.create_streaming_table(
    name="dim_department",
    comment="Department dimension. SCD Type 2. Tracks restructures, renames, merges.",
    table_properties={"quality": "silver"}
)

def get_department_snapshot(latest_version):
    df = spark.table(f"{catalog}.bronze.bronze_departments")
    if latest_version is not None:
        df = df.filter(F.col("_ingested_at") > latest_version)
    if df.limit(1).count() == 0:
        return None
    version = df.agg(F.max("_ingested_at")).collect()[0][0]
    snapshot = df.select(
        "department_id", "name", "cost_centre",
        F.lit(None).cast(StringType()).alias("parent_department_id")
    ).distinct()
    return (snapshot, version)

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_department",
    source=get_department_snapshot,
    keys=["department_id"],
    stored_as_scd_type=2
)

# COMMAND ----------

# DBTITLE 1, dim_job_role — SCD Type 2 from snapshot
dp.create_streaming_table(
    name="dim_job_role",
    comment="Job role dimension. SCD Type 2. Tracks band and family changes.",
    table_properties={"quality": "silver"}
)

def get_job_role_snapshot(latest_version):
    df = spark.table(f"{catalog}.bronze.bronze_job_roles")
    if latest_version is not None:
        df = df.filter(F.col("_ingested_at") > latest_version)
    if df.limit(1).count() == 0:
        return None
    version = df.agg(F.max("_ingested_at")).collect()[0][0]
    snapshot = df.select(
        "role_id", "title", "band", "function", "job_family"
    ).distinct()
    return (snapshot, version)

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_job_role",
    source=get_job_role_snapshot,
    keys=["role_id"],
    stored_as_scd_type=2
)

# COMMAND ----------

# DBTITLE 1, dim_employee — SCD Type 2 from snapshot
dp.create_streaming_table(
    name="dim_employee",
    comment="Employee dimension. SCD Type 2. Tracks all personal and role attribute changes.",
    table_properties={"quality": "silver"}
)

def get_employee_snapshot(latest_version):
    df = spark.table(f"{catalog}.bronze.bronze_employees")
    if latest_version is not None:
        df = df.filter(F.col("_ingested_at") > latest_version)
    # Drop CRITICAL violations — null employee_id
    df = df.filter(F.col("employee_id").isNotNull())
    if df.limit(1).count() == 0:
        return None
    version = df.agg(F.max("_ingested_at")).collect()[0][0]
    snapshot = (
        df.select(
            "employee_id", "full_name", "email", "gender",
            "hire_date", "termination_date",
            "department_id", "role_id", "manager_id", "nationality"
        )
        # ERROR: null termination_date field where termination before hire
        .withColumn(
            "termination_date",
            F.when(
                F.col("termination_date").isNotNull() &
                (F.col("termination_date") < F.col("hire_date")),
                F.lit(None)
            ).otherwise(F.col("termination_date"))
        )
        .distinct()
    )
    return (snapshot, version)

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_employee",
    source=get_employee_snapshot,
    keys=["employee_id"],
    stored_as_scd_type=2
)

# COMMAND ----------

# DBTITLE 1, dim_contract — SCD Type 2, composite key
dp.create_streaming_table(
    name="dim_contract",
    comment="Contract dimension. SCD Type 2. Composite key: employee_id + contract_type + start_date.",
    table_properties={"quality": "silver"}
)

def get_contract_snapshot(latest_version):
    df = spark.table(f"{catalog}.bronze.bronze_contracts")
    if latest_version is not None:
        df = df.filter(F.col("_ingested_at") > latest_version)
    df = df.filter(
        F.col("employee_id").isNotNull() &
        F.col("contract_type").isNotNull() &
        F.col("start_date").isNotNull()
    )
    if df.limit(1).count() == 0:
        return None
    version = df.agg(F.max("_ingested_at")).collect()[0][0]
    snapshot = df.select(
        "contract_id", "employee_id", "contract_type", "start_date",
        F.lit(None).cast(StringType()).alias("end_date")
    ).distinct()
    return (snapshot, version)

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_contract",
    source=get_contract_snapshot,
    keys=["employee_id", "contract_type", "start_date"],
    stored_as_scd_type=2
)

# COMMAND ----------

# DBTITLE 1, fact_headcount — monthly snapshot with tenure_days and tenure_band
@dp.table(
    name="fact_headcount",
    comment="Monthly headcount snapshot. One row per employee per period. Includes tenure_days and tenure_band.",
    table_properties={"quality": "silver"}
)
def fact_headcount():
    return (
        spark.table(f"{catalog}.bronze.bronze_employees")
        .filter(F.col("employee_id").isNotNull())
        .withColumn(
            "period",
            F.date_format(F.col("_ingested_at"), "yyyy-MM")
        )
        .withColumn(
            "period_date",
            F.to_date(F.date_format(F.col("_ingested_at"), "yyyy-MM-01"))
        )
        .withColumn(
            "hire_date_parsed",
            F.to_date(F.substring(F.col("hire_date"), 1, 10), "yyyy-MM-dd")
        )
        .withColumn(
            "tenure_days",
            F.when(
                F.col("hire_date_parsed").isNotNull(),
                F.datediff(F.col("period_date"), F.col("hire_date_parsed"))
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "tenure_days",
            F.when(F.col("tenure_days") < 0, F.lit(None))
             .otherwise(F.col("tenure_days"))
        )
        .withColumn(
            "tenure_band",
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
    employees = (
        spark.table(f"{catalog}.bronze.bronze_employees")
        .select("employee_id")
        .distinct()
    )
    return (
        payroll
        .join(employees, on="employee_id", how="inner")
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
    comment="Compensation per employee. One row per compensation change event. Schema evolves from month 3.",
    table_properties={"quality": "silver"}
)
def fact_compensation():
    comp = spark.table(f"{catalog}.bronze.bronze_compensation")
    comp_cols = comp.columns
    select_cols = [
        "employee_id", "salary", "currency", "effective_date",
        "_ingested_at", "_source_file_name"
    ]
    if "compensation_category" in comp_cols:
        select_cols.insert(4, "compensation_category")
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
@dp.table(
    name="fact_workforce_movements",
    comment="Workforce movement events derived by comparing consecutive monthly snapshots.",
    table_properties={"quality": "silver"}
)
def fact_workforce_movements():
    # Read headcount with period to derive movements
    headcount = spark.table(f"{catalog}.silver.fact_headcount").select(
        "employee_id", "department_id", "role_id", "period"
    )

    # Self-join on employee_id to find consecutive period changes
    from pyspark.sql.window import Window
    w = Window.partitionBy("employee_id").orderBy("period")

    with_prev = (
        headcount
        .withColumn("prev_period", F.lag("period").over(w))
        .withColumn("prev_department_id", F.lag("department_id").over(w))
        .withColumn("prev_role_id", F.lag("role_id").over(w))
    )

    # Hires: appeared this period, no previous period
    hires = (
        with_prev
        .filter(F.col("prev_period").isNull())
        .withColumn("event_type", F.lit("HIRE"))
        .withColumn("from_department_id", F.lit(None).cast(StringType()))
        .withColumn("from_role_id", F.lit(None).cast(StringType()))
        .select(
            "employee_id", "period", "event_type",
            "from_department_id", "department_id",
            "from_role_id", "role_id"
        )
    )

    # Transfers: department or role changed
    transfers = (
        with_prev
        .filter(
            F.col("prev_period").isNotNull() &
            (
                (F.col("department_id") != F.col("prev_department_id")) |
                (F.col("role_id") != F.col("prev_role_id"))
            )
        )
        .withColumn("event_type", F.lit("TRANSFER"))
        .withColumnRenamed("prev_department_id", "from_department_id")
        .withColumnRenamed("prev_role_id", "from_role_id")
        .select(
            "employee_id", "period", "event_type",
            "from_department_id", "department_id",
            "from_role_id", "role_id"
        )
    )

    return hires.union(transfers)

# COMMAND ----------

# DBTITLE 1, data_quality_log — dead letter table
@dp.table(
    name="data_quality_log",
    comment="Dead letter table. All records failing quality expectations. Never silently discarded.",
    table_properties={"quality": "silver"}
)
def data_quality_log():
    # CRITICAL: null employee_id
    emp_critical = (
        spark.table(f"{catalog}.bronze.bronze_employees")
        .filter(F.col("employee_id").isNull())
        .withColumn("entity", F.lit("employees"))
        .withColumn("field", F.lit("employee_id"))
        .withColumn("rule", F.lit("employee_id IS NOT NULL"))
        .withColumn("severity", F.lit("CRITICAL"))
        .withColumn("actual_value", F.lit(None).cast(StringType()))
        .withColumn("requires_review", F.lit(True))
        .withColumn("resolved", F.lit(False))
        .select(
            "_ingested_at", "_source_file_name",
            "entity", "field", "rule", "severity",
            "actual_value", "requires_review", "resolved"
        )
    )

    # ERROR: termination before hire
    emp_termination_error = (
        spark.table(f"{catalog}.bronze.bronze_employees")
        .filter(
            F.col("employee_id").isNotNull() &
            F.col("termination_date").isNotNull() &
            (F.col("termination_date") < F.col("hire_date"))
        )
        .withColumn("entity", F.lit("employees"))
        .withColumn("field", F.lit("termination_date"))
        .withColumn("rule", F.lit("termination_date IS NULL OR termination_date > hire_date"))
        .withColumn("severity", F.lit("ERROR"))
        .withColumn("actual_value", F.col("termination_date").cast(StringType()))
        .withColumn("requires_review", F.lit(True))
        .withColumn("resolved", F.lit(False))
        .select(
            "_ingested_at", "_source_file_name",
            "entity", "field", "rule", "severity",
            "actual_value", "requires_review", "resolved"
        )
    )

    # ERROR: payroll referential integrity
    payroll = spark.table(f"{catalog}.bronze.bronze_payroll_events")
    employees = (
        spark.table(f"{catalog}.bronze.bronze_employees")
        .select("employee_id")
        .distinct()
    )
    payroll_integrity_errors = (
        payroll
        .join(employees, on="employee_id", how="left_anti")
        .filter(F.col("employee_id").isNotNull())
        .withColumn("entity", F.lit("payroll_events"))
        .withColumn("field", F.lit("employee_id"))
        .withColumn("rule", F.lit("employee_id exists in employees"))
        .withColumn("severity", F.lit("ERROR"))
        .withColumn("actual_value", F.col("employee_id").cast(StringType()))
        .withColumn("requires_review", F.lit(True))
        .withColumn("resolved", F.lit(False))
        .select(
            "_ingested_at", "_source_file_name",
            "entity", "field", "rule", "severity",
            "actual_value", "requires_review", "resolved"
        )
    )

    return emp_critical.union(emp_termination_error).union(payroll_integrity_errors)
