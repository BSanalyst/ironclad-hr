# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1, Configuration
catalog = spark.conf.get("catalog", "ironclad_hr")

# COMMAND ----------

# DBTITLE 1, headcount_monthly
@dp.table(
    name="headcount_monthly",
    comment="Active employee count by department, contract type, band and period.",
    table_properties={"quality": "gold"}
)
def headcount_monthly():
    headcount = spark.table(f"{catalog}.silver.fact_headcount")
    departments = (
        spark.table(f"{catalog}.silver.dim_department")
        .filter(F.col("__END_AT").isNull())
        .select("department_id", F.col("name").alias("department_name"))
    )
    contracts = (
        spark.table(f"{catalog}.silver.dim_contract")
        .filter(F.col("__END_AT").isNull())
        .select("employee_id", "contract_type")
    )
    roles = (
        spark.table(f"{catalog}.silver.dim_job_role")
        .filter(F.col("__END_AT").isNull())
        .select("role_id", "band")
    )
    return (
        headcount
        .filter(F.col("termination_date").isNull())
        .join(departments, on="department_id", how="left")
        .join(contracts, on="employee_id", how="left")
        .join(roles, on="role_id", how="left")
        .groupBy("period","department_id","department_name","contract_type","band","tenure_band")
        .agg(F.count("employee_id").alias("employee_count"))
        .select(
            "period","department_id","department_name",
            "contract_type","band","tenure_band","employee_count"
        )
    )

# COMMAND ----------

# DBTITLE 1, attrition_summary
@dp.table(
    name="attrition_summary",
    comment="Terminations by department, rolling 12 months.",
    table_properties={"quality": "gold"}
)
def attrition_summary():
    movements = spark.table(f"{catalog}.silver.fact_workforce_movements")
    departments = (
        spark.table(f"{catalog}.silver.dim_department")
        .filter(F.col("__END_AT").isNull())
        .select("department_id", F.col("name").alias("department_name"))
    )
    return (
        movements
        .filter(F.col("event_type") == "TERMINATION")
        .join(
            departments.withColumnRenamed("department_id","from_department_id")
                       .withColumnRenamed("department_name","department_name"),
            on="from_department_id",
            how="left"
        )
        .groupBy("period","from_department_id","department_name")
        .agg(F.count("employee_id").alias("termination_count"))
        .withColumnRenamed("from_department_id","department_id")
        .select("period","department_id","department_name","termination_count")
    )

# COMMAND ----------

# DBTITLE 1, compensation_bands
@dp.table(
    name="compensation_bands",
    comment="Salary distribution by job band and department. Sensitive fields controlled via UC masking.",
    table_properties={"quality": "gold"}
)
def compensation_bands():
    comp = spark.table(f"{catalog}.silver.fact_compensation")
    roles = (
        spark.table(f"{catalog}.silver.dim_job_role")
        .filter(F.col("__END_AT").isNull())
        .select("role_id","band","job_family")
    )
    employees = (
        spark.table(f"{catalog}.silver.dim_employee")
        .filter(F.col("__END_AT").isNull())
        .select("employee_id","role_id","department_id")
    )
    departments = (
        spark.table(f"{catalog}.silver.dim_department")
        .filter(F.col("__END_AT").isNull())
        .select("department_id", F.col("name").alias("department_name"))
    )
    return (
        comp
        .join(employees, on="employee_id", how="left")
        .join(roles, on="role_id", how="left")
        .join(departments, on="department_id", how="left")
        .groupBy("band","job_family","department_name")
        .agg(
            F.count("employee_id").alias("employee_count"),
            F.avg("salary").alias("avg_salary"),
            F.min("salary").alias("min_salary"),
            F.max("salary").alias("max_salary")
        )
        .select(
            "band","job_family","department_name",
            "employee_count","avg_salary","min_salary","max_salary"
        )
    )

# COMMAND ----------

# DBTITLE 1, workforce_movements
@dp.table(
    name="workforce_movements",
    comment="Hires, transfers and terminations by period.",
    table_properties={"quality": "gold"}
)
def workforce_movements():
    movements = spark.table(f"{catalog}.silver.fact_workforce_movements")
    departments = (
        spark.table(f"{catalog}.silver.dim_department")
        .filter(F.col("__END_AT").isNull())
        .select("department_id", F.col("name").alias("department_name"))
    )
    return (
        movements
        .join(
            departments.withColumnRenamed("department_id","from_department_id")
                       .withColumnRenamed("department_name","from_department_name"),
            on="from_department_id",
            how="left"
        )
        .join(
            departments.withColumnRenamed("department_name","to_department_name"),
            on="department_id",
            how="left"
        )
        .groupBy("period","event_type","from_department_name","to_department_name")
        .agg(F.count("employee_id").alias("movement_count"))
        .select(
            "period","event_type",
            "from_department_name","to_department_name",
            "movement_count"
        )
    )

# COMMAND ----------

# DBTITLE 1, tenure_bands — CI/CD demonstration feature
# This view was added as the centrepiece of the CI/CD demonstration.
# It uses Window.partitionBy to compute department-level percentage
# without any import hacks — the Window import is at the top of the file.
@dp.table(
    name="tenure_bands",
    comment="Employee count by tenure bracket per period, department and contract type.",
    table_properties={"quality": "gold"}
)
def tenure_bands():
    headcount = spark.table(f"{catalog}.silver.fact_headcount")
    departments = (
        spark.table(f"{catalog}.silver.dim_department")
        .filter(F.col("__END_AT").isNull())
        .select("department_id", F.col("name").alias("department_name"))
    )
    contracts = (
        spark.table(f"{catalog}.silver.dim_contract")
        .filter(F.col("__END_AT").isNull())
        .select("employee_id","contract_type")
    )

    w_dept = Window.partitionBy("period","department_id")

    return (
        headcount
        .filter(
            F.col("termination_date").isNull() &
            F.col("tenure_band").isNotNull()
        )
        .join(departments, on="department_id", how="left")
        .join(contracts, on="employee_id", how="left")
        .groupBy("period","department_id","department_name","contract_type","tenure_band")
        .agg(F.count("employee_id").alias("employee_count"))
        .withColumn(
            "pct_of_department",
            F.round(
                F.col("employee_count") /
                F.sum("employee_count").over(w_dept) * 100,
                2
            )
        )
        .select(
            "period","department_id","department_name",
            "contract_type","tenure_band",
            "employee_count","pct_of_department"
        )
    )

# COMMAND ----------

# DBTITLE 1, data_quality_review — steward daily review
@dp.table(
    name="data_quality_review",
    comment="Unresolved WARNING and ERROR records for daily data steward review.",
    table_properties={"quality": "gold"}
)
def data_quality_review():
    return (
        spark.table(f"{catalog}.silver.data_quality_log")
        .filter(
            F.col("resolved").isNull() |
            (F.col("resolved") == False)
        )
        .filter(F.col("severity").isin(["WARNING","ERROR","CRITICAL"]))
        .orderBy(F.col("severity").desc(), F.col("_ingested_at").desc())
    )
