# Databricks notebook source
# MAGIC %pip install dbldatagen==0.4.0.post1
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

# COMMAND ----------

# DBTITLE 1, Parameters
dbutils.widgets.text("start_year", "2025")
dbutils.widgets.text("start_month", "1")
dbutils.widgets.text("num_periods", "6")
dbutils.widgets.text("num_employees", "500")
dbutils.widgets.text("catalog", "ironclad_hr")
dbutils.widgets.text("inject_bad_records_period", "2")
dbutils.widgets.text("schema_evolution_month", "3")

start_year = int(dbutils.widgets.get("start_year"))
start_month = int(dbutils.widgets.get("start_month"))
num_periods = int(dbutils.widgets.get("num_periods"))
num_employees = int(dbutils.widgets.get("num_employees"))
catalog = dbutils.widgets.get("catalog")
inject_bad_records_period = int(dbutils.widgets.get("inject_bad_records_period"))
schema_evolution_month = int(dbutils.widgets.get("schema_evolution_month"))

begin_date_str = "2018-01-01 00:00:00"

print(f"Demo mode: {num_periods} periods from {start_year}-{str(start_month).zfill(2)}")
print(f"Bad records injected at period: {inject_bad_records_period}")
print(f"Schema evolution (compensation_category) from period: {schema_evolution_month}")

# COMMAND ----------

# DBTITLE 1, Static reference data — departments and job roles
dept_schema = StructType([
    StructField("department_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("cost_centre", StringType(), True),
    StructField("parent_department_id", StringType(), True),
])

dept_data = [
    ("D001", "Engineering", "CC-ENG", None),
    ("D002", "Product", "CC-PRD", None),
    ("D003", "Finance", "CC-FIN", None),
    ("D004", "HR", "CC-HR", None),
    ("D005", "Sales", "CC-SAL", None),
    ("D006", "Marketing", "CC-MKT", None),
    ("D007", "Operations", "CC-OPS", None),
    ("D008", "Legal", "CC-LEG", None),
]

roles_schema = StructType([
    StructField("role_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("band", StringType(), True),
    StructField("function", StringType(), True),
    StructField("job_family", StringType(), True),
])

roles_data = [
    ("R001", "Software Engineer", "IC3", "Engineering", "Technology"),
    ("R002", "Senior Software Engineer", "IC4", "Engineering", "Technology"),
    ("R003", "Staff Engineer", "IC5", "Engineering", "Technology"),
    ("R004", "Product Manager", "IC4", "Product", "Product"),
    ("R005", "Senior Product Manager", "IC5", "Product", "Product"),
    ("R006", "Financial Analyst", "IC3", "Finance", "Finance"),
    ("R007", "Senior Financial Analyst", "IC4", "Finance", "Finance"),
    ("R008", "HR Business Partner", "IC3", "HR", "People"),
    ("R009", "Account Executive", "IC3", "Sales", "Revenue"),
    ("R010", "Senior Account Executive", "IC4", "Sales", "Revenue"),
    ("R011", "Marketing Manager", "IC4", "Marketing", "Growth"),
    ("R012", "Operations Analyst", "IC3", "Operations", "Operations"),
    ("R013", "Legal Counsel", "IC4", "Legal", "Legal"),
]

dept_ids = [r[0] for r in dept_data]
role_ids = [r[0] for r in roles_data]

# COMMAND ----------

# DBTITLE 1, Helper — compute year/month from period offset
def get_year_month(start_year, start_month, offset):
    month = ((start_month - 1 + offset) % 12) + 1
    year = start_year + ((start_month - 1 + offset) // 12)
    return year, month

# COMMAND ----------

# DBTITLE 1, Multi-period generation loop
for i in range(num_periods):
    year, month = get_year_month(start_year, start_month, i)
    period = f"{year}-{str(month).zfill(2)}"
    period_num = i + 1
    landing_path = f"/Volumes/{catalog}/bronze/landing/{period}"
    end_date_str = f"{year}-{str(month).zfill(2)}-01 00:00:00"
    inject_bad = period_num == inject_bad_records_period
    include_comp_cat = period_num >= schema_evolution_month

    print(f"\nGenerating period {period_num}/{num_periods}: {period}")
    print(f"  Bad records: {'YES' if inject_bad else 'no'}")
    print(f"  compensation_category: {'included' if include_comp_cat else 'absent'}")

    # Static reference data written once per period
    dept_df = spark.createDataFrame(dept_data, schema=dept_schema)
    dept_df.write.mode("overwrite").json(f"{landing_path}/departments/")

    roles_df = spark.createDataFrame(roles_data, schema=roles_schema)
    roles_df.write.mode("overwrite").json(f"{landing_path}/job_roles/")

    # Employees
    emp_spec = (
        dg.DataGenerator(spark, name=f"employees_{period}", rows=num_employees, partitions=4)
        .withIdOutput()
        .withColumn("employee_id", StringType(), template=r"EMPddddd", baseColumn="id")
        .withColumn("full_name", StringType(), template=r"\w \w")
        .withColumn("email", StringType(), template=r"dddd.dddd@ironcladhr.com", baseColumn="id")
        .withColumn("gender", StringType(), values=["Male", "Female", "Non-binary"], weights=[45, 45, 10])
        .withColumn("hire_date", StringType(),
                    dataRange=dg.DateRange(begin_date_str, end_date_str, "days=1"),
                    random=True)
        .withColumn("termination_date", StringType(), values=[""], percentNulls=1.0)
        .withColumn("department_id", StringType(), values=dept_ids, random=True)
        .withColumn("role_id", StringType(), values=role_ids, random=True)
        .withColumn("manager_id", StringType(), template=r"EMPddddd", baseColumn="id")
        .withColumn("nationality", StringType(),
                    values=["British", "American", "French", "German", "Indian", "Australian"],
                    random=True)
    )

    emp_df = emp_spec.build().drop("id")
    emp_df = emp_df.withColumn(
        "termination_date",
        F.when(F.col("termination_date") == "", F.lit(None)).otherwise(F.col("termination_date"))
    )

    if inject_bad:
        bad_schema = emp_df.schema
        bad_records = spark.createDataFrame([
            # CRITICAL severity: null employee_id
            (None, "Bad Employee", "bad@ironcladhr.com", "Male",
             "2020-01-01 00:00:00", None, "D001", "R001", "EMP00001", "British"),
            # ERROR severity: termination_date before hire_date
            ("EMP99998", "Early Exit", "early@ironcladhr.com", "Female",
             "2023-06-01 00:00:00", "2022-01-01 00:00:00", "D002", "R002", "EMP00002", "American"),
        ], schema=bad_schema)
        emp_df = emp_df.union(bad_records)
        print(f"  Injected 2 bad employee records (1 CRITICAL, 1 ERROR)")

    emp_df.write.mode("overwrite").json(f"{landing_path}/employees/")
    print(f"  Employees written: {emp_df.count()} rows")

    # Contracts
    contract_spec = (
        dg.DataGenerator(spark, name=f"contracts_{period}", rows=num_employees, partitions=4)
        .withIdOutput()
        .withColumn("contract_id", StringType(), template=r"CONddddd", baseColumn="id")
        .withColumn("employee_id", StringType(), template=r"EMPddddd", baseColumn="id")
        .withColumn("contract_type", StringType(),
                    values=["Permanent", "Contractor", "Fixed-Term"], weights=[70, 20, 10])
        .withColumn("start_date", StringType(),
                    dataRange=dg.DateRange(begin_date_str, end_date_str, "days=1"),
                    random=True)
        .withColumn("end_date", StringType(), values=[""], percentNulls=1.0)
    )
    contract_df = contract_spec.build().drop("id")
    contract_df = contract_df.withColumn(
        "end_date",
        F.when(F.col("end_date") == "", F.lit(None)).otherwise(F.col("end_date"))
    )
    contract_df.write.mode("overwrite").json(f"{landing_path}/contracts/")
    print(f"  Contracts written: {contract_df.count()} rows")

    # Compensation
    comp_spec = (
        dg.DataGenerator(spark, name=f"compensation_{period}", rows=num_employees, partitions=4)
        .withIdOutput()
        .withColumn("employee_id", StringType(), template=r"EMPddddd", baseColumn="id")
        .withColumn("salary", LongType(), minValue=30000, maxValue=200000, step=1000, random=True)
        .withColumn("currency", StringType(), values=["GBP"], weights=[100])
        .withColumn("effective_date", StringType(),
                    dataRange=dg.DateRange(begin_date_str, end_date_str, "days=1"),
                    random=True)
    )
    comp_df = comp_spec.build().drop("id")
    if include_comp_cat:
        comp_df = comp_df.withColumn(
            "compensation_category",
            F.when(F.col("salary") < 50000, "Band 1")
             .when(F.col("salary") < 80000, "Band 2")
             .when(F.col("salary") < 120000, "Band 3")
             .otherwise("Band 4")
        )
    comp_df.write.mode("overwrite").json(f"{landing_path}/compensation/")
    print(f"  Compensation written: {comp_df.count()} rows")

    # Payroll events
    payroll_spec = (
        dg.DataGenerator(spark, name=f"payroll_{period}", rows=num_employees, partitions=4)
        .withIdOutput()
        .withColumn("employee_id", StringType(), template=r"EMPddddd", baseColumn="id")
        .withColumn("period", StringType(), values=[period], weights=[100])
        .withColumn("gross_pay", LongType(), minValue=2500, maxValue=16667, step=100, random=True)
        .withColumn("deductions", LongType(), minValue=500, maxValue=3000, step=50, random=True)
    )
    payroll_df = payroll_spec.build().drop("id")
    payroll_df = payroll_df.withColumn("net_pay", F.col("gross_pay") - F.col("deductions"))
    payroll_df.write.mode("overwrite").json(f"{landing_path}/payroll_events/")
    print(f"  Payroll events written: {payroll_df.count()} rows")

    print(f"  Period {period} complete — all files written to {landing_path}")

# COMMAND ----------

print(f"\nDemo generation complete.")
print(f"Generated {num_periods} periods of HR data.")
print(f"Bad records injected at period {inject_bad_records_period}.")
print(f"Schema evolution introduced at period {schema_evolution_month}.")
