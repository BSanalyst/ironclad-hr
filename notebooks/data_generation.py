# Databricks notebook source
# MAGIC %pip install dbldatagen==0.4.0.post1
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

# COMMAND ----------

# DBTITLE 1, Parameters
dbutils.widgets.text("year", "2025")
dbutils.widgets.text("month", "1")
dbutils.widgets.text("num_employees", "500")
dbutils.widgets.text("catalog", "ironclad_hr")

year = int(dbutils.widgets.get("year"))
month = int(dbutils.widgets.get("month"))
num_employees = int(dbutils.widgets.get("num_employees"))
catalog = dbutils.widgets.get("catalog")

period = f"{year}-{str(month).zfill(2)}"
landing_path = f"/Volumes/{catalog}/bronze/landing/{period}"
begin_date_str = "2018-01-01 00:00:00"
end_date_str = f"{year}-{str(month).zfill(2)}-01 00:00:00"
include_compensation_category = month >= 3

print(f"Generating HR data for period: {period}")
print(f"Landing path: {landing_path}")
print(f"Schema evolution - compensation_category: {'included' if include_compensation_category else 'absent'}")

# COMMAND ----------

# DBTITLE 1, Departments
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

dept_df = spark.createDataFrame(dept_data, schema=dept_schema)
dept_df.write.mode("overwrite").json(f"{landing_path}/departments/")
print(f"Departments written: {dept_df.count()} rows")

# COMMAND ----------

# DBTITLE 1, Job Roles
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

roles_df = spark.createDataFrame(roles_data, schema=roles_schema)
roles_df.write.mode("overwrite").json(f"{landing_path}/job_roles/")
print(f"Job roles written: {roles_df.count()} rows")

# COMMAND ----------

# DBTITLE 1, Employees
dept_ids = [r[0] for r in dept_data]
role_ids = [r[0] for r in roles_data]

emp_spec = (
    dg.DataGenerator(spark, name="employees", rows=num_employees, partitions=4)
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
emp_df.write.mode("overwrite").json(f"{landing_path}/employees/")
print(f"Employees written: {emp_df.count()} rows")

# COMMAND ----------

# DBTITLE 1, Contracts
contract_spec = (
    dg.DataGenerator(spark, name="contracts", rows=num_employees, partitions=4)
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
print(f"Contracts written: {contract_df.count()} rows")

# COMMAND ----------

# DBTITLE 1, Compensation
comp_spec = (
    dg.DataGenerator(spark, name="compensation", rows=num_employees, partitions=4)
    .withIdOutput()
    .withColumn("employee_id", StringType(), template=r"EMPddddd", baseColumn="id")
    .withColumn("salary", LongType(), minValue=30000, maxValue=200000, step=1000, random=True)
    .withColumn("currency", StringType(), values=["GBP"], weights=[100])
    .withColumn("effective_date", StringType(),
                dataRange=dg.DateRange(begin_date_str, end_date_str, "days=1"),
                random=True)
)

comp_df = comp_spec.build().drop("id")

if include_compensation_category:
    comp_df = comp_df.withColumn(
        "compensation_category",
        F.when(F.col("salary") < 50000, "Band 1")
         .when(F.col("salary") < 80000, "Band 2")
         .when(F.col("salary") < 120000, "Band 3")
         .otherwise("Band 4")
    )
    print(f"Schema evolution: compensation_category included (month {month})")
else:
    print(f"Schema evolution: compensation_category absent (month {month})")

comp_df.write.mode("overwrite").json(f"{landing_path}/compensation/")
print(f"Compensation written: {comp_df.count()} rows")

# COMMAND ----------

# DBTITLE 1, Payroll Events
payroll_spec = (
    dg.DataGenerator(spark, name="payroll_events", rows=num_employees, partitions=4)
    .withIdOutput()
    .withColumn("employee_id", StringType(), template=r"EMPddddd", baseColumn="id")
    .withColumn("period", StringType(), values=[period], weights=[100])
    .withColumn("gross_pay", LongType(), minValue=2500, maxValue=16667, step=100, random=True)
    .withColumn("deductions", LongType(), minValue=500, maxValue=3000, step=50, random=True)
)

payroll_df = payroll_spec.build().drop("id")
payroll_df = payroll_df.withColumn("net_pay", F.col("gross_pay") - F.col("deductions"))
payroll_df.write.mode("overwrite").json(f"{landing_path}/payroll_events/")
print(f"Payroll events written: {payroll_df.count()} rows")

# COMMAND ----------

print(f"Data generation complete for period: {period}")
print(f"All files written to: {landing_path}")
