# Databricks notebook source

# DBTITLE 1, Restatement Audit Logger
# Logs every restatement operation to the audit table.
# Runs as the final task in the restatement job.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType
from datetime import datetime

# COMMAND ----------

# DBTITLE 1, Parameters
dbutils.widgets.text("catalog", "ironclad_hr")
dbutils.widgets.text("employee_id", "")
dbutils.widgets.text("start_period", "")
dbutils.widgets.text("end_period", "")
dbutils.widgets.text("triggered_by", "")
dbutils.widgets.text("restatement_status", "SUCCESS")

catalog = dbutils.widgets.get("catalog")
employee_id = dbutils.widgets.get("employee_id")
start_period = dbutils.widgets.get("start_period")
end_period = dbutils.widgets.get("end_period")
triggered_by = dbutils.widgets.get("triggered_by")
restatement_status = dbutils.widgets.get("restatement_status")

# COMMAND ----------

# DBTITLE 1, Validate parameters
errors = []
if not employee_id:
    errors.append("employee_id is required")
if not start_period:
    errors.append("start_period is required")
if not end_period:
    errors.append("end_period is required")
if not triggered_by:
    errors.append("triggered_by is required")
if errors:
    raise ValueError(f"Parameter validation failed: {', '.join(errors)}")

print(f"Logging restatement audit entry:")
print(f"  employee_id: {employee_id}")
print(f"  start_period: {start_period}")
print(f"  end_period: {end_period}")
print(f"  triggered_by: {triggered_by}")
print(f"  status: {restatement_status}")

# COMMAND ----------

# DBTITLE 1, Write audit entry
audit_schema = StructType([
    StructField("restatement_id", StringType(), True),
    StructField("employee_id", StringType(), True),
    StructField("start_period", StringType(), True),
    StructField("end_period", StringType(), True),
    StructField("triggered_by", StringType(), True),
    StructField("triggered_at", StringType(), True),
    StructField("status", StringType(), True),
])

audit_row = [(
    str(datetime.now().timestamp()),
    employee_id,
    start_period,
    end_period,
    triggered_by,
    datetime.now().isoformat(),
    restatement_status,
)]

audit_df = spark.createDataFrame(audit_row, schema=audit_schema)

audit_df.write.mode("append").saveAsTable(f"{catalog}.silver.restatement_audit_log")

print(f"Restatement audit entry written to {catalog}.silver.restatement_audit_log")
