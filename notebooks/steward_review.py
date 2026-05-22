# Databricks notebook source

# DBTITLE 1, Data Steward Daily Review Report
# Surfaces all unresolved WARNING, ERROR and CRITICAL records
# for data steward review. Runs as final task in normal pipeline job.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1, Parameters
dbutils.widgets.text("catalog", "ironclad_hr")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# DBTITLE 1, Load unresolved quality issues
review_df = (
    spark.table(f"{catalog}.silver.data_quality_log")
    .filter(
        (F.col("resolved").isNull()) |
        (F.col("resolved") == False)
    )
    .filter(F.col("severity").isin(["WARNING", "ERROR", "CRITICAL"]))
    .orderBy(F.col("severity").desc(), F.col("_ingested_at").desc())
)

total_issues = review_df.count()
critical = review_df.filter(F.col("severity") == "CRITICAL").count()
errors = review_df.filter(F.col("severity") == "ERROR").count()
warnings = review_df.filter(F.col("severity") == "WARNING").count()

# COMMAND ----------

# DBTITLE 1, Summary
print("=" * 60)
print("IRONCLAD HR — DATA QUALITY STEWARD REVIEW")
print("=" * 60)
print(f"Total unresolved issues: {total_issues}")
print(f"  CRITICAL: {critical}")
print(f"  ERROR:    {errors}")
print(f"  WARNING:  {warnings}")
print("=" * 60)

if total_issues > 0:
    print("\nUnresolved issues requiring review:")
    review_df.show(50, truncate=False)
else:
    print("\nNo unresolved issues. Pipeline data quality is clean.")

# COMMAND ----------

# DBTITLE 1, Resolution options reminder
print("""
RESOLUTION OPTIONS FOR DATA STEWARD:
1. ACCEPT      — Value is valid. Update quality_rules.json. Raise PR. Pipeline absorbs on next run.
2. CORRECT     — Raise ticket with source system team. Await corrected file. Trigger restatement job.
3. OVERRIDE    — Exceptional cases only. Contact data engineering team for controlled override.

To mark a record as resolved, update data_quality_log:
  UPDATE ironclad_hr.silver.data_quality_log
  SET resolved = true, resolved_by = '<your name>', resolved_at = current_timestamp(), resolution_type = '<type>'
  WHERE _ingested_at = '<timestamp>' AND entity = '<entity>' AND field = '<field>'
""")
