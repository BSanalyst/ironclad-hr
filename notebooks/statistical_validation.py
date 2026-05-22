# Databricks notebook source

# DBTITLE 1, Statistical Validation — Headcount Anomaly Detection
# Runs after Silver pipeline completes, before Gold refreshes.
# Compares current headcount against 3-month rolling average.
# If variance exceeds threshold, raises an alert before Gold refreshes.

# COMMAND ----------

import json
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1, Parameters
dbutils.widgets.text("catalog", "ironclad_hr")
dbutils.widgets.text("period", "")
dbutils.widgets.text("quality_rules_path", "/Workspace/Users/benjaminstringer1994@gmail.com/ironclad-hr/config/quality_rules.json")

catalog = dbutils.widgets.get("catalog")
period = dbutils.widgets.get("period")
quality_rules_path = dbutils.widgets.get("quality_rules_path")

# COMMAND ----------

# DBTITLE 1, Load threshold from config
with open(quality_rules_path, "r") as f:
    quality_rules = json.load(f)

variance_threshold_pct = quality_rules.get("thresholds", {}).get("headcount_variance_pct", 10)
print(f"Headcount variance threshold: {variance_threshold_pct}%")

# COMMAND ----------

# DBTITLE 1, Compute current and rolling average headcount
headcount_df = spark.table(f"{catalog}.silver.fact_headcount")

# Current period headcount
if period:
    current_period = period
else:
    current_period = headcount_df.agg(F.max("period")).collect()[0][0]

print(f"Validating period: {current_period}")

current_count = (
    headcount_df
    .filter(F.col("period") == current_period)
    .filter(F.col("termination_date").isNull())
    .select("employee_id")
    .distinct()
    .count()
)

print(f"Current headcount: {current_count}")

# 3-month rolling average excluding current period
rolling_avg_df = (
    headcount_df
    .filter(F.col("period") < current_period)
    .filter(F.col("termination_date").isNull())
    .groupBy("period")
    .agg(F.countDistinct("employee_id").alias("headcount"))
    .orderBy(F.col("period").desc())
    .limit(3)
)

rolling_periods = rolling_avg_df.count()
print(f"Rolling periods available: {rolling_periods}")

# COMMAND ----------

# DBTITLE 1, Anomaly detection
if rolling_periods == 0:
    print("No historical data available for comparison. Skipping anomaly detection.")
else:
    rolling_avg = rolling_avg_df.agg(F.avg("headcount")).collect()[0][0]
    print(f"3-month rolling average headcount: {rolling_avg:.1f}")

    if rolling_avg > 0:
        variance_pct = abs(current_count - rolling_avg) / rolling_avg * 100
        print(f"Variance: {variance_pct:.1f}%")

        if variance_pct > variance_threshold_pct:
            alert_msg = (
                f"ANOMALY DETECTED: Headcount variance {variance_pct:.1f}% "
                f"exceeds threshold {variance_threshold_pct}%. "
                f"Current: {current_count}, Rolling avg: {rolling_avg:.1f}. "
                f"Period: {current_period}. "
                f"Review before Gold layer refreshes."
            )
            print(f"WARNING: {alert_msg}")
            # In production this would trigger a Lakeflow Job alert
            # For now raise an exception to halt the pipeline task
            raise Exception(alert_msg)
        else:
            print(f"Headcount variance within acceptable range. Proceeding to Gold refresh.")
    else:
        print("Rolling average is zero. Skipping variance check.")
