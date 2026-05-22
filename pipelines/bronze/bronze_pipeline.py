# Databricks notebook source
import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1, Configuration
landing_base = spark.conf.get(
    "landing_base",
    "/Volumes/ironclad_hr/bronze/landing"
)

# COMMAND ----------

# DBTITLE 1, Helper — create Bronze streaming table from Auto Loader
def create_bronze_table(entity_name, landing_base):
    """
    Creates a Bronze streaming table for a given HR entity.
    - Reads from landing volume using Auto Loader
    - Schema evolution mode: rescue (new columns land in _rescued_data)
    - Appends three metadata columns for audit and debugging
    - No transformations, no expectations, no business rules
    """
    source_path = f"{landing_base}/*/{entity_name}/"

    @dlt.table(
        name=f"bronze_{entity_name}",
        comment=f"Bronze raw landing table for {entity_name}. Append only. No transformations.",
        table_properties={
            "quality": "bronze",
            "pipelines.autoOptimize.managed": "true"
        }
    )
    def bronze_table():
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("cloudFiles.inferColumnTypes", "false")
            .load(source_path)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file_name", F.col("_metadata.file_path"))
            .withColumn("_pipeline_run_id", F.expr("uuid()"))
        )

    return bronze_table

# COMMAND ----------

# DBTITLE 1, Bronze streaming tables — one per entity
create_bronze_table("employees", landing_base)
create_bronze_table("departments", landing_base)
create_bronze_table("job_roles", landing_base)
create_bronze_table("contracts", landing_base)
create_bronze_table("compensation", landing_base)
create_bronze_table("payroll_events", landing_base)
