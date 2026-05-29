"""
Unit tests for SCD Type 2 merge logic.

Tests the expected behaviour of AUTO CDC FROM SNAPSHOT by simulating
the before/after state that the Silver pipeline produces.
"""
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, TimestampType
)
from pyspark.sql.window import Window
from datetime import datetime


SCD_SCHEMA = StructType([
    StructField("employee_id", StringType(), True),
    StructField("department_id", StringType(), True),
    StructField("role_id", StringType(), True),
    StructField("__START_AT", TimestampType(), True),
    StructField("__END_AT", TimestampType(), True),
    StructField("__IS_CURRENT", BooleanType(), True),
])

SNAPSHOT_SCHEMA = StructType([
    StructField("employee_id", StringType(), True),
    StructField("department_id", StringType(), True),
    StructField("role_id", StringType(), True),
])

KEY_COLS = ["employee_id"]
VALUE_COLS = ["department_id", "role_id"]
NOW = datetime(2025, 3, 1, 0, 0, 0)
JAN = datetime(2025, 1, 1, 0, 0, 0)
FEB = datetime(2025, 2, 1, 0, 0, 0)


def make_existing(spark, rows, is_current=True, start=JAN, end=None):
    """Create a dimension DataFrame with SCD Type 2 columns."""
    data = [
        (r[0], r[1], r[2], start, end, is_current)
        for r in rows
    ]
    return spark.createDataFrame(data, SCD_SCHEMA)


def make_snapshot(spark, rows):
    """Create a snapshot DataFrame (no SCD columns)."""
    return spark.createDataFrame(rows, SNAPSHOT_SCHEMA)


def simulate_scd_merge(spark, existing_df, new_snapshot_df, key_cols, value_cols):
    """
    Simulates what AUTO CDC FROM SNAPSHOT does for SCD Type 2.

    Logic:
    - Rows in snapshot not in existing → INSERT as current
    - Rows in existing and snapshot with changed values → CLOSE old, INSERT new
    - Rows in existing and snapshot with same values → KEEP unchanged
    - Historical rows (already closed) → PASS THROUGH
    """
    all_cols = key_cols + value_cols + ["__START_AT", "__END_AT", "__IS_CURRENT"]
    current = existing_df.filter(F.col("__IS_CURRENT") == True)
    historical = existing_df.filter(F.col("__IS_CURRENT") == False)

    # Detect new employees (in snapshot, not in current)
    new_only = (
        new_snapshot_df
        .join(current.select(key_cols), on=key_cols, how="left_anti")
        .withColumn("__START_AT", F.lit(NOW).cast(TimestampType()))
        .withColumn("__END_AT", F.lit(None).cast(TimestampType()))
        .withColumn("__IS_CURRENT", F.lit(True))
    )

    # Detect changes among employees in both current and snapshot
    def row_hash(df, alias):
        return df.withColumn(
            f"_hash",
            F.md5(F.concat_ws("|", *[
                F.coalesce(F.col(c).cast(StringType()), F.lit("__NULL__"))
                for c in value_cols
            ]))
        ).alias(alias)

    current_hashed = row_hash(current.join(new_snapshot_df.select(key_cols), on=key_cols), "c")
    snapshot_hashed = row_hash(new_snapshot_df.join(current.select(key_cols), on=key_cols), "s")

    changed_keys = (
        current_hashed.join(snapshot_hashed, on=key_cols)
        .filter(F.col("c._hash") != F.col("s._hash"))
        .select(*[F.col(f"c.{k}") for k in key_cols])
    )

    # Close rows that changed
    closed = (
        current.join(changed_keys, on=key_cols)
        .withColumn("__END_AT", F.lit(NOW).cast(TimestampType()))
        .withColumn("__IS_CURRENT", F.lit(False))
    )

    # Insert new versions of changed rows
    new_versions = (
        new_snapshot_df.join(changed_keys, on=key_cols)
        .withColumn("__START_AT", F.lit(NOW).cast(TimestampType()))
        .withColumn("__END_AT", F.lit(None).cast(TimestampType()))
        .withColumn("__IS_CURRENT", F.lit(True))
    )

    # Unchanged current rows: in both, no change detected
    unchanged = (
        current
        .join(changed_keys, on=key_cols, how="left_anti")
        .join(new_snapshot_df.select(key_cols), on=key_cols)
    )

    return (
        historical.select(all_cols)
        .union(unchanged.select(all_cols))
        .union(closed.select(all_cols))
        .union(new_versions.select(all_cols))
        .union(new_only.select(all_cols))
    )


class TestSCDMergeNoChange:
    def test_identical_snapshot_produces_no_new_rows(self, spark):
        existing = make_existing(spark, [["EMP00001","D001","R001"]])
        snapshot = make_snapshot(spark, [["EMP00001","D001","R001"]])
        result = simulate_scd_merge(spark, existing, snapshot, KEY_COLS, VALUE_COLS)
        assert result.count() == 1
        assert result.filter(F.col("__IS_CURRENT") == True).count() == 1
        assert result.filter(F.col("__IS_CURRENT") == False).count() == 0


class TestSCDMergeWithChange:
    def test_changed_department_creates_history_row(self, spark):
        existing = make_existing(spark, [["EMP00001","D001","R001"]])
        snapshot = make_snapshot(spark, [["EMP00001","D003","R001"]])
        result = simulate_scd_merge(spark, existing, snapshot, KEY_COLS, VALUE_COLS)
        assert result.count() == 2
        current_rows = result.filter(F.col("__IS_CURRENT") == True)
        history_rows = result.filter(F.col("__IS_CURRENT") == False)
        assert current_rows.count() == 1
        assert history_rows.count() == 1
        assert current_rows.first()["department_id"] == "D003"
        assert history_rows.first()["department_id"] == "D001"
        assert history_rows.first()["__END_AT"] is not None

    def test_changed_role_creates_history_row(self, spark):
        existing = make_existing(spark, [["EMP00002","D001","R001"]])
        snapshot = make_snapshot(spark, [["EMP00002","D001","R002"]])
        result = simulate_scd_merge(spark, existing, snapshot, KEY_COLS, VALUE_COLS)
        assert result.count() == 2
        current_rows = result.filter(F.col("__IS_CURRENT") == True)
        assert current_rows.first()["role_id"] == "R002"


class TestSCDMergeNewEmployee:
    def test_new_employee_inserts_current_row(self, spark):
        existing = make_existing(spark, [["EMP00001","D001","R001"]])
        snapshot = make_snapshot(spark, [
            ["EMP00001","D001","R001"],
            ["EMP00002","D002","R002"],
        ])
        result = simulate_scd_merge(spark, existing, snapshot, KEY_COLS, VALUE_COLS)
        emp2_rows = result.filter(F.col("employee_id") == "EMP00002")
        assert emp2_rows.count() == 1
        new_emp = emp2_rows.first()
        assert new_emp["__IS_CURRENT"] is True
        assert new_emp["__END_AT"] is None


class TestSCDMergeHistoryPreserved:
    def test_pre_existing_history_rows_are_preserved(self, spark):
        hist_schema = StructType([
            StructField("employee_id", StringType(), True),
            StructField("department_id", StringType(), True),
            StructField("role_id", StringType(), True),
            StructField("__START_AT", TimestampType(), True),
            StructField("__END_AT", TimestampType(), True),
            StructField("__IS_CURRENT", BooleanType(), True),
        ])
        historical = spark.createDataFrame(
            [("EMP00001","D001","R001", JAN, FEB, False)], hist_schema
        )
        current_row = spark.createDataFrame(
            [("EMP00001","D002","R001", FEB, None, True)], hist_schema
        )
        existing = historical.union(current_row)
        snapshot = make_snapshot(spark, [["EMP00001","D002","R001"]])
        result = simulate_scd_merge(spark, existing, snapshot, KEY_COLS, VALUE_COLS)
        assert result.count() == 2
        assert result.filter(F.col("__IS_CURRENT") == False).count() == 1
        assert result.filter(F.col("__IS_CURRENT") == True).count() == 1
