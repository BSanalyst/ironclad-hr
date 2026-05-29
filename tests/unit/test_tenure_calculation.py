"""
Unit tests for tenure_days calculation and tenure_band assignment.

These tests cover the logic added as part of the tenure banding
CI/CD demonstration feature. They run in local Spark mode.
"""
import pytest
from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def calculate_tenure_days(hire_date_str, period_date_str):
    """
    Calculate number of days between hire_date and period_date.
    Returns None if either date is None or if result would be negative
    (hire_date after period_date is a data error).
    """
    if hire_date_str is None or period_date_str is None:
        return None
    hire_date = date.fromisoformat(hire_date_str[:10])
    period_date = date.fromisoformat(period_date_str[:10])
    days = (period_date - hire_date).days
    return None if days < 0 else days


def calculate_tenure_band(tenure_days):
    """
    Assign tenure band from tenure_days.
    Bands defined in master reference:
      0-1 year:  0–364 days
      1-3 years: 365–1094 days
      3-5 years: 1095–1824 days
      5+ years:  1825+ days
    """
    if tenure_days is None:
        return None
    if tenure_days < 365:
        return "0-1 year"
    elif tenure_days < 1095:
        return "1-3 years"
    elif tenure_days < 1825:
        return "3-5 years"
    else:
        return "5+ years"


def spark_tenure_days(spark, hire_date_str, period_date_str):
    """Apply tenure_days logic via Spark DataFrame (mirrors pipeline logic)."""
    df = spark.createDataFrame(
        [(hire_date_str, period_date_str)],
        ["hire_date", "period_date_str"]
    )
    return (
        df
        .withColumn("hire_parsed", F.to_date(F.col("hire_date"), "yyyy-MM-dd"))
        .withColumn("period_parsed", F.to_date(F.col("period_date_str"), "yyyy-MM-dd"))
        .withColumn("tenure_days_raw",
            F.datediff(F.col("period_parsed"), F.col("hire_parsed"))
        )
        .withColumn("tenure_days",
            F.when(F.col("tenure_days_raw") < 0, F.lit(None).cast(IntegerType()))
             .otherwise(F.col("tenure_days_raw"))
        )
        .withColumn("tenure_band",
            F.when(F.col("tenure_days").isNull(), F.lit(None))
             .when(F.col("tenure_days") < 365, "0-1 year")
             .when(F.col("tenure_days") < 1095, "1-3 years")
             .when(F.col("tenure_days") < 1825, "3-5 years")
             .otherwise("5+ years")
        )
        .first()
    )


class TestTenureDaysCalculation:
    def test_two_years_with_leap_year(self):
        """2023-01-01 to 2025-01-01 spans leap year 2024 → 731 days."""
        days = calculate_tenure_days("2023-01-01", "2025-01-01")
        assert days == 731

    def test_six_months(self):
        """2024-07-01 to 2025-01-01 = 184 days."""
        days = calculate_tenure_days("2024-07-01", "2025-01-01")
        assert days == 184

    def test_five_plus_years(self):
        """2020-01-01 to 2025-01-01 spans two leap years → 1827 days."""
        days = calculate_tenure_days("2020-01-01", "2025-01-01")
        assert days == 1827

    def test_three_to_five_years(self):
        """2021-07-01 to 2025-01-01 = 1280 days."""
        days = calculate_tenure_days("2021-07-01", "2025-01-01")
        assert days == 1280

    def test_hire_after_period_date_returns_none(self):
        """Hire date after period date is a data error → None."""
        days = calculate_tenure_days("2025-06-01", "2025-01-01")
        assert days is None

    def test_none_hire_date_returns_none(self):
        days = calculate_tenure_days(None, "2025-01-01")
        assert days is None

    def test_none_period_date_returns_none(self):
        days = calculate_tenure_days("2023-01-01", None)
        assert days is None

    def test_same_day_hire_and_period_is_zero(self):
        days = calculate_tenure_days("2025-01-01", "2025-01-01")
        assert days == 0


class TestTenureBandAssignment:
    def test_zero_days_is_under_one_year(self):
        assert calculate_tenure_band(0) == "0-1 year"

    def test_364_days_is_under_one_year(self):
        assert calculate_tenure_band(364) == "0-1 year"

    def test_365_days_is_one_to_three_years(self):
        """365 days is exactly the boundary: 0-1 year ends at 364."""
        assert calculate_tenure_band(365) == "1-3 years"

    def test_1094_days_is_one_to_three_years(self):
        assert calculate_tenure_band(1094) == "1-3 years"

    def test_1095_days_is_three_to_five_years(self):
        """1095 days = exactly 3 years boundary."""
        assert calculate_tenure_band(1095) == "3-5 years"

    def test_1824_days_is_three_to_five_years(self):
        assert calculate_tenure_band(1824) == "3-5 years"

    def test_1825_days_is_five_plus_years(self):
        """1825 days = exactly 5 years boundary."""
        assert calculate_tenure_band(1825) == "5+ years"

    def test_5000_days_is_five_plus_years(self):
        assert calculate_tenure_band(5000) == "5+ years"

    def test_none_tenure_days_returns_none_band(self):
        assert calculate_tenure_band(None) is None


class TestTenureSparkImplementation:
    """Verify the Spark DataFrame logic matches the Python logic."""

    def test_spark_tenure_matches_python_2years(self, spark):
        row = spark_tenure_days(spark, "2023-01-01", "2025-01-01")
        assert row["tenure_days"] == 731
        assert row["tenure_band"] == "1-3 years"

    def test_spark_tenure_matches_python_6months(self, spark):
        row = spark_tenure_days(spark, "2024-07-01", "2025-01-01")
        assert row["tenure_days"] == 184
        assert row["tenure_band"] == "0-1 year"

    def test_spark_nulls_negative_tenure(self, spark):
        """hire_date after period_date → tenure_days should be NULL in Spark."""
        row = spark_tenure_days(spark, "2025-06-01", "2025-01-01")
        assert row["tenure_days"] is None
        assert row["tenure_band"] is None

    def test_spark_five_plus_years(self, spark):
        row = spark_tenure_days(spark, "2020-01-01", "2025-01-01")
        assert row["tenure_days"] == 1827
        assert row["tenure_band"] == "5+ years"
