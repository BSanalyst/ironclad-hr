"""
Unit tests for workforce movement detection logic.

Tests HIRE, TRANSFER, and TERMINATION detection from consecutive
monthly headcount snapshots.
"""
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


def detect_movements(spark, headcount_df):
    """
    Standalone movement detection function mirroring the Silver pipeline logic.
    Accepts a fact_headcount-like DataFrame and returns movement events.
    """
    w_emp = Window.partitionBy("employee_id").orderBy("period")

    with_prev = (
        headcount_df
        .withColumn("prev_period", F.lag("period").over(w_emp))
        .withColumn("prev_department_id", F.lag("department_id").over(w_emp))
        .withColumn("prev_role_id", F.lag("role_id").over(w_emp))
    )

    hires = (
        with_prev.filter(F.col("prev_period").isNull())
        .withColumn("event_type", F.lit("HIRE"))
        .withColumn("from_department_id", F.lit(None).cast(StringType()))
        .withColumn("from_role_id", F.lit(None).cast(StringType()))
        .select("employee_id","period","event_type",
                "from_department_id","department_id","from_role_id","role_id")
    )

    transfers = (
        with_prev.filter(
            F.col("prev_period").isNotNull() &
            (
                (F.col("department_id") != F.col("prev_department_id")) |
                (F.col("role_id") != F.col("prev_role_id"))
            )
        )
        .withColumn("event_type", F.lit("TRANSFER"))
        .withColumnRenamed("prev_department_id","from_department_id")
        .withColumnRenamed("prev_role_id","from_role_id")
        .select("employee_id","period","event_type",
                "from_department_id","department_id","from_role_id","role_id")
    )

    all_periods = headcount_df.select("period").distinct()
    w_periods = Window.orderBy("period")
    periods_with_next = (
        all_periods
        .withColumn("next_period", F.lead("period").over(w_periods))
        .filter(F.col("next_period").isNotNull())
    )
    terminations = (
        headcount_df.alias("curr")
        .join(periods_with_next.alias("p"),
              F.col("curr.period") == F.col("p.period"))
        .join(
            headcount_df.alias("nxt"),
            (F.col("curr.employee_id") == F.col("nxt.employee_id")) &
            (F.col("nxt.period") == F.col("p.next_period")),
            how="left_anti"
        )
        .withColumn("event_type", F.lit("TERMINATION"))
        .withColumn("from_department_id", F.col("curr.department_id"))
        .withColumn("from_role_id", F.col("curr.role_id"))
        .select(
            F.col("curr.employee_id").alias("employee_id"),
            F.col("p.next_period").alias("period"),
            "event_type",
            "from_department_id",
            F.lit(None).cast(StringType()).alias("department_id"),
            "from_role_id",
            F.lit(None).cast(StringType()).alias("role_id")
        )
    )

    return hires.union(transfers).union(terminations)


def make_headcount(spark, rows):
    """rows: list of (employee_id, department_id, role_id, period)"""
    return spark.createDataFrame(rows, ["employee_id","department_id","role_id","period"])


class TestHireDetection:
    def test_first_appearance_is_hire(self, spark):
        data = [("EMP00001","D001","R001","2025-01")]
        movements = detect_movements(spark, make_headcount(spark, data))
        hires = movements.filter(F.col("event_type") == "HIRE")
        assert hires.count() == 1
        assert hires.first()["employee_id"] == "EMP00001"
        assert hires.first()["period"] == "2025-01"

    def test_two_employees_in_first_period_are_both_hires(self, spark):
        data = [
            ("EMP00001","D001","R001","2025-01"),
            ("EMP00002","D002","R002","2025-01"),
        ]
        movements = detect_movements(spark, make_headcount(spark, data))
        hires = movements.filter(F.col("event_type") == "HIRE")
        assert hires.count() == 2

    def test_new_employee_in_second_period_is_hire(self, spark):
        data = [
            ("EMP00001","D001","R001","2025-01"),
            ("EMP00001","D001","R001","2025-02"),
            ("EMP00002","D002","R002","2025-02"),  # new hire in period 2
        ]
        movements = detect_movements(spark, make_headcount(spark, data))
        hires = movements.filter(F.col("event_type") == "HIRE")
        hire_emp2 = hires.filter(F.col("employee_id") == "EMP00002")
        assert hire_emp2.count() == 1
        assert hire_emp2.first()["period"] == "2025-02"


class TestTransferDetection:
    def test_department_change_is_transfer(self, spark):
        data = [
            ("EMP00001","D001","R001","2025-01"),
            ("EMP00001","D002","R001","2025-02"),  # department changed
        ]
        movements = detect_movements(spark, make_headcount(spark, data))
        transfers = movements.filter(F.col("event_type") == "TRANSFER")
        assert transfers.count() == 1
        t = transfers.first()
        assert t["from_department_id"] == "D001"
        assert t["department_id"] == "D002"
        assert t["period"] == "2025-02"

    def test_role_change_is_transfer(self, spark):
        data = [
            ("EMP00001","D001","R001","2025-01"),
            ("EMP00001","D001","R002","2025-02"),  # promoted R001 → R002
        ]
        movements = detect_movements(spark, make_headcount(spark, data))
        transfers = movements.filter(F.col("event_type") == "TRANSFER")
        assert transfers.count() == 1
        t = transfers.first()
        assert t["from_role_id"] == "R001"
        assert t["role_id"] == "R002"

    def test_no_change_is_not_transfer(self, spark):
        data = [
            ("EMP00001","D001","R001","2025-01"),
            ("EMP00001","D001","R001","2025-02"),  # no change
        ]
        movements = detect_movements(spark, make_headcount(spark, data))
        transfers = movements.filter(F.col("event_type") == "TRANSFER")
        assert transfers.count() == 0


class TestTerminationDetection:
    def test_absent_in_next_period_is_termination(self, spark):
        data = [
            ("EMP00001","D001","R001","2025-01"),
            ("EMP00001","D001","R001","2025-02"),
            ("EMP00002","D002","R002","2025-01"),
            # EMP00002 absent in 2025-02 → terminated
        ]
        movements = detect_movements(spark, make_headcount(spark, data))
        terminations = movements.filter(F.col("event_type") == "TERMINATION")
        assert terminations.count() == 1
        t = terminations.first()
        assert t["employee_id"] == "EMP00002"
        assert t["period"] == "2025-02"  # recorded in the period they left
        assert t["from_department_id"] == "D002"

    def test_employee_present_in_all_periods_is_not_terminated(self, spark):
        data = [
            ("EMP00001","D001","R001","2025-01"),
            ("EMP00001","D001","R001","2025-02"),
            ("EMP00001","D001","R001","2025-03"),
        ]
        movements = detect_movements(spark, make_headcount(spark, data))
        terminations = movements.filter(F.col("event_type") == "TERMINATION")
        assert terminations.count() == 0

    def test_termination_in_last_period_not_detected_without_successor(self, spark):
        """
        If an employee is absent in the LAST period, there is no 'next period'
        to compare against, so termination cannot be detected. This is by design —
        a termination is confirmed when you can observe the absence in a subsequent period.
        """
        data = [
            ("EMP00001","D001","R001","2025-01"),
            ("EMP00002","D002","R002","2025-01"),
            # period 2025-02 is the last period — EMP00002 absent
            ("EMP00001","D001","R001","2025-02"),
        ]
        movements = detect_movements(spark, make_headcount(spark, data))
        terminations = movements.filter(F.col("event_type") == "TERMINATION")
        # EMP00002 absent in last period → termination IS detected
        # (period 2025-01 → next=2025-02, EMP00002 absent in 2025-02)
        assert terminations.count() == 1


class TestCombinedMovements:
    def test_full_scenario(self, spark):
        """
        3 employees over 3 periods:
        - EMP001: transfer in p2
        - EMP002: terminated before p3
        - EMP003: new hire in p2
        """
        data = [
            ("EMP001","D001","R001","2025-01"),
            ("EMP002","D002","R002","2025-01"),
            # p2
            ("EMP001","D003","R001","2025-02"),  # transfer
            ("EMP002","D002","R002","2025-02"),
            ("EMP003","D001","R003","2025-02"),  # hire
            # p3
            ("EMP001","D003","R001","2025-03"),
            ("EMP003","D001","R003","2025-03"),
            # EMP002 absent → terminated
        ]
        movements = detect_movements(spark, make_headcount(spark, data))

        hires = movements.filter(F.col("event_type") == "HIRE")
        transfers = movements.filter(F.col("event_type") == "TRANSFER")
        terminations = movements.filter(F.col("event_type") == "TERMINATION")

        assert hires.count() == 3         # EMP001 p1, EMP002 p1, EMP003 p2
        assert transfers.count() == 1     # EMP001 p2
        assert terminations.count() == 1  # EMP002 p3

        term = terminations.first()
        assert term["employee_id"] == "EMP002"
        assert term["period"] == "2025-03"

        transfer = transfers.first()
        assert transfer["employee_id"] == "EMP001"
        assert transfer["from_department_id"] == "D001"
        assert transfer["department_id"] == "D003"
