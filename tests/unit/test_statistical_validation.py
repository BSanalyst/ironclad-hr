"""
Unit tests for statistical anomaly detection logic.

Tests that the headcount variance check fires at the correct threshold
and does not fire when variance is within acceptable bounds.
"""
import pytest


def compute_variance_pct(current_count, rolling_avg):
    """Compute percentage variance from rolling average."""
    if rolling_avg is None or rolling_avg == 0:
        return None
    return abs(current_count - rolling_avg) / rolling_avg * 100


def is_anomaly(current_count, rolling_avg, threshold_pct=10):
    """Returns True if variance exceeds threshold."""
    variance = compute_variance_pct(current_count, rolling_avg)
    if variance is None:
        return False
    return variance > threshold_pct


def compute_rolling_avg(headcounts):
    """Compute rolling average of a list of headcounts (3-month window)."""
    if not headcounts:
        return None
    return sum(headcounts) / len(headcounts)


class TestVarianceCalculation:
    def test_11_percent_drop_is_anomaly(self):
        avg = 100
        current = 89  # 11% drop
        assert is_anomaly(current, avg, threshold_pct=10)

    def test_8_percent_drop_is_not_anomaly(self):
        avg = 100
        current = 92  # 8% drop
        assert not is_anomaly(current, avg, threshold_pct=10)

    def test_exactly_10_percent_is_not_anomaly(self):
        """10% exactly is AT the threshold, not above it."""
        avg = 100
        current = 90  # exactly 10%
        assert not is_anomaly(current, avg, threshold_pct=10)

    def test_10_point_1_percent_is_anomaly(self):
        avg = 1000
        current = 899  # 10.1% drop
        assert is_anomaly(current, avg, threshold_pct=10)

    def test_increase_also_triggers_anomaly(self):
        """Unexpected headcount increase should also be flagged."""
        avg = 100
        current = 115  # 15% increase
        assert is_anomaly(current, avg, threshold_pct=10)

    def test_zero_rolling_avg_returns_no_anomaly(self):
        """No prior data — cannot compute variance."""
        assert not is_anomaly(100, 0, threshold_pct=10)
        assert not is_anomaly(100, None, threshold_pct=10)


class TestRollingAverage:
    def test_three_month_rolling_avg(self):
        avg = compute_rolling_avg([100, 102, 98])
        assert abs(avg - 100.0) < 0.01

    def test_single_month_rolling_avg(self):
        avg = compute_rolling_avg([95])
        assert avg == 95.0

    def test_empty_history_returns_none(self):
        avg = compute_rolling_avg([])
        assert avg is None


class TestCustomThreshold:
    def test_custom_5_percent_threshold(self):
        """Pipeline supports configurable threshold from quality_rules.json."""
        avg = 100
        assert is_anomaly(94, avg, threshold_pct=5)    # 6% drop — above 5% threshold
        assert not is_anomaly(96, avg, threshold_pct=5)  # 4% drop — within 5% threshold

    def test_custom_15_percent_threshold(self):
        avg = 100
        assert not is_anomaly(89, avg, threshold_pct=15)  # 11% drop — within 15% threshold
        assert is_anomaly(84, avg, threshold_pct=15)       # 16% drop — above 15% threshold
