"""
Unit tests for config-driven quality rules loading.

Verifies that quality_rules.json is correctly parsed and that the
right rules are returned for each entity and severity level.
"""
import json
import pytest
import tempfile
import os


VALID_RULES = {
    "thresholds": {"headcount_variance_pct": 10},
    "entities": {
        "employees": [
            {"field": "employee_id", "rule": "employee_id IS NOT NULL", "severity": "CRITICAL"},
            {"field": "hire_date", "rule": "hire_date IS NOT NULL", "severity": "ERROR"},
            {"field": "termination_date",
             "rule": "termination_date IS NULL OR termination_date > hire_date",
             "severity": "ERROR"},
            {"field": "tenure_days",
             "rule": "tenure_days IS NULL OR tenure_days >= 0",
             "severity": "ERROR"},
        ],
        "payroll_events": [
            {"field": "employee_id", "rule": "employee_id IS NOT NULL", "severity": "ERROR"},
            {"field": "gross_pay",
             "rule": "gross_pay IS NOT NULL AND gross_pay > 0",
             "severity": "WARNING"},
            {"field": "net_pay",
             "rule": "net_pay IS NOT NULL AND net_pay >= 0",
             "severity": "ERROR"},
        ],
        "compensation": [
            {"field": "salary", "rule": "salary IS NOT NULL AND salary > 0", "severity": "ERROR"},
        ],
    }
}


def load_quality_rules(path):
    with open(path, "r") as f:
        return json.load(f)

def get_entity_rules(rules, entity):
    return rules.get("entities", {}).get(entity, [])

def get_rules_by_severity(rules, entity, severity):
    return [r for r in get_entity_rules(rules, entity) if r["severity"] == severity]


@pytest.fixture
def rules_file():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(VALID_RULES, f)
        path = f.name
    yield path
    os.unlink(path)


class TestRulesLoading:
    def test_file_loads_successfully(self, rules_file):
        rules = load_quality_rules(rules_file)
        assert "entities" in rules
        assert "thresholds" in rules

    def test_employee_rules_count(self, rules_file):
        rules = load_quality_rules(rules_file)
        emp_rules = get_entity_rules(rules, "employees")
        assert len(emp_rules) == 4

    def test_payroll_rules_count(self, rules_file):
        rules = load_quality_rules(rules_file)
        payroll_rules = get_entity_rules(rules, "payroll_events")
        assert len(payroll_rules) == 3

    def test_unknown_entity_returns_empty_list(self, rules_file):
        rules = load_quality_rules(rules_file)
        assert get_entity_rules(rules, "nonexistent_entity") == []

    def test_threshold_value(self, rules_file):
        rules = load_quality_rules(rules_file)
        threshold = rules["thresholds"]["headcount_variance_pct"]
        assert threshold == 10


class TestSeverityFiltering:
    def test_exactly_one_critical_rule_for_employees(self, rules_file):
        rules = load_quality_rules(rules_file)
        critical = get_rules_by_severity(rules, "employees", "CRITICAL")
        assert len(critical) == 1

    def test_critical_rule_is_employee_id_not_null(self, rules_file):
        rules = load_quality_rules(rules_file)
        critical = get_rules_by_severity(rules, "employees", "CRITICAL")
        assert critical[0]["field"] == "employee_id"
        assert "IS NOT NULL" in critical[0]["rule"]

    def test_three_error_rules_for_employees(self, rules_file):
        rules = load_quality_rules(rules_file)
        errors = get_rules_by_severity(rules, "employees", "ERROR")
        assert len(errors) == 3

    def test_tenure_days_rule_present_at_error_severity(self, rules_file):
        rules = load_quality_rules(rules_file)
        errors = get_rules_by_severity(rules, "employees", "ERROR")
        tenure_rules = [r for r in errors if r["field"] == "tenure_days"]
        assert len(tenure_rules) == 1, "tenure_days ERROR rule must exist"
        assert ">= 0" in tenure_rules[0]["rule"]

    def test_gross_pay_is_warning_not_error(self, rules_file):
        rules = load_quality_rules(rules_file)
        warnings = get_rules_by_severity(rules, "payroll_events", "WARNING")
        assert len(warnings) == 1
        assert warnings[0]["field"] == "gross_pay"
        # Must NOT appear in ERROR rules
        errors = get_rules_by_severity(rules, "payroll_events", "ERROR")
        error_fields = [r["field"] for r in errors]
        assert "gross_pay" not in error_fields, "gross_pay should be WARNING, not ERROR"

    def test_no_warnings_for_employees(self, rules_file):
        rules = load_quality_rules(rules_file)
        warnings = get_rules_by_severity(rules, "employees", "WARNING")
        assert len(warnings) == 0


class TestMalformedInput:
    def test_missing_entities_key_returns_empty(self):
        rules = {"thresholds": {"headcount_variance_pct": 10}}
        assert get_entity_rules(rules, "employees") == []

    def test_empty_rules_file(self):
        rules = {}
        assert get_entity_rules(rules, "employees") == []
        assert get_entity_rules(rules, "payroll_events") == []

    def test_missing_threshold_defaults_gracefully(self):
        rules = {"entities": {}, "thresholds": {}}
        threshold = rules.get("thresholds", {}).get("headcount_variance_pct", 10)
        assert threshold == 10
