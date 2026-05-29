"""
pytest configuration and shared fixtures for Ironclad HR test suite.

Unit tests run in local Spark mode — no Databricks cluster, no Unity Catalog,
no cloud storage required. Fast execution in GitHub Actions CI on every PR.
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Local Spark session for unit tests.
    Single session shared across all tests in a pytest run for efficiency.
    """
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("ironclad-hr-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
