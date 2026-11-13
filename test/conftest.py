import sys
from unittest.mock import MagicMock

import pytest


def mock_F_upload(extra_col_methods=None, extra_f_methods=None):
    f_mock = MagicMock()

    def col_side_effect(name):
        col_mock = MagicMock()
        col_mock.__gt__ = MagicMock(return_value=MagicMock())
        if extra_col_methods:
            for method_name in extra_col_methods:
                setattr(col_mock, method_name, MagicMock(return_value=col_mock))
        return col_mock

    f_mock.col.side_effect = col_side_effect
    f_mock.lit.return_value = MagicMock()
    if extra_f_methods:
        for method_name in extra_f_methods:
            setattr(f_mock, method_name, MagicMock(return_value=f_mock))
    return f_mock


def mock_app_config_upload():
    cfg = MagicMock()
    cfg.data.directory = "../GEO_Download/data"
    cfg.logging.directory = "logs"
    cfg.logging.level_int = 20
    cfg.database.db_schema = "geo"
    cfg.database.jdbc_driver = "org.postgresql.Driver"
    return cfg


def mock_db_config(driver: bool = True) -> MagicMock:
    """SupabaseConnectionConfig mock: the JDBC URL + properties every job test shares.

    Args:
        driver: Include the JDBC driver property (all tests but the shared
            single-ETL behaviour suite, whose frame mocks need it absent).

    Returns:
        The configured MagicMock.
    """
    cfg = MagicMock()
    cfg.get_jdbc_url.return_value = "jdbc:postgresql://db.example.com:5432/postgres"
    properties = {"user": "user", "password": "pass"}
    if driver:
        properties["driver"] = "org.postgresql.Driver"
    cfg.get_jdbc_properties.return_value = properties
    return cfg


def mock_app_config(
    *,
    database: bool = True,
    logging: bool = False,
    spark: str | None = None,
    data_directory: str | None = None,
) -> MagicMock:
    """AppConfig mock shared by the job and script tests.

    Each scene of the suite reads a different subset of the config — the
    keywords keep every test on exactly the fields it had when the helpers
    lived per test file.

    Args:
        database: Set the ``geo`` schema and JDBC driver on ``cfg.database``.
        logging: Set ``logs`` / level 20 on ``cfg.logging``.
        spark: Set ``cfg.spark`` (master + app-name prefix) when given.
        data_directory: Set ``cfg.data.directory`` when given.

    Returns:
        The configured MagicMock.
    """
    cfg = MagicMock()
    if database:
        cfg.database.db_schema = "geo"
        cfg.database.jdbc_driver = "org.postgresql.Driver"
    if logging:
        cfg.logging.directory = "logs"
        cfg.logging.level_int = 20
    if spark is not None:
        cfg.spark.master = "spark://localhost:7078"
        cfg.spark.app_name_prefix = spark
    if data_directory is not None:
        cfg.data.directory = data_directory
    return cfg


def mock_spark_upload(max_file_date=None, count=10):
    spark = MagicMock()
    spark.sparkContext.master = "spark://localhost:7078"

    df = MagicMock()
    df.count.return_value = count
    df.withColumn.return_value = df
    df.unionByName.return_value = df
    df.select.return_value = df

    jdbc_df = MagicMock()
    jdbc_df.first.return_value = MagicMock(max_file_date=max_file_date)

    spark.read.csv.return_value = df
    spark.read.jdbc.return_value = jdbc_df
    return spark, df


def _start_spark():
    """Start a local SparkSession, or skip the test when Spark is unavailable."""
    try:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[2]")
            .appName("geo_etl_tests")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .config("spark.pyspark.python", sys.executable)
            .config("spark.pyspark.driver.python", sys.executable)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as exc:  # pragma: no cover - only hit without a JVM
        import pytest

        pytest.skip(f"local SparkSession unavailable: {exc}")


@pytest.fixture(scope="session")
def spark_session():
    """Session-scoped local SparkSession for tests that execute DataFrames."""
    spark = _start_spark()
    yield spark
    spark.stop()
