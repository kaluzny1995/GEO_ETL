import datetime as dt
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from jobs.data_quality import run


class DummyModel(BaseModel):
    name: str


def _mock_app_config():
    cfg = MagicMock()
    cfg.database.db_schema = "geo"
    cfg.database.jdbc_driver = "org.postgresql.Driver"
    return cfg


def _mock_db_config():
    cfg = MagicMock()
    cfg.get_jdbc_url.return_value = "jdbc:postgresql://db.example.com:5432/postgres"
    cfg.get_jdbc_properties.return_value = {
        "user": "user",
        "password": "pass",
        "driver": "org.postgresql.Driver",
    }
    return cfg


def _metric_row_count():
    return 725


def _setup_metrics(mock_dq, date_count=2):
    """Wire the mocked metrics DataFrame for the cache/count/write path.

    ``data_quality_rows`` must stay inside Spark: building the frame from a
    Python list would require Python workers on the executors, which the
    Docker-based cluster does not have.
    """
    df = mock_dq.return_value
    df.cache.return_value = df
    df.count.return_value = _metric_row_count()
    df.select.return_value.distinct.return_value.count.return_value = date_count
    return df


PURGE_SQL = (
    "DELETE FROM geo.data_quality AS q WHERE q.table_name = 'avalanche_org' "
    "AND NOT EXISTS (SELECT 1 FROM geo.avalanche_org s WHERE s.file_date = q.file_date)"
)


@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.read.jdbc.return_value = MagicMock()
    return spark


@patch("jobs.data_quality.data_quality_rows")
@patch("jobs.data_quality._max_stored_file_date", return_value=None)
@patch("jobs.data_quality.execute_statement")
@patch("jobs.data_quality.execute_ddl")
@patch("jobs.data_quality.create_schema")
@patch("jobs.data_quality.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
@patch("jobs.data_quality.AppConfig.from_json", return_value=_mock_app_config())
@patch("jobs.data_quality.TableFactory.get", return_value=DummyModel)
class TestHistoricalRun:
    """No metrics stored yet → every file_date is measured."""

    def test_returns_success_message(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _setup_metrics(mock_dq)

        result = run(mock_spark, "avalanche_org")

        assert "avalanche_org" in result
        assert "data_quality" in result
        assert "HISTORICAL" in result

    def test_creates_geo_schema(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _setup_metrics(mock_dq)

        run(mock_spark, "avalanche_org")

        mock_schema.assert_called_once()
        assert mock_schema.call_args[0][1] == "geo"

    def test_executes_ddl_for_metrics_table(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _setup_metrics(mock_dq)

        run(mock_spark, "avalanche_org")

        mock_ddl.assert_called_once()
        ddl = mock_ddl.call_args[0][1]
        assert "CREATE TABLE IF NOT EXISTS geo.data_quality" in ddl

    def test_reads_source_table(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _setup_metrics(mock_dq)

        run(mock_spark, "avalanche_org")

        assert mock_spark.read.jdbc.call_count == 1
        assert mock_spark.read.jdbc.call_args.kwargs["table"] == "geo.avalanche_org"

    def test_no_file_date_filter(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _setup_metrics(mock_dq)

        run(mock_spark, "avalanche_org")

        mock_spark.read.jdbc.return_value.filter.assert_not_called()

    def test_deletes_all_stored_metrics(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _setup_metrics(mock_dq)

        run(mock_spark, "avalanche_org")

        mock_delete.assert_called_once()
        assert mock_delete.call_args[0][1] == "DELETE FROM geo.data_quality WHERE table_name = 'avalanche_org'"

    def test_writes_metrics(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        metrics_df = _setup_metrics(mock_dq)

        result = run(mock_spark, "avalanche_org")

        mock_spark.createDataFrame.assert_not_called()
        metrics_df.write.jdbc.assert_called_once()
        assert metrics_df.write.jdbc.call_args.kwargs["table"] == "geo.data_quality"
        assert metrics_df.write.jdbc.call_args.kwargs["mode"] == "append"
        metrics_df.unpersist.assert_called_once()
        assert f"{_metric_row_count()} metrics" in result

    def test_empty_source_returns_no_data_message(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        mock_spark.read.jdbc.return_value.head.return_value = []

        result = run(mock_spark, "avalanche_org")

        assert "No data in geo.avalanche_org" in result
        mock_dq.assert_not_called()
        mock_spark.createDataFrame.assert_not_called()
        mock_delete.assert_not_called()

    def test_logs_steps(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _setup_metrics(mock_dq)
        mock_logger = MagicMock()

        run(mock_spark, "avalanche_org", logger=mock_logger)

        mock_logger.info.assert_any_call("Creating table 'geo.data_quality' if not exists...")
        mock_logger.info.assert_any_call("No metrics stored for avalanche_org → HISTORICAL: measuring every file_date")
        mock_logger.info.assert_any_call("Reading geo.avalanche_org...")
        mock_logger.info.assert_any_call(
            f"Computed {_metric_row_count()} metrics for 2 file_date(s) in avalanche_org (HISTORICAL)"
        )
        mock_logger.info.assert_any_call("Replacing stored metrics for avalanche_org...")
        mock_logger.info.assert_any_call("Writing to geo.data_quality...")

    def test_default_logger_used_when_missing(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _setup_metrics(mock_dq)

        result = run(mock_spark, "avalanche_org")

        assert "avalanche_org" in result


@patch("jobs.data_quality.data_quality_rows")
@patch("jobs.data_quality._max_stored_file_date", return_value=dt.date(2026, 9, 15))
@patch("jobs.data_quality.execute_statement")
@patch("jobs.data_quality.execute_ddl")
@patch("jobs.data_quality.create_schema")
@patch("jobs.data_quality.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
@patch("jobs.data_quality.AppConfig.from_json", return_value=_mock_app_config())
@patch("jobs.data_quality.TableFactory.get", return_value=DummyModel)
class TestIncrementalRun:
    """Metrics already stored → only newer file_dates are measured."""

    def test_filters_newer_file_dates(
        self,
        mock_get,
        mock_app,
        mock_db,
        mock_schema,
        mock_ddl,
        mock_delete,
        mock_max,
        mock_dq,
        mock_spark,
        spark_session,
    ):
        _setup_metrics(mock_dq)

        run(mock_spark, "avalanche_org")

        mock_spark.read.jdbc.return_value.filter.assert_called_once()
        expr = str(mock_spark.read.jdbc.return_value.filter.call_args[0][0])
        assert "file_date" in expr
        assert "2026-09-15" in expr

    def test_purges_metrics_for_removed_file_dates(
        self,
        mock_get,
        mock_app,
        mock_db,
        mock_schema,
        mock_ddl,
        mock_delete,
        mock_max,
        mock_dq,
        mock_spark,
        spark_session,
    ):
        _setup_metrics(mock_dq)

        run(mock_spark, "avalanche_org")

        assert mock_delete.call_args_list[0][0][1] == PURGE_SQL

    def test_delete_scoped_to_new_file_dates(
        self,
        mock_get,
        mock_app,
        mock_db,
        mock_schema,
        mock_ddl,
        mock_delete,
        mock_max,
        mock_dq,
        mock_spark,
        spark_session,
    ):
        _setup_metrics(mock_dq)

        run(mock_spark, "avalanche_org")

        assert mock_delete.call_count == 2
        assert (
            mock_delete.call_args_list[1][0][1]
            == "DELETE FROM geo.data_quality WHERE table_name = 'avalanche_org' AND file_date > '2026-09-15'"
        )

    def test_returns_incremental_success_message(
        self,
        mock_get,
        mock_app,
        mock_db,
        mock_schema,
        mock_ddl,
        mock_delete,
        mock_max,
        mock_dq,
        mock_spark,
        spark_session,
    ):
        _setup_metrics(mock_dq)

        result = run(mock_spark, "avalanche_org")

        assert "INCREMENTAL" in result
        assert "avalanche_org" in result
        assert "data_quality" in result

    def test_no_new_file_date_skips_computation(
        self,
        mock_get,
        mock_app,
        mock_db,
        mock_schema,
        mock_ddl,
        mock_delete,
        mock_max,
        mock_dq,
        mock_spark,
        spark_session,
    ):
        mock_spark.read.jdbc.return_value.filter.return_value.head.return_value = []

        result = run(mock_spark, "avalanche_org")

        assert "No new file_date for avalanche_org" in result
        mock_dq.assert_not_called()
        mock_spark.createDataFrame.assert_not_called()

    def test_purge_runs_even_without_new_data(
        self,
        mock_get,
        mock_app,
        mock_db,
        mock_schema,
        mock_ddl,
        mock_delete,
        mock_max,
        mock_dq,
        mock_spark,
        spark_session,
    ):
        mock_spark.read.jdbc.return_value.filter.return_value.head.return_value = []

        run(mock_spark, "avalanche_org")

        mock_delete.assert_called_once()
        assert mock_delete.call_args[0][1] == PURGE_SQL

    def test_logs_load_type(
        self,
        mock_get,
        mock_app,
        mock_db,
        mock_schema,
        mock_ddl,
        mock_delete,
        mock_max,
        mock_dq,
        mock_spark,
        spark_session,
    ):
        _setup_metrics(mock_dq)
        mock_logger = MagicMock()

        run(mock_spark, "avalanche_org", logger=mock_logger)

        mock_logger.info.assert_any_call(
            "Latest measured file_date: 2026-09-15 → INCREMENTAL: measuring file_date > 2026-09-15"
        )
        mock_logger.info.assert_any_call("Purging metrics for file_dates no longer present in geo.avalanche_org...")

    def test_writes_only_new_metrics(
        self,
        mock_get,
        mock_app,
        mock_db,
        mock_schema,
        mock_ddl,
        mock_delete,
        mock_max,
        mock_dq,
        mock_spark,
        spark_session,
    ):
        metrics_df = _setup_metrics(mock_dq)

        run(mock_spark, "avalanche_org")

        metrics_df.write.jdbc.assert_called_once()
        assert metrics_df.write.jdbc.call_args.kwargs["mode"] == "append"
        mock_spark.createDataFrame.assert_not_called()


def test_unknown_table_raises(mock_spark):
    with pytest.raises(ValueError, match="Unknown table"):
        run(mock_spark, "nonexistent_table")
