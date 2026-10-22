import datetime as dt
from types import SimpleNamespace
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


def _overall_metric_row_count():
    return 73


def _setup_metrics(mock_dq, date_count=2):
    """Wire two distinct mocked metrics DataFrames — dated and overall.

    ``data_quality_rows`` is called twice on a normal run (per-``file_date``
    with ``overall=False``, then the aggregate refresh with ``overall=True``),
    so the factory returns a different frame per mode. ``data_quality_rows``
    must stay inside Spark: building the frame from a Python list would
    require Python workers on the executors, which the Docker-based cluster
    does not have.
    """
    dated_df = MagicMock()
    dated_df.cache.return_value = dated_df
    dated_df.count.return_value = _metric_row_count()
    dated_df.select.return_value.distinct.return_value.count.return_value = date_count

    overall_df = MagicMock()
    overall_df.cache.return_value = overall_df
    overall_df.count.return_value = _overall_metric_row_count()

    def _factory(*_args, overall=False, **_kwargs):
        return overall_df if overall else dated_df

    mock_dq.side_effect = _factory
    return dated_df, overall_df


PURGE_SQL = (
    "DELETE FROM geo.data_quality AS q WHERE q.table_name = 'avalanche_org' "
    "AND q.file_date <> 'overall' "
    "AND NOT EXISTS (SELECT 1 FROM geo.avalanche_org s WHERE s.file_date = q.file_date)"
)

HISTORICAL_DELETE_SQL = "DELETE FROM geo.data_quality WHERE table_name = 'avalanche_org' AND file_date <> 'overall'"

OVERALL_DELETE_SQL = "DELETE FROM geo.data_quality WHERE table_name = 'avalanche_org' AND file_date = 'overall'"


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
@patch("jobs.data_quality.SupabaseConnectionConfig.from_env", return_value=_mock_db_config())
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

    def test_retries_transient_connection_failure(
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
        monkeypatch,
    ):
        monkeypatch.setattr("utils.jdbc_retry.time.sleep", lambda _seconds: None)
        _setup_metrics(mock_dq)
        mock_schema.side_effect = [
            RuntimeError("Caused by: java.net.SocketTimeoutException: Read timed out"),
            None,
        ]

        result = run(mock_spark, "avalanche_org")

        assert mock_schema.call_count == 2
        assert "HISTORICAL" in result

    def test_delete_and_write_are_retried_as_one_unit(
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
        monkeypatch,
    ):
        monkeypatch.setattr("utils.jdbc_retry.time.sleep", lambda _seconds: None)
        dated_df, overall_df = _setup_metrics(mock_dq)
        dated_df.write.jdbc.side_effect = [
            RuntimeError("Caused by: java.net.UnknownHostException: pooler.supabase.com"),
            None,
        ]

        run(mock_spark, "avalanche_org")

        # The dated delete+write is retried as one unit (2 delete, 2 write), then the
        # overall refresh adds one more delete and one overall write.
        assert dated_df.write.jdbc.call_count == 2
        assert overall_df.write.jdbc.call_count == 1
        assert mock_delete.call_count == 3
        dated_df.unpersist.assert_called_once()
        overall_df.unpersist.assert_called_once()

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

        # Two deletes: the dated HISTORICAL range, then the overall snapshot refresh.
        assert mock_delete.call_count == 2
        assert mock_delete.call_args_list[0][0][1] == HISTORICAL_DELETE_SQL
        assert mock_delete.call_args_list[1][0][1] == OVERALL_DELETE_SQL

    def test_dated_metrics_use_overall_false_and_refresh_uses_true(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _setup_metrics(mock_dq)

        run(mock_spark, "avalanche_org")

        assert mock_dq.call_count == 2
        assert mock_dq.call_args_list[0].kwargs["overall"] is False
        assert mock_dq.call_args_list[1].kwargs["overall"] is True

    def test_writes_metrics(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        dated_df, overall_df = _setup_metrics(mock_dq)

        result = run(mock_spark, "avalanche_org")

        mock_spark.createDataFrame.assert_not_called()
        dated_df.write.jdbc.assert_called_once()
        assert dated_df.write.jdbc.call_args.kwargs["table"] == "geo.data_quality"
        assert dated_df.write.jdbc.call_args.kwargs["mode"] == "append"
        dated_df.unpersist.assert_called_once()
        overall_df.write.jdbc.assert_called_once()
        overall_df.unpersist.assert_called_once()
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
@patch("jobs.data_quality.SupabaseConnectionConfig.from_env", return_value=_mock_db_config())
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

        # Three deletes: the purge, the dated INCREMENTAL range, then the overall refresh.
        assert mock_delete.call_count == 3
        assert (
            mock_delete.call_args_list[1][0][1] == "DELETE FROM geo.data_quality WHERE table_name = 'avalanche_org' "
            "AND file_date > '2026-09-15' AND file_date <> 'overall'"
        )
        assert mock_delete.call_args_list[2][0][1] == OVERALL_DELETE_SQL

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
        dated_df, overall_df = _setup_metrics(mock_dq)

        run(mock_spark, "avalanche_org")

        dated_df.write.jdbc.assert_called_once()
        assert dated_df.write.jdbc.call_args.kwargs["mode"] == "append"
        overall_df.write.jdbc.assert_called_once()
        mock_spark.createDataFrame.assert_not_called()


@patch("jobs.data_quality.data_quality_rows")
@patch("jobs.data_quality._max_stored_file_date", return_value=None)
@patch("jobs.data_quality.execute_statement")
@patch("jobs.data_quality.execute_ddl")
@patch("jobs.data_quality.create_schema")
@patch("jobs.data_quality.SupabaseConnectionConfig.from_env", return_value=_mock_db_config())
@patch("jobs.data_quality.AppConfig.from_json", return_value=_mock_app_config())
@patch("jobs.data_quality.TableFactory.get", return_value=DummyModel)
class TestOverallRun:
    """``overall=True`` measures every row as one ``'overall'`` snapshot."""

    def test_returns_overall_message(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _setup_metrics(mock_dq)

        result = run(mock_spark, "avalanche_org", overall=True)

        assert "OVERALL" in result
        assert "avalanche_org" in result
        assert "data_quality" in result

    def test_skips_load_type_detection(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _setup_metrics(mock_dq)

        run(mock_spark, "avalanche_org", overall=True)

        mock_max.assert_not_called()

    def test_does_not_purge_dated_rows(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _setup_metrics(mock_dq)

        run(mock_spark, "avalanche_org", overall=True)

        assert mock_delete.call_count == 1

    def test_replaces_only_stored_overall_rows(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _setup_metrics(mock_dq)

        run(mock_spark, "avalanche_org", overall=True)

        mock_delete.assert_called_once()
        assert mock_delete.call_args[0][1] == OVERALL_DELETE_SQL

    def test_reads_the_whole_source_unfiltered(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _setup_metrics(mock_dq)

        run(mock_spark, "avalanche_org", overall=True)

        assert mock_spark.read.jdbc.call_count == 1
        assert mock_spark.read.jdbc.call_args.kwargs["table"] == "geo.avalanche_org"
        mock_spark.read.jdbc.return_value.filter.assert_not_called()

    def test_forwards_overall_to_metrics(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _setup_metrics(mock_dq)

        run(mock_spark, "avalanche_org", overall=True)

        assert mock_dq.call_args.kwargs["overall"] is True

    def test_writes_metrics(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _, overall_df = _setup_metrics(mock_dq)

        result = run(mock_spark, "avalanche_org", overall=True)

        overall_df.write.jdbc.assert_called_once()
        assert overall_df.write.jdbc.call_args.kwargs["table"] == "geo.data_quality"
        assert overall_df.write.jdbc.call_args.kwargs["mode"] == "append"
        overall_df.unpersist.assert_called_once()
        assert f"{_overall_metric_row_count()} metrics" in result

    def test_empty_source_returns_no_data_message(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        mock_spark.read.jdbc.return_value.head.return_value = []

        result = run(mock_spark, "avalanche_org", overall=True)

        assert "No data in geo.avalanche_org" in result
        mock_dq.assert_not_called()
        mock_delete.assert_not_called()

    def test_logs_overall_steps(
        self, mock_get, mock_app, mock_db, mock_schema, mock_ddl, mock_delete, mock_max, mock_dq, mock_spark
    ):
        _setup_metrics(mock_dq)
        mock_logger = MagicMock()

        run(mock_spark, "avalanche_org", overall=True, logger=mock_logger)

        mock_logger.info.assert_any_call(
            "OVERALL: measuring every row of geo.avalanche_org as a single 'overall' snapshot..."
        )
        mock_logger.info.assert_any_call(
            f"Computed {_overall_metric_row_count()} metrics for all rows of avalanche_org (OVERALL)"
        )
        mock_logger.info.assert_any_call("Replacing stored metrics for avalanche_org...")
        mock_logger.info.assert_any_call("Writing to geo.data_quality...")


def test_max_stored_file_date_ignores_overall_rows(mock_spark):
    """The stored-date probe must never feed 'overall' to TO_DATE."""
    from jobs.data_quality import _max_stored_file_date

    mock_spark.read.jdbc.return_value.first.return_value = None

    assert _max_stored_file_date(mock_spark, "jdbc:postgresql://db", {}, "geo", "avalanche_org") is None

    subquery = mock_spark.read.jdbc.call_args.kwargs["table"]
    assert "table_name = 'avalanche_org'" in subquery
    assert "file_date <> 'overall'" in subquery


def test_unknown_table_raises(mock_spark):
    with pytest.raises(ValueError, match="Unknown table"):
        run(mock_spark, "nonexistent_table")


@patch("jobs.data_quality.retry_connection")
def test_max_stored_file_date_should_return_stored_date(mock_retry):
    from jobs.data_quality import _max_stored_file_date

    # Arrange
    mock_retry.return_value = SimpleNamespace(max_file_date=dt.date(2026, 9, 30))
    spark = MagicMock()

    # Act
    result = _max_stored_file_date(spark, "jdbc:postgresql://db:5432/postgres", {}, "geo", "avalanche_org")

    # Assert
    assert result == dt.date(2026, 9, 30)


@patch("jobs.data_quality.retry_connection")
def test_max_stored_file_date_should_return_none_on_error(mock_retry):
    from jobs.data_quality import _max_stored_file_date

    # Arrange
    mock_retry.side_effect = RuntimeError("connection refused")
    spark = MagicMock()

    # Act
    result = _max_stored_file_date(spark, "jdbc:postgresql://db:5432/postgres", {}, "geo", "avalanche_org")

    # Assert
    assert result is None
