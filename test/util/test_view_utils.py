from unittest.mock import MagicMock, patch

from model.enums import LoadType
from utils.view_utils import LOAD_TYPE, run_view

QUERY = "SELECT event_name, file_date FROM geo.union_etl WHERE source_name IS NOT NULL;"


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


def _mock_spark(rows=("EQ",)):
    spark = MagicMock()
    spark.read.jdbc.return_value.head.return_value = list(rows)
    return spark


class TestLoadType:
    def test_views_are_historical_only(self):
        assert LOAD_TYPE is LoadType.HISTORICAL


class TestRunView:
    def test_runs_the_query_as_a_jdbc_subquery(self):
        spark = _mock_spark()
        logger = MagicMock()

        with patch("utils.view_utils.execute_statement"):
            run_view(spark, _mock_app_config(), _mock_db_config(), "union_etl", "union_vw", QUERY, logger=logger)

        spark.read.jdbc.assert_called_once_with(
            url="jdbc:postgresql://db.example.com:5432/postgres",
            table=f"({QUERY.strip().rstrip(';')}) AS t",
            properties={
                "user": "user",
                "password": "pass",
                "driver": "org.postgresql.Driver",
            },
        )

    def test_defines_the_view_with_create_or_replace(self):
        spark = _mock_spark()
        config = _mock_app_config()
        db_config = _mock_db_config()
        expected_ddl = f"CREATE OR REPLACE VIEW geo.union_vw AS {QUERY.strip().rstrip(';')};"

        with patch("utils.view_utils.execute_statement") as mock_exec:
            run_view(spark, config, db_config, "union_etl", "union_vw", QUERY, logger=MagicMock())

        mock_exec.assert_called_once_with(
            spark, expected_ddl, db_config.get_jdbc_url.return_value, db_config.get_jdbc_properties.return_value
        )

    def test_trailing_semicolon_is_stripped_from_the_query(self):
        spark = _mock_spark()

        with patch("utils.view_utils.execute_statement") as mock_exec:
            run_view(spark, _mock_app_config(), _mock_db_config(), "union_etl", "union_vw", QUERY, logger=MagicMock())

        # The subquery wrapping must not see the statement terminator...
        assert spark.read.jdbc.call_args.kwargs["table"] == (
            "(SELECT event_name, file_date FROM geo.union_etl WHERE source_name IS NOT NULL) AS t"
        )
        # …while the stored view definition ends with exactly one.
        assert mock_exec.call_args[0][1] == (
            "CREATE OR REPLACE VIEW geo.union_vw AS "
            "SELECT event_name, file_date FROM geo.union_etl WHERE source_name IS NOT NULL;"
        )

    def test_returns_loaded_summary_on_success(self):
        spark = _mock_spark()

        with patch("utils.view_utils.execute_statement"):
            outcome = run_view(spark, _mock_app_config(), _mock_db_config(), "union_etl", "union_vw", QUERY)

        assert outcome.loaded is True
        assert outcome.summary == "HISTORICAL: defined view geo.union_vw over geo.union_etl"

    def test_empty_query_result_reports_not_loaded(self):
        spark = _mock_spark(rows=())

        with patch("utils.view_utils.execute_statement"):
            outcome = run_view(spark, _mock_app_config(), _mock_db_config(), "union_etl", "union_vw", QUERY)

        assert outcome.loaded is False
        assert outcome.summary == "No records loaded into union_vw — query returned no rows from union_etl"

    def test_empty_query_result_leaves_the_view_untouched(self):
        spark = _mock_spark(rows=())

        with patch("utils.view_utils.execute_statement") as mock_exec:
            run_view(spark, _mock_app_config(), _mock_db_config(), "union_etl", "union_vw", QUERY)

        mock_exec.assert_not_called()

    def test_logs_banner_and_load_type(self):
        spark = _mock_spark()
        logger = MagicMock()

        with patch("utils.view_utils.execute_statement"):
            run_view(spark, _mock_app_config(), _mock_db_config(), "union_etl", "union_vw", QUERY, logger=logger)

        messages = [call.args[0] for call in logger.info.call_args_list]
        assert "---- View started: geo.union_etl → geo.union_vw ----" in messages
        assert "Load type: HISTORICAL (the only load type views support)" in messages

    def test_default_logger_is_used_when_none_is_given(self):
        spark = _mock_spark()

        with patch("utils.view_utils.execute_statement"):
            outcome = run_view(spark, _mock_app_config(), _mock_db_config(), "union_etl", "union_vw", QUERY)

        assert outcome.loaded is True
