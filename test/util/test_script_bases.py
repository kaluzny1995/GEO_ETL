from unittest.mock import MagicMock, patch

import pytest

from ingestion.raw2plain.raw2plain_script import Raw2PlainScript
from ingestion.upload.upload_script import UploadScript
from single_etl.single_etl_script import SingleEtlScript
from utils.etl_utils import EtlOutcome
from views.view_script import ViewScript


class _StubScript(SingleEtlScript):
    SOURCE_TABLE = "src_plain"
    TARGET_TABLE = "src_etl"

    def _convert(self, df):
        return df


class TestSingleEtlScript:
    def test_base_cannot_be_instantiated(self):
        with pytest.raises(TypeError):
            SingleEtlScript()  # type: ignore[abstract]

    def test_subclass_without_convert_cannot_be_instantiated(self):
        class Incomplete(SingleEtlScript):
            SOURCE_TABLE = "src_plain"
            TARGET_TABLE = "src_etl"

        with pytest.raises(TypeError):
            Incomplete()

    def test_run_delegates_to_run_etl_with_the_class_tables_and_convert(self):
        script = _StubScript()
        spark, config, db_config = MagicMock(), MagicMock(), MagicMock()

        with patch("utils.etl_utils.run_etl", return_value=EtlOutcome(True, "HISTORICAL: wrote")) as mock_run:
            outcome = script.run(spark, config, db_config)

        assert outcome.loaded is True
        assert outcome.summary == "HISTORICAL: wrote"
        args, kwargs = mock_run.call_args
        assert args[:5] == (spark, config, db_config, "src_plain", "src_etl")
        assert args[5] == script._convert
        assert script._convert("frame") == "frame"
        assert kwargs == {"dedup_keys": None, "load_type": None, "logger": None}

    def test_run_passes_the_class_dedup_keys_through(self):
        class _KeyedScript(SingleEtlScript):
            SOURCE_TABLE = "src_plain"
            TARGET_TABLE = "src_etl"
            DEDUP_KEYS = ("event_name", "latitude", "longitude", "event_start")

            def _convert(self, df):
                return df

        script = _KeyedScript()

        with patch("utils.etl_utils.run_etl", return_value=EtlOutcome(True, "ok")) as mock_run:
            script.run(MagicMock(), MagicMock(), MagicMock())

        assert mock_run.call_args.kwargs["dedup_keys"] == ("event_name", "latitude", "longitude", "event_start")

    def test_run_passes_a_stack_of_source_tables_through(self):
        class _StackScript(SingleEtlScript):
            SOURCE_TABLE = ("src_plain", "other_plain")
            TARGET_TABLE = "stack_etl"

            def _convert(self, df):
                return df

        script = _StackScript()
        assert script._convert("frame") == "frame"
        with patch("utils.etl_utils.run_etl", return_value=EtlOutcome(True, "ok")) as mock_run:
            script.run(MagicMock(), MagicMock(), MagicMock())

        args, _kwargs = mock_run.call_args
        assert args[3] == ("src_plain", "other_plain")
        assert args[4] == "stack_etl"

    def test_run_passes_load_type_and_logger_through(self):
        script = _StubScript()
        load_type, logger = MagicMock(name="load_type"), MagicMock(name="logger")

        with patch("utils.etl_utils.run_etl", return_value=EtlOutcome(True, "ok")) as mock_run:
            script.run(MagicMock(), MagicMock(), MagicMock(), load_type=load_type, logger=logger)

        assert mock_run.call_args.kwargs == {"dedup_keys": None, "load_type": load_type, "logger": logger}


class _StubRaw2PlainScript(Raw2PlainScript):
    RAW_TABLE = "src_raw"
    PLAIN_TABLE = "src_plain"

    def _convert(self, df):
        return df


class TestRaw2PlainScript:
    def test_base_cannot_be_instantiated(self):
        with pytest.raises(TypeError):
            Raw2PlainScript()  # type: ignore[abstract]

    def test_subclass_without_convert_cannot_be_instantiated(self):
        class Incomplete(Raw2PlainScript):
            RAW_TABLE = "src_raw"
            PLAIN_TABLE = "src_plain"

        with pytest.raises(TypeError):
            Incomplete()

    def test_raw2plain_delegates_to_runner_with_the_class_tables_and_convert(self):
        script = _StubRaw2PlainScript()
        spark, config, db_config = MagicMock(), MagicMock(), MagicMock()

        with patch("utils.raw2plain_utils.raw2plain", return_value=True) as mock_run:
            result = script.raw2plain(spark, config, db_config)

        assert result is True
        args = mock_run.call_args[0]
        assert args[:5] == (spark, config, db_config, "src_raw", "src_plain")
        assert args[5] == script._convert
        assert script._convert("frame") == "frame"
        assert args[6] is None

    def test_raw2plain_passes_logger_through(self):
        script = _StubRaw2PlainScript()
        logger = MagicMock(name="logger")

        with patch("utils.raw2plain_utils.raw2plain", return_value=False) as mock_run:
            result = script.raw2plain(MagicMock(), MagicMock(), MagicMock(), logger=logger)

        assert result is False
        assert mock_run.call_args[0][6] is logger


class _StubUploadScript(UploadScript):
    TABLE_NAME = "src_raw"
    MODEL = MagicMock(name="MODEL")
    DATASET_FOLDER = "Src"


class TestUploadScript:
    def test_base_cannot_be_instantiated(self):
        with pytest.raises(TypeError):
            UploadScript()  # type: ignore[abstract]

    def test_subclass_without_config_cannot_be_instantiated(self):
        class Incomplete(UploadScript):
            TABLE_NAME = "src_raw"
            MODEL = MagicMock(name="MODEL")

        with pytest.raises(TypeError):
            Incomplete()

    def test_upload_delegates_to_runner_with_the_class_config(self):
        script = _StubUploadScript()
        spark, config, db_config = MagicMock(), MagicMock(), MagicMock()

        with patch("utils.upload_utils.upload", return_value=True) as mock_run:
            result = script.upload(spark, config, db_config)

        assert result is True
        args = mock_run.call_args[0]
        assert args[:3] == (spark, config, db_config)
        assert args[3] == "src_raw"
        assert args[4] is _StubUploadScript.MODEL
        assert args[5] == "Src"
        assert args[6] is None

    def test_upload_passes_logger_through(self):
        script = _StubUploadScript()
        logger = MagicMock(name="logger")

        with patch("utils.upload_utils.upload", return_value=False) as mock_run:
            result = script.upload(MagicMock(), MagicMock(), MagicMock(), logger=logger)

        assert result is False
        assert mock_run.call_args[0][6] is logger


class _StubViewScript(ViewScript):
    SOURCE_TABLE = "src_etl"
    TARGET_VIEW = "src_vw"
    QUERY = "SELECT * FROM geo.src_etl"


class TestViewScript:
    def test_run_delegates_to_run_view_with_the_class_names_and_query(self):
        script = _StubViewScript()
        spark, config, db_config = MagicMock(), MagicMock(), MagicMock()

        with patch("utils.view_utils.run_view", return_value=EtlOutcome(True, "HISTORICAL: defined view")) as mock_run:
            outcome = script.run(spark, config, db_config)

        assert outcome.loaded is True
        assert outcome.summary == "HISTORICAL: defined view"
        args, kwargs = mock_run.call_args
        assert args == (spark, config, db_config, "src_etl", "src_vw", "SELECT * FROM geo.src_etl")
        assert kwargs == {"logger": None}

    def test_run_passes_logger_through(self):
        script = _StubViewScript()
        logger = MagicMock(name="logger")

        with patch("utils.view_utils.run_view", return_value=EtlOutcome(True, "ok")) as mock_run:
            script.run(MagicMock(), MagicMock(), MagicMock(), logger=logger)

        assert mock_run.call_args.kwargs == {
            "logger": logger,
        }
