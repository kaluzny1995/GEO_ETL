"""Tests for utils/spark_monitor.py — stage memory logging."""

from unittest.mock import MagicMock, patch


class TestRegisterMemoryLogger:
    """Tests for register_memory_logger()."""

    @patch("utils.spark_monitor.os.makedirs")
    @patch("utils.spark_monitor.os.path.abspath", return_value="/abs/path")
    def test_creates_log_directory(self, mock_abspath, mock_makedirs):
        from utils.spark_monitor import register_memory_logger

        spark = MagicMock()
        register_memory_logger(spark, "/tmp/logs/memory/stage_memory.log")

        mock_makedirs.assert_called_once_with("/tmp/logs/memory", exist_ok=True)

    @patch("utils.spark_monitor.os.makedirs")
    @patch("utils.spark_monitor.os.path.abspath", return_value="/abs/path")
    def test_registers_java_listener(self, mock_abspath, mock_makedirs):
        from utils.spark_monitor import register_memory_logger

        spark = MagicMock()
        register_memory_logger(spark, "/tmp/stage_memory.log")

        jvm = spark.sparkContext._jvm
        jvm.sparkmonitor.StageMemoryListener.assert_called_once_with("/abs/path")
        jvm.sparkmonitor.ListenerHelper.addListener.assert_called_once()

    @patch("utils.spark_monitor.os.makedirs")
    @patch("utils.spark_monitor.os.path.abspath", return_value="/abs/path")
    def test_listener_created_with_log_path(self, mock_abspath, mock_makedirs):
        from utils.spark_monitor import register_memory_logger

        spark = MagicMock()
        register_memory_logger(spark, "/tmp/stage_memory.log")

        spark.sparkContext._jvm.sparkmonitor.StageMemoryListener.assert_called_once_with("/abs/path")

    @patch("utils.spark_monitor.os.makedirs")
    @patch("utils.spark_monitor.os.path.abspath", return_value="/abs/path")
    def test_listener_added_via_bridge(self, mock_abspath, mock_makedirs):
        from utils.spark_monitor import register_memory_logger

        spark = MagicMock()
        register_memory_logger(spark, "/tmp/stage_memory.log")

        jvm = spark.sparkContext._jvm
        call_args = jvm.sparkmonitor.ListenerHelper.addListener.call_args
        assert call_args[0][0] == spark.sparkContext._jsc.sc()
        assert call_args[0][1] == jvm.sparkmonitor.StageMemoryListener()

    @patch("utils.spark_monitor.os.makedirs")
    @patch("utils.spark_monitor.os.path.abspath", return_value="/abs/path")
    def test_no_return_value(self, mock_abspath, mock_makedirs):
        from utils.spark_monitor import register_memory_logger

        spark = MagicMock()
        result = register_memory_logger(spark, "/tmp/stage_memory.log")

        assert result is None

    @patch("utils.spark_monitor.os.makedirs")
    @patch("utils.spark_monitor.os.path.abspath", return_value="/abs/path")
    def test_skips_makedirs_when_no_dirname(self, mock_abspath, mock_makedirs):
        from utils.spark_monitor import register_memory_logger

        spark = MagicMock()
        register_memory_logger(spark, "stage_memory.log")

        mock_makedirs.assert_not_called()


def test_register_memory_logger_raises_when_gateway_missing():
    import pytest

    from utils.spark_monitor import register_memory_logger

    # Arrange
    spark = MagicMock()
    spark.sparkContext._jvm = None

    # Act & Assert
    with pytest.raises(RuntimeError, match="JVM gateway"):
        register_memory_logger(spark, "/tmp/logs/memory/stage_memory.log")
