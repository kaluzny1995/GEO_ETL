from unittest.mock import MagicMock

import pytest

from utils import jdbc_retry
from utils.jdbc_retry import DEFAULT_ATTEMPTS, DEFAULT_DELAY_SECONDS, is_connection_error, retry_connection

CONNECTION_ERRORS = [
    "py4j.protocol.Py4JJavaError: An error occurred while calling o39.jdbc.\n"
    "org.postgresql.util.PSQLException: Próba nawiązania połączenia nie powiodła się.\n"
    "Caused by: java.net.SocketTimeoutException: Read timed out",
    "java.net.UnknownHostException: aws-1-eu-west-1.pooler.supabase.com",
    "java.net.ConnectException: Connection refused",
    "org.springframework.transaction.CannotCreateTransactionException: "
    "could not create connection; SQLTransientConnectionException",
]


@pytest.mark.parametrize("message", CONNECTION_ERRORS)
def test_is_connection_error_accepts_transient_failures(message):
    assert is_connection_error(RuntimeError(message))


@pytest.mark.parametrize(
    "message",
    [
        'ERROR: prepared statement "S_1" already exists',
        "org.postgresql.util.PSQLException: ERROR: division by zero",
        "ValueError: Unknown table",
    ],
)
def test_is_connection_error_rejects_other_failures(message):
    assert not is_connection_error(RuntimeError(message))


def test_defaults():
    assert DEFAULT_ATTEMPTS == 3
    assert DEFAULT_DELAY_SECONDS == 5.0


def test_returns_result_without_retrying():
    calls = []

    def operation():
        calls.append("call")
        return 42

    assert retry_connection(operation, delay=0) == 42
    assert calls == ["call"]


def test_retries_connection_error_until_success():
    calls = []

    def operation():
        calls.append("call")
        if len(calls) < 3:
            raise RuntimeError("Caused by: java.net.SocketTimeoutException: Read timed out")
        return "done"

    assert retry_connection(operation, delay=0, logger=MagicMock()) == "done"
    assert len(calls) == 3


def test_raises_after_all_attempts_fail():
    calls = []
    error = RuntimeError("java.net.UnknownHostException: pooler.supabase.com")

    def operation():
        calls.append("call")
        raise error

    with pytest.raises(RuntimeError) as excinfo:
        retry_connection(operation, attempts=3, delay=0)

    assert excinfo.value is error
    assert len(calls) == 3


def test_does_not_retry_unrelated_errors():
    calls = []
    error = ValueError("Unknown table")

    def operation():
        calls.append("call")
        raise error

    with pytest.raises(ValueError) as excinfo:
        retry_connection(operation, attempts=3, delay=0)

    assert excinfo.value is error
    assert len(calls) == 1


def test_logs_a_warning_per_retry(monkeypatch):
    sleeps = []
    monkeypatch.setattr(jdbc_retry.time, "sleep", sleeps.append)
    logger = MagicMock()

    def operation():
        raise RuntimeError("Caused by: java.net.ConnectException: Connection refused")

    with pytest.raises(RuntimeError):
        retry_connection(operation, attempts=3, delay=2.5, logger=logger)

    assert logger.warning.call_count == 2
    assert sleeps == [2.5, 2.5]


def test_uses_module_logger_when_missing(monkeypatch):
    monkeypatch.setattr(jdbc_retry.time, "sleep", lambda _seconds: None)

    def operation():
        raise RuntimeError("Caused by: java.net.SocketTimeoutException: Read timed out")

    with pytest.raises(RuntimeError):
        retry_connection(operation, attempts=2, delay=0)
