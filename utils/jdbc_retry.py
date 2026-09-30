"""Bounded retries for transient database connection failures.

Supabase is reached through its pooler over the public network, so an
otherwise healthy database can still fail a single JDBC call with a socket
timeout or an unresolved host. Such errors are transient — repeating the
same operation after a short pause usually succeeds.

Only errors that look like a *connection* problem are retried; SQL errors,
constraint violations and programming mistakes are raised on the first
attempt. Operations must therefore be idempotent: DDL with ``IF NOT
EXISTS``, ``DELETE`` by key, reads, and a ``DELETE`` + append pair that is
re-run as a single unit.
"""

import logging
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")

DEFAULT_ATTEMPTS = 3
DEFAULT_DELAY_SECONDS = 5.0

CONNECTION_ERROR_MARKERS = (
    "SocketTimeoutException",
    "ConnectException",
    "UnknownHostException",
    "SQLTransientConnectionException",
    "Connection refused",
    "connection refused",
    "Próba nawiązania połączenia nie powiodła się",
)


def is_connection_error(error: BaseException) -> bool:
    """Tell whether *error* is a transient connection failure.

    Args:
        error: Exception raised by a JDBC or Spark operation.

    Returns:
        ``True`` for socket/DNS/connection errors worth retrying,
        ``False`` for everything else.
    """
    text = str(error)
    return any(marker in text for marker in CONNECTION_ERROR_MARKERS)


def retry_connection(
    operation: Callable[[], T],
    *,
    attempts: int = DEFAULT_ATTEMPTS,
    delay: float = DEFAULT_DELAY_SECONDS,
    logger: logging.Logger | None = None,
) -> T:
    """Run *operation*, retrying transient connection failures.

    Args:
        operation: Zero-argument callable to execute.
        attempts: Total number of attempts (first try plus retries).
        delay: Seconds to wait between attempts.
        logger: Logger used for the retry warnings; defaults to the module
            logger.

    Returns:
        Whatever *operation* returns.

    Raises:
        Exception: The original error when it is not a connection failure,
            or when every attempt has failed.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    attempt = 0
    while True:
        attempt += 1
        try:
            return operation()
        except Exception as error:
            if not is_connection_error(error) or attempt >= attempts:
                raise
            logger.warning(
                f"Transient connection failure on attempt {attempt}/{attempts}: {error}. Retrying in {delay:g}s..."
            )
            time.sleep(delay)
