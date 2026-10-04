"""Interface shared by every script in ``ingestion/raw2plain/``."""

import logging
from abc import ABC, abstractmethod
from typing import ClassVar

from pyspark.sql import DataFrame, SparkSession

import utils.raw2plain_utils as raw2plain_utils
from model.config import AppConfig, SupabaseConnectionConfig


class Raw2PlainScript(ABC):
    """Interface every raw2plain script in ``ingestion/raw2plain/`` implements.

    Subclasses set :attr:`RAW_TABLE` and :attr:`PLAIN_TABLE` and implement
    :meth:`_convert`; the concrete :meth:`raw2plain` method delegates to the
    shared :func:`~utils.raw2plain_utils.raw2plain` runner.
    """

    RAW_TABLE: ClassVar[str]
    PLAIN_TABLE: ClassVar[str]

    @abstractmethod
    def _convert(self, df: DataFrame) -> DataFrame:
        """Map the raw table's columns onto the typed plain structure.

        Args:
            df: Rows read from :attr:`RAW_TABLE`.

        Returns:
            The DataFrame in :attr:`PLAIN_TABLE` column order.
        """

    def raw2plain(
        self,
        spark: SparkSession,
        config: AppConfig,
        db_config: SupabaseConnectionConfig,
        logger: logging.Logger | None = None,
    ) -> bool:
        """Run this script's raw → plain transformation via the shared runner.

        Args:
            spark: Active Spark session.
            config: Application configuration.
            db_config: Supabase connection credentials.
            logger: Optional logger; when ``None`` a file-based logger is created.

        Returns:
            ``True`` when converted rows were written to the plain table,
            ``False`` when there was nothing to transform.
        """
        return raw2plain_utils.raw2plain(
            spark, config, db_config, self.RAW_TABLE, self.PLAIN_TABLE, self._convert, logger
        )
