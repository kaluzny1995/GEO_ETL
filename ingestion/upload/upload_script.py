"""Interface shared by every script in ``ingestion/upload/``."""

import logging
from abc import ABC, abstractmethod
from typing import ClassVar

from pyspark.sql import DataFrame, SparkSession

import utils.upload_utils as upload_utils
from model.config import AppConfig, SupabaseConnectionConfig


class UploadScript(ABC):
    """Interface every upload script in ``ingestion/upload/`` implements.

    Subclasses set :attr:`TABLE_NAME` and :attr:`DATASET_FOLDER` and implement
    :meth:`_convert`; the concrete :meth:`upload` method delegates to the
    shared :func:`~utils.upload_utils.upload` runner.
    """

    TABLE_NAME: ClassVar[str]
    DATASET_FOLDER: ClassVar[str]

    @abstractmethod
    def _convert(self, df: DataFrame) -> DataFrame:
        """Cast the CSV text columns to the plain table's typed structure.

        Args:
            df: CSV rows (all columns as text) with ``file_date`` stamped.

        Returns:
            The DataFrame in :attr:`TABLE_NAME` column order.
        """

    def upload(
        self,
        spark: SparkSession,
        config: AppConfig,
        db_config: SupabaseConnectionConfig,
        logger: logging.Logger | None = None,
    ) -> bool:
        """Run this script's CSV → plain-table ingestion via the shared runner.

        Args:
            spark: Active Spark session.
            config: Application configuration.
            db_config: Supabase connection credentials.
            logger: Optional logger; when ``None`` a file-based logger is created.

        Returns:
            ``True`` when CSV files were written to the plain table, ``False``
            when there was nothing to ingest.
        """
        return upload_utils.upload(
            spark, config, db_config, self.TABLE_NAME, self.DATASET_FOLDER, self._convert, logger
        )
