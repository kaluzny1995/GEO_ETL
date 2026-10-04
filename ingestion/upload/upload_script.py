"""Interface shared by every script in ``ingestion/upload/``."""

import logging
from abc import ABC, abstractmethod

from pydantic import BaseModel
from pyspark.sql import SparkSession

import utils.upload_utils as upload_utils
from model.config import AppConfig, SupabaseConnectionConfig


class UploadScript(ABC):
    """Interface every upload script in ``ingestion/upload/`` implements.

    Subclasses must provide the abstract :attr:`TABLE_NAME`, :attr:`MODEL`
    and :attr:`DATASET_FOLDER` attributes (plain class attributes satisfy
    them); the concrete :meth:`upload` method delegates to the shared
    :func:`~utils.upload_utils.upload` runner.
    """

    @property
    @abstractmethod
    def TABLE_NAME(self) -> str:
        """Raw table the script writes to (e.g. ``"avalanche_org_raw"``)."""

    @property
    @abstractmethod
    def MODEL(self) -> type[BaseModel]:
        """Pydantic model class defining the raw table schema."""

    @property
    @abstractmethod
    def DATASET_FOLDER(self) -> str:
        """Subdirectory under the data path containing the CSV files."""

    def upload(
        self,
        spark: SparkSession,
        config: AppConfig,
        db_config: SupabaseConnectionConfig,
        logger: logging.Logger | None = None,
    ) -> bool:
        """Run this script's CSV → raw-table upload via the shared runner.

        Args:
            spark: Active Spark session.
            config: Application configuration.
            db_config: Supabase connection credentials.
            logger: Optional logger; when ``None`` a file-based logger is created.

        Returns:
            ``True`` when CSV files were written to the raw table, ``False``
            when there was nothing to ingest.
        """
        return upload_utils.upload(spark, config, db_config, self.TABLE_NAME, self.MODEL, self.DATASET_FOLDER, logger)
