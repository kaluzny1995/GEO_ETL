"""Interface shared by every script in ``single_etl/``."""

import logging
from abc import ABC, abstractmethod
from typing import ClassVar

from pyspark.sql import DataFrame, SparkSession

import utils.etl_utils as etl_utils
from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType


class SingleEtlScript(ABC):
    """Interface shared by every script in ``single_etl/``.

    Subclasses provide the two table names and the column mapping — stamping
    :attr:`FILE_DATE_COL` and using :attr:`UNKNOWN_CODE` where the source has
    no location — and the concrete :meth:`run` method owns no pipeline logic
    of its own and delegates to :func:`~utils.etl_utils.run_etl`.
    """

    SOURCE_TABLE: ClassVar[str]
    """Plain table the script reads (e.g. ``"usgs_volcano"``)."""

    TARGET_TABLE: ClassVar[str]
    """ETL table the script appends to (e.g. ``"usgs_volcano_etl"``)."""

    FILE_DATE_COL: ClassVar[str] = etl_utils.FILE_DATE_COL
    """Canonical ``file_date`` stamp column every ``_convert`` must select."""

    UNKNOWN_CODE: ClassVar[str] = "ZZ"
    """Placeholder country/state code when the source has no location."""

    @abstractmethod
    def _convert(self, df: DataFrame) -> DataFrame:
        """Map the source rows onto the canonical 21-column structure."""

    def run(
        self,
        spark: SparkSession,
        config: AppConfig,
        db_config: SupabaseConnectionConfig,
        *,
        load_type: LoadType | None = None,
        logger: logging.Logger | None = None,
    ) -> str:
        """Read ``geo.<SOURCE_TABLE>``, convert it and append to ``geo.<TARGET_TABLE>``.

        Args:
            spark: Active Spark session.
            config: Application configuration (DB schema, JDBC driver, logging).
            db_config: Supabase connection credentials.
            load_type: Load type forced by the caller (``--load-type``), or
                ``None`` for automatic detection from the target table.
            logger: Optional logger; defaults to the runner's logger.

        Returns:
            A human-readable summary, e.g. ``"HISTORICAL: wrote to geo.usgs_volcano_etl"``
            or ``"INCREMENTAL: nothing to do — usgs_volcano_etl is up to date"``.

        See also:
            :func:`~utils.etl_utils.run_etl` for the shared pipeline and the load-type contract.
        """
        return etl_utils.run_etl(
            spark,
            config,
            db_config,
            self.SOURCE_TABLE,
            self.TARGET_TABLE,
            self._convert,
            load_type=load_type,
            logger=logger,
        )
