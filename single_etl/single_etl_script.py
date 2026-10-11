"""Interface shared by every script in ``single_etl/``."""

import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import ClassVar

from pyspark.sql import DataFrame, SparkSession

import utils.etl_utils as etl_utils
from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType


class SingleEtlScript(ABC):
    """Interface shared by every script in ``single_etl/``.

    Subclasses provide the two table names, the column mapping — stamping
    :attr:`FILE_DATE_COL` and using :attr:`UNKNOWN_CODE` where the source has
    no location — and the :attr:`DEDUP_KEYS` of their natural event — and the
    concrete :meth:`run` method owns no pipeline logic of its own and
    delegates to :func:`~utils.etl_utils.run_etl`.
    """

    SOURCE_TABLE: ClassVar[str | Sequence[str]]
    """Plain table the script reads (e.g. ``"usgs_volcano"``) — or several
    tables to stack with ``unionByName`` (the union script)."""

    TARGET_TABLE: ClassVar[str]
    """ETL table the script appends to (e.g. ``"usgs_volcano_etl"``)."""

    DEDUP_KEYS: ClassVar[Sequence[str] | None] = None
    """Canonical columns identifying one natural event (e.g. ``event_name``,
    ``latitude``, ``longitude``, ``event_start``), or ``None`` to skip
    deduplication. :meth:`run` hands them to
    :func:`~utils.etl_utils.run_etl`, which collapses rows sharing the keys
    **inside one ``file_date``** — one row per event per file, while the
    cross-file snapshots of the same event are kept. The keys must not
    include ``file_date``; the runner adds it. Declared by every source
    script; ``None`` only for ``union_etl``, whose sources are already
    deduplicated by their own scripts."""

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
    ) -> etl_utils.EtlOutcome:
        """Read ``geo.<SOURCE_TABLE>`` (one table, or several stacked), convert it,
        deduplicate it on :attr:`DEDUP_KEYS` and append to ``geo.<TARGET_TABLE>``.

        Args:
            spark: Active Spark session.
            config: Application configuration (DB schema, JDBC driver, logging).
            db_config: Supabase connection credentials.
            load_type: Load type forced by the caller (``--load-type``), or
                ``None`` for automatic detection from the target table.
            logger: Optional logger; defaults to the runner's logger.

        Returns:
            The :class:`~utils.etl_utils.EtlOutcome` of the shared run —
            ``loaded`` together with a summary message (the write report, or
            ``No records loaded into …`` when nothing was loaded). The script
            itself never fails the run: ``jobs/single_etl.py`` raises
            ``RuntimeError`` when ``loaded`` is ``False``.

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
            dedup_keys=self.DEDUP_KEYS,
            load_type=load_type,
            logger=logger,
        )
