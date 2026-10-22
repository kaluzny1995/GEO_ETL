"""Interface shared by every script in ``views/``."""

import logging
from abc import ABC
from typing import ClassVar

from pyspark.sql import SparkSession

import utils.view_utils as view_utils
from model.config import AppConfig, SupabaseConnectionConfig
from utils.etl_utils import EtlOutcome


class ViewScript(ABC):
    """Interface shared by every script in ``views/``.

    Subclasses provide the two object names and the query — usually read
    from their ``config.json`` entry with
    :func:`~model.config.datasets.load_dataset`::

        _cfg = load_dataset(__file__, "view", ViewDatasetConfig)

        class UnionVwScript(ViewScript):
            SOURCE_TABLE = _cfg.source_table
            TARGET_VIEW = _cfg.target_view
            QUERY = _cfg.query

    The concrete :meth:`run` method owns no pipeline logic of its own and
    delegates to :func:`~utils.view_utils.run_view` — **HISTORICAL** is the
    only load type: every run redefines the whole view.
    """

    SOURCE_TABLE: ClassVar[str]
    """``*_etl`` table the view is defined over (e.g. ``"union_etl"``)."""

    TARGET_VIEW: ClassVar[str]
    """View the script creates, always ``*_vw`` (e.g. ``"union_vw"``)."""

    QUERY: ClassVar[str]
    """Complete ``SELECT`` statement the view exposes — it must read
    :attr:`SOURCE_TABLE` (schema-qualified, e.g. ``geo.union_etl``)."""

    def run(
        self,
        spark: SparkSession,
        config: AppConfig,
        db_config: SupabaseConnectionConfig,
        *,
        logger: logging.Logger | None = None,
    ) -> EtlOutcome:
        """Run :attr:`QUERY` over ``geo.<SOURCE_TABLE>`` and define ``geo.<TARGET_VIEW>``.

        Args:
            spark: Active Spark session.
            config: Application configuration (DB schema, JDBC driver, logging).
            db_config: Supabase connection credentials.
            logger: Optional logger; defaults to the runner's logger.

        Returns:
            The :class:`~utils.etl_utils.EtlOutcome` of the shared run —
            ``loaded`` together with a summary message (the create report, or
            ``No records loaded into …`` when the query returned no rows).
            The script itself never fails the run: ``jobs/view.py`` raises
            ``RuntimeError`` when ``loaded`` is ``False``.

        See also:
            :func:`~utils.view_utils.run_view` for the shared pipeline and the
            HISTORICAL-only load contract.
        """
        return view_utils.run_view(
            spark,
            config,
            db_config,
            self.SOURCE_TABLE,
            self.TARGET_VIEW,
            self.QUERY,
            logger=logger,
        )
