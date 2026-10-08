"""Every ``*_etl`` table → ``geo.union_etl`` (vertical union of canonical rows).

Stacks the nine source ``*_etl`` tables into one ``geo.union_etl`` table. The
rows keep every value their own script produced — ``source_name`` says which
source they came from (``AVALANCHE_ORG``, ``EONET``, ``GDACS``, …) along with
that source's ``event_category``, ``metric_name`` / ``metric_unit`` and
``other_metrics``. Nothing is remapped: :meth:`UnionEtlScript._convert` is the
identity because the rows already carry the canonical 21-column structure from
``model/etl/event_etl.py``, and ``SOURCE_TABLE`` is the tuple of tables the
shared runner stacks with ``unionByName``.

Run::

    python run_etl.py SINGLE_ETL union_etl
    python run_etl.py SINGLE_ETL union_etl --load-type historical
    python run_etl.py SINGLE_ETL union_etl --load-type incremental

**Load types** — ``HISTORICAL``, ``INCREMENTAL`` and the ``--load-type auto``
detection behave as in every other script: the target's latest ``file_date``
decides, an ``INCREMENTAL`` run filters *each* source table with the same
``file_date`` condition before stacking, and an empty target falls back to
``HISTORICAL``. When no source row is newer than the target's maximum the run
loads nothing and ``jobs/single_etl.py`` fails with ``RuntimeError`` — see
:func:`utils.etl_utils.run_etl`.
"""

from typing import ClassVar

from pyspark.sql import DataFrame

from model.config.datasets import UnionEtlConfig, load_dataset
from single_etl.single_etl_script import SingleEtlScript

_cfg = load_dataset(__file__, "single_etl", UnionEtlConfig)


class UnionEtlScript(SingleEtlScript):
    """Stack every configured ``*_etl`` table into ``geo.union_etl``."""

    SOURCE_TABLE: ClassVar[tuple[str, ...]] = tuple(_cfg.source_tables)
    TARGET_TABLE: ClassVar[str] = _cfg.target_table

    def _convert(self, df: DataFrame) -> DataFrame:
        """Return the stacked rows unchanged — they are already canonical.

        Args:
            df: Rows read from :attr:`SOURCE_TABLE` and stacked with
                ``unionByName`` by the shared runner.

        Returns:
            *df* unchanged: the source ``*_etl`` scripts already applied the
            canonical structure, so the union only appends.
        """
        return df
