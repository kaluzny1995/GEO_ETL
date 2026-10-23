"""``geo.union_etl`` (iceberg rows) → ``geo.metrics_iceberg_vw``.

Flattens every iceberg-category metric of the union table into one row per
(metric name, value, unit) triple, always carrying the row's ``file_date``
snapshot stamp (``YYYY-MM-DD`` cast to ``timestamp``) and its
``event_category`` / ``event_subcategory``:

* the row's **own** metric — ``metric_name`` / ``metric_value`` /
  ``metric_unit`` straight from ``geo.union_etl`` (EONET's ``surface`` in
  ``N/m²``), kept only when it carries a value;
* **appended** rows for the numeric entries of its ``other_metrics`` JSON —
  each key ``<name>`` with a JSON number value becomes a metric row, its unit
  taken from the companion ``<name>_unit`` key where the source ships one.
  EONET iceberg rows carry ``geometry_count`` (the event's geometry points —
  a count with no companion unit, so ``metric_unit`` is NULL there).
  Companion ``*_unit`` keys are units, not metrics, and are never emitted as
  their own rows; non-numeric entries carry no measurement and are skipped.

The view covers only ``event_category = 'iceberg'``; the query lives in
``config.json`` (``datasets.view.metrics_iceberg_vw``) and both the
filtering and the ``other_metrics`` explosion happen **in the database** —
this script is the usual wiring.

Run::

    python run_etl.py VIEW metrics_iceberg_vw

**Load type** — HISTORICAL only, like every view: each run re-proves the
query through Spark JDBC and replaces the whole view definition — see
:func:`utils.view_utils.run_view`.
"""

from model.config.datasets import ViewDatasetConfig, load_dataset
from views.view_script import ViewScript

_cfg = load_dataset(__file__, "view", ViewDatasetConfig)


class MetricsIcebergVwScript(ViewScript):
    """Iceberg metrics of ``geo.union_etl`` → ``geo.metrics_iceberg_vw``."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_VIEW = _cfg.target_view
    QUERY = _cfg.query
