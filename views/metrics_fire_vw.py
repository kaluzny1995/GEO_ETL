"""``geo.union_etl`` (fire rows) → ``geo.metrics_fire_vw``.

Flattens every fire-category metric of the union table into one row per
(metric name, value, unit) triple, always carrying the row's ``file_date``
snapshot stamp (``YYYY-MM-DD`` cast to ``timestamp``) and its
``event_category`` / ``event_subcategory``:

* the row's **own** metric — ``metric_name`` / ``metric_value`` /
  ``metric_unit`` straight from ``geo.union_etl`` (the fire perimeter's
  ``area`` in ``km²``), kept only when it carries a value;
* **appended** rows for the numeric entries of its ``other_metrics`` JSON —
  each key ``<name>`` with a JSON number value becomes a metric row, its unit
  taken from the companion ``<name>_unit`` key where the source ships one:
  NIFC rows contribute ``percent_contained`` / ``%``, EONET fire rows
  contribute ``geometry_count`` (the event's geometry points — a count with
  no companion unit, so ``metric_unit`` is NULL there). Rows with NULL
  ``other_metrics`` contribute only their own metric. Companion ``*_unit``
  keys are units, not metrics, and are never emitted as their own rows;
  non-numeric entries carry no measurement and are skipped.

The view covers only ``event_category = 'fire'``; the query lives in
``config.json`` (``datasets.view.metrics_fire_vw``) and both the filtering
and the ``other_metrics`` explosion happen **in the database** — this script
is the usual wiring.

Run::

    python run_etl.py VIEW metrics_fire_vw

**Load type** — HISTORICAL only, like every view: each run re-proves the
query through Spark JDBC and replaces the whole view definition — see
:func:`utils.view_utils.run_view`.
"""

from model.config.datasets import ViewDatasetConfig, load_dataset
from views.view_script import ViewScript

_cfg = load_dataset(__file__, "view", ViewDatasetConfig)


class MetricsFireVwScript(ViewScript):
    """Fire metrics of ``geo.union_etl`` → ``geo.metrics_fire_vw``."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_VIEW = _cfg.target_view
    QUERY = _cfg.query
