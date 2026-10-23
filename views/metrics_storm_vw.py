"""``geo.union_etl`` (storm rows) → ``geo.metrics_storm_vw``.

Flattens every storm-category metric of the union table into one row per
(metric name, value, unit) triple, always carrying the row's ``file_date``
snapshot stamp (``YYYY-MM-DD`` cast to ``timestamp``) and its
``event_category`` / ``event_subcategory``:

* the row's **own** metric — ``metric_name`` / ``metric_value`` /
  ``metric_unit`` straight from ``geo.union_etl`` (``wind speed`` in
  ``km/h``, published by IBTRACS, GDACS and EONET alike), kept only when it
  carries a value;
* **appended** rows for the numeric entries of its ``other_metrics`` JSON —
  each key ``<name>`` with a JSON number value becomes a metric row, its unit
  taken from the companion ``<name>_unit`` key where the source ships one:
  IBTRACS rows contribute ``pressure`` / ``mb``, ``speed`` / ``km/h``,
  ``direction`` / ``degrees``, ``gust`` / ``km/h`` and ``dist2land`` /
  ``landfall`` in ``km``; EONET storm rows contribute ``geometry_count``
  (the event's geometry points — a count with no companion unit, so
  ``metric_unit`` is NULL there). Rows with NULL ``other_metrics`` (GDACS)
  contribute only their own metric. The ``saffir_simpson`` class is a
  string, carries no measurement and is skipped, as are the ``*_unit``
  companions themselves.

The view covers only ``event_category = 'storm'``; the query lives in
``config.json`` (``datasets.view.metrics_storm_vw``) and both the filtering
and the ``other_metrics`` explosion happen **in the database** — this script
is the usual wiring.

Run::

    python run_etl.py VIEW metrics_storm_vw

**Load type** — HISTORICAL only, like every view: each run re-proves the
query through Spark JDBC and replaces the whole view definition — see
:func:`utils.view_utils.run_view`.
"""

from model.config.datasets import ViewDatasetConfig, load_dataset
from views.view_script import ViewScript

_cfg = load_dataset(__file__, "view", ViewDatasetConfig)


class MetricsStormVwScript(ViewScript):
    """Storm metrics of ``geo.union_etl`` → ``geo.metrics_storm_vw``."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_VIEW = _cfg.target_view
    QUERY = _cfg.query
