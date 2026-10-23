"""``geo.union_etl`` (flood rows) → ``geo.metrics_flood_vw``.

Flattens every flood-category metric of the union table into one row per
(metric name, value, unit) triple, always carrying the row's ``file_date``
snapshot stamp (``YYYY-MM-DD`` cast to ``timestamp``) and its
``event_category`` / ``event_subcategory``:

* the row's **own** metric — ``metric_name`` / ``metric_value`` /
  ``metric_unit`` straight from ``geo.union_etl`` (``discharge`` in ``m³/s``
  for OPENMETEO and USGS_NWIS rows, ``magnitude`` without a unit for GDACS),
  kept only when it carries a value;
* **appended** rows for the numeric entries of its ``other_metrics`` JSON —
  each key ``<name>`` with a JSON number value becomes a metric row, its unit
  taken from the companion ``<name>_unit`` key where the source ships one
  (``threshold`` in ``m³/s``; unit-less numbers such as ``baseline_days`` and
  ``exceedance_ratio`` land with ``metric_unit = NULL``). Non-numeric
  entries — booleans like ``threshold_exceeded``, dates, gauge ids and
  levels — carry no measurement and are skipped, as are the ``*_unit``
  companions themselves; rows with NULL ``other_metrics`` (GDACS) contribute
  only their own metric.

Both branches are whitelisted to exactly five metrics — ``discharge``,
``threshold``, ``baseline_days``, ``exceedance_ratio`` and ``magnitude`` —
so the OPENMETEO percentile/ensemble block and the NWIS stage metrics never
reach the view regardless of what other keys future sources add.

The view covers only ``event_category = 'flood'``; the query lives in
``config.json`` (``datasets.view.metrics_flood_vw``) and both the filtering
and the ``other_metrics`` explosion happen **in the database** — this script
is the usual wiring.

Run::

    python run_etl.py VIEW metrics_flood_vw

**Load type** — HISTORICAL only, like every view: each run re-proves the
query through Spark JDBC and replaces the whole view definition — see
:func:`utils.view_utils.run_view`.
"""

from model.config.datasets import ViewDatasetConfig, load_dataset
from views.view_script import ViewScript

_cfg = load_dataset(__file__, "view", ViewDatasetConfig)


class MetricsFloodVwScript(ViewScript):
    """Flood metrics of ``geo.union_etl`` → ``geo.metrics_flood_vw``."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_VIEW = _cfg.target_view
    QUERY = _cfg.query
