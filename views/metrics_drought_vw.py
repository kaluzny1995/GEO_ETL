"""``geo.union_etl`` (drought rows) → ``geo.metrics_drought_vw``.

Flattens every drought-category metric of the union table into one row per
(metric name, value, unit) triple, always carrying the row's ``file_date``
snapshot stamp (``YYYY-MM-DD`` cast to ``timestamp``) and its
``event_category`` / ``event_subcategory``:

* the row's **own** metric — ``metric_name`` / ``metric_value`` /
  ``metric_unit`` straight from ``geo.union_etl``, kept only when it carries
  a value (rows with a bare name and no measurement are skipped);
* **appended** rows for the numeric entries of its ``other_metrics`` JSON —
  each key ``<name>`` with a JSON number value becomes a metric row, its unit
  taken from the companion ``<name>_unit`` key the canonical structure
  guarantees. Companion ``*_unit`` keys are units, not metrics, and are never
  emitted as their own rows; non-numeric entries (free text and boolean
  flags) carry no measurement and are skipped.

Today's drought rows all come from GDACS, whose own metric is ``area`` /
``km²`` and whose ``other_metrics`` is always NULL — the explosion branch
contributes no rows yet but is kept so the view behaves identically if other
sources start publishing drought rows with ``other_metrics`` (it is also
drought-only: only ``event_category = 'drought'`` rows are read). The query
lives in ``config.json`` (``datasets.view.metrics_drought_vw``) and all
filtering happens **in the database** — this script is the usual wiring.

Run::

    python run_etl.py VIEW metrics_drought_vw

**Load type** — HISTORICAL only, like every view: each run re-proves the
query through Spark JDBC and replaces the whole view definition — see
:func:`utils.view_utils.run_view`.
"""

from model.config.datasets import ViewDatasetConfig, load_dataset
from views.view_script import ViewScript

_cfg = load_dataset(__file__, "view", ViewDatasetConfig)


class MetricsDroughtVwScript(ViewScript):
    """Drought metrics of ``geo.union_etl`` → ``geo.metrics_drought_vw``."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_VIEW = _cfg.target_view
    QUERY = _cfg.query
