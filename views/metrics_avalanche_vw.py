"""``geo.union_etl`` (avalanche rows) → ``geo.metrics_avalanche_vw``.

Flattens every avalanche-category metric of the union table into one row per
(metric name, value, unit) triple, always carrying the row's ``file_date``
snapshot stamp (``YYYY-MM-DD`` cast to ``timestamp``) and its
``event_category`` / ``event_subcategory``:

* the row's **own** metric — ``metric_name`` / ``metric_value`` /
  ``metric_unit`` straight from ``geo.union_etl``, kept only when it carries
  a value (rows with a bare name and no measurement, e.g. ``size`` with NULL
  value/unit, are skipped);
* **appended** rows for the numeric entries of its ``other_metrics`` JSON —
  each key ``<name>`` with a JSON number value becomes a metric row, its unit
  taken from the companion ``<name>_unit`` key the canonical structure
  guarantees (e.g. ``{"danger_level": 3, "danger_level_unit": "NAPADS
  level"}`` yields ``danger_level`` / ``3`` / ``NAPADS level``). Companion
  ``*_unit`` keys are units, not metrics, and are never emitted as their own
  rows; non-numeric entries (free text and boolean flags such as ``warning``
  or ``off_season``) carry no measurement and are skipped.

The view covers only ``event_category = 'avalanche'``; the query lives in
``config.json`` (``datasets.view.metrics_avalanche_vw``) and both the
filtering and the ``other_metrics`` explosion happen **in the database** —
this script is the usual wiring.

Run::

    python run_etl.py VIEW metrics_avalanche_vw

**Load type** — HISTORICAL only, like every view: each run re-proves the
query through Spark JDBC and replaces the whole view definition — see
:func:`utils.view_utils.run_view`.
"""

from model.config.datasets import ViewDatasetConfig, load_dataset
from views.view_script import ViewScript

_cfg = load_dataset(__file__, "view", ViewDatasetConfig)


class MetricsAvalancheVwScript(ViewScript):
    """Avalanche metrics of ``geo.union_etl`` → ``geo.metrics_avalanche_vw``."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_VIEW = _cfg.target_view
    QUERY = _cfg.query
