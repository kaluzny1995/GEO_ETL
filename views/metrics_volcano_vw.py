"""``geo.union_etl`` (volcano rows) → ``geo.metrics_volcano_vw``.

Flattens every volcano-category metric of the union table into one row per
(metric name, value, unit) triple, always carrying the row's ``file_date``
snapshot stamp (``YYYY-MM-DD`` cast to ``timestamp``) and its
``event_category`` / ``event_subcategory``:

* the row's **own** metric — ``metric_name`` / ``metric_value`` /
  ``metric_unit`` straight from ``geo.union_etl`` (USGS's ``vei`` in ``VEI``,
  GDACS's ``magnitude`` without a unit), kept only when it carries a value;
* **appended** rows for the numeric entries of its ``other_metrics`` JSON —
  each key ``<name>`` with a JSON number value becomes a metric row, its unit
  taken from the companion ``<name>_unit`` key where the source ships one:
  USGS volcano notices contribute ``vnum`` / ``Smithsonian GVP`` (the
  Smithsonian GVP volcano number — an identifier in numeric form) and, on
  damage-affected volcanoes, ``casualties`` / ``people`` and ``damage`` /
  ``million USD``. Rows with NULL ``other_metrics`` (GDACS) contribute only
  their own metric. The ``color_code``, ``alert_level`` and
  ``notice_identifier`` strings carry no measurement and are skipped, as are
  the ``*_unit`` companions themselves.

The view covers only ``event_category = 'volcano'``; the query lives in
``config.json`` (``datasets.view.metrics_volcano_vw``) and both the
filtering and the ``other_metrics`` explosion happen **in the database** —
this script is the usual wiring.

Run::

    python run_etl.py VIEW metrics_volcano_vw

**Load type** — HISTORICAL only, like every view: each run re-proves the
query through Spark JDBC and replaces the whole view definition — see
:func:`utils.view_utils.run_view`.
"""

from model.config.datasets import ViewDatasetConfig, load_dataset
from views.view_script import ViewScript

_cfg = load_dataset(__file__, "view", ViewDatasetConfig)


class MetricsVolcanoVwScript(ViewScript):
    """Volcano metrics of ``geo.union_etl`` → ``geo.metrics_volcano_vw``."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_VIEW = _cfg.target_view
    QUERY = _cfg.query
