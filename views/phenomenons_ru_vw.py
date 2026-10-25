"""``geo.union_etl`` (Russia rows) → ``geo.phenomenons_ru_vw``.

The Russia twin of :mod:`views.phenomenons_us_vw` — the exact same
denormalized, one-row-per-snapshot display query for **map visualization in
Looker Studio**, filtered to ``country_code = 'RU'`` instead of ``'US'``:

* ``event_category`` / ``event_subcategory`` — ``initcap()``-capitalized;
* ``event_ndesc`` — ``event_name`` (fallback: capitalized category) plus
  ``":\\n\\n"`` + ``event_description`` when present;
* ``latitude``, ``longitude``, ``country_code``, ``state_province``,
  ``place_location`` — map pin and tooltip fields;
* ``danger_rating`` — the row's categorical rating passed through;
* ``metric_name``, ``metric_value``, ``metric_unit`` — the row's own metric;
* ``other_metrics`` — Looker Studio friendly ``key: value (unit); ...`` text
  (``*_unit`` companions consumed), NULL when the row has none;
* ``event_start_date`` — ``event_start``, falling back to midnight of
  ``file_date``; ``event_end_date`` — ``event_end`` as stored.

The query lives in ``config.json`` (``datasets.view.phenomenons_ru_vw``) and
every reshape happens **in the database** — this script is the usual wiring.

Run::

    python run_etl.py VIEW phenomenons_ru_vw

**Load type** — HISTORICAL only, like every view: each run re-proves the
query through Spark JDBC and replaces the whole view definition — see
:func:`utils.view_utils.run_view`.
"""

from model.config.datasets import ViewDatasetConfig, load_dataset
from views.view_script import ViewScript

_cfg = load_dataset(__file__, "view", ViewDatasetConfig)


class PhenomenonsRuVwScript(ViewScript):
    """Russia rows of ``geo.union_etl``, Looker-Studio-ready → ``geo.phenomenons_ru_vw``."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_VIEW = _cfg.target_view
    QUERY = _cfg.query
