"""``geo.union_etl`` (United States rows) → ``geo.phenomenons_us_vw``.

A denormalized, one-row-per-snapshot view built for **map visualization in
Looker Studio** — unlike the ``metrics_*`` views it does not explode metrics
into rows; it keeps the union table's own metric columns and reshapes the
rest for display. Only ``country_code = 'US'`` rows are included.

Display columns (in order):

* ``event_category`` / ``event_subcategory`` — ``initcap()``-capitalized
  (``earthquake`` → ``Earthquake``);
* ``event_ndesc`` — the display name: ``event_name`` (falling back to the
  capitalized category when NULL) and, when ``event_description`` exists,
  ``":\\n\\n"`` + the description appended to it;
* ``latitude``, ``longitude``, ``country_code``, ``state_province``,
  ``place_location`` — passed through for the map pin and its tooltip;
* ``danger_rating`` — the row's categorical rating passed through as stored
  (``low`` / ``medium`` / ``high`` / ``extreme`` / ``none`` / ``no_rating``);
* ``metric_name``, ``metric_value``, ``metric_unit`` — the row's own metric;
* ``other_metrics`` — **Looker Studio friendly text**: every non-``*_unit``
  JSON entry rendered as ``key: value (unit)`` pairs joined by ``"; "``
  (e.g. ``danger_level: -1 (NAPADS level); off_season: true``), NULL when
  the row has no ``other_metrics``;
* ``event_start_date`` — ``event_start``, falling back to midnight of
  ``file_date`` when NULL;
* ``event_end_date`` — ``event_end`` as stored (often NULL: only events
  that ended carry it).

The query lives in ``config.json`` (``datasets.view.phenomenons_us_vw``)
and every reshape happens **in the database** — this script is the usual
wiring.

Run::

    python run_etl.py VIEW phenomenons_us_vw

**Load type** — HISTORICAL only, like every view: each run re-proves the
query through Spark JDBC and replaces the whole view definition — see
:func:`utils.view_utils.run_view`.
"""

from model.config.datasets import ViewDatasetConfig, load_dataset
from views.view_script import ViewScript

_cfg = load_dataset(__file__, "view", ViewDatasetConfig)


class PhenomenonsUsVwScript(ViewScript):
    """US rows of ``geo.union_etl``, Looker-Studio-ready → ``geo.phenomenons_us_vw``."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_VIEW = _cfg.target_view
    QUERY = _cfg.query
