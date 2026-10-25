"""``geo.union_etl`` (open-water events) → ``geo.phenomenons_waters_vw``.

The **water** twin of :mod:`views.phenomenons_us_vw` — the exact same
denormalized, one-row-per-snapshot display query for **map visualization in
Looker Studio**, filtered to events that happened **on oceans and seas**.

No PostGIS/land polygons exist in the database, so water-ness is decided by
a pure-SQL heuristic over ``country_code``, ``event_subcategory`` and the
event text (with a geocode guard), per the agreed definition:

1. **geocode present** — ``latitude IS NOT NULL AND longitude IS NOT NULL``;
2. ``country_code = 'ZZ'`` — the provider's *no country* marker, used for
   open-water rows (mid-ocean typhoons, offshore/undersea earthquake
   epicenters, drifting icebergs);
3. ``country_code = 'AQ'`` — Antarctica has no continent view, so all of
   its events (icebergs, sea-ice, polar/quadrant earthquakes) are routed
   to the water/ice domain here;
4. ``event_subcategory = 'sea_lake_ice'`` — iceberg/sea-ice events;
5. **maritime keywords** in ``event_name``/``place_location``/
   ``event_description`` — ``ocean``, ``sea``, ``gulf``, ``strait``,
   ``offshore``, ``coast``, or the phrases ``off the coast`` /
   ``open water`` (PG word-boundary ``\\y`` regex, case-insensitive
   ``~*``). ``bay`` and ``maritime`` were deliberately dropped after an
   audit: they matched *land* places (``Pedro Bay``/``Cruz Bay`` Alaskan
   towns, Alaska's ``Maritime`` region) instead of water.

**Known trade-off** (agreed): events on water that carry a *real* country
code and no maritime wording (e.g. an offshore earthquake placed
``114 km E of ... , Japan``) are not matched — a PostGIS land-polygon test
would be needed to catch those; the heuristic was chosen deliberately to
avoid any extension/helper-table infrastructure. Conversely a handful of
land places named after water still match (``Ocean City``, ``Sea Ranch``).

Live shape: 2,392 rows (storm 1,774, earthquake 557, iceberg 61).
``country_code`` stays in the output so ``ZZ``/``AQ``/… rows are visible in
Looker Studio.

The query lives in ``config.json`` (``datasets.view.phenomenons_waters_vw``);
this script is the usual wiring.

Run::

    python run_etl.py VIEW phenomenons_waters_vw

**Load type** — HISTORICAL only, like every view: each run re-proves the
query through Spark JDBC and replaces the whole view definition — see
:func:`utils.view_utils.run_view`.
"""

from model.config.datasets import ViewDatasetConfig, load_dataset
from views.view_script import ViewScript

_cfg = load_dataset(__file__, "view", ViewDatasetConfig)


class PhenomenonsWatersVwScript(ViewScript):
    """Open-water (ocean/sea) rows of ``geo.union_etl`` → ``geo.phenomenons_waters_vw``."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_VIEW = _cfg.target_view
    QUERY = _cfg.query
