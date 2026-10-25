"""``geo.union_etl`` (Asian rows) → ``geo.phenomenons_asia_vw``.

The Asia twin of :mod:`views.phenomenons_us_vw` — the exact same
denormalized, one-row-per-snapshot display query for **map visualization in
Looker Studio**, filtered to the **strict UN M49 definition of Asia
without Russia**:

* whitelist = every ISO 3166-1 alpha-2 code of UN M49 **Asia** (Eastern,
  Southern, South-eastern, Western and Central Asia — including microstates
  and territories such as ``HK`` and ``MO``) **minus ``RU``**;
* complementary to :mod:`views.phenomenons_europe_vw`: the two whitelists
  are disjoint (pinned by a cross-view test), and the M49-“Western Asia”
  edge cases excluded from the Europe view — ``TR``, ``CY``, ``AM``, ``AZ``,
  ``GE`` — sit **here**, so every country lands in exactly one view;
* Oceania stays out (``AU``, ``NZ``, ``PG``, … are M49 Oceania, not Asia)
  and ``ZZ`` (unknown) is excluded.

``AE AF AM AZ BD BH BN BT CN CY GE HK ID IL IN IQ IR JO JP KG KH KP KR KW KZ
LA LB LK MM MN MO MV MY NP OM PH PK PS QA SA SG SY TH TJ TL TM TR TW UZ VN YE``

Everything else — ``initcap`` categories, the ``event_ndesc`` formula,
``danger_rating`` passthrough, metric columns, the ``key: value (unit)``
``other_metrics`` text and the ``event_start``/``file_date`` coalescing — is
byte-identical to the other phenomenons twins (pinned by a shared-shape
test).

The query lives in ``config.json`` (``datasets.view.phenomenons_asia_vw``);
this script is the usual wiring.

Run::

    python run_etl.py VIEW phenomenons_asia_vw

**Load type** — HISTORICAL only, like every view: each run re-proves the
query through Spark JDBC and replaces the whole view definition — see
:func:`utils.view_utils.run_view`.
"""

from model.config.datasets import ViewDatasetConfig, load_dataset
from views.view_script import ViewScript

_cfg = load_dataset(__file__, "view", ViewDatasetConfig)

#: ISO 3166-1 alpha-2 codes of UN M49 Asia (minus RU) accepted by the view.
COUNTRY_CODES: tuple[str, ...] = (
    "AE",
    "AF",
    "AM",
    "AZ",
    "BD",
    "BH",
    "BN",
    "BT",
    "CN",
    "CY",
    "GE",
    "HK",
    "ID",
    "IL",
    "IN",
    "IQ",
    "IR",
    "JO",
    "JP",
    "KG",
    "KH",
    "KP",
    "KR",
    "KW",
    "KZ",
    "LA",
    "LB",
    "LK",
    "MM",
    "MN",
    "MO",
    "MV",
    "MY",
    "NP",
    "OM",
    "PH",
    "PK",
    "PS",
    "QA",
    "SA",
    "SG",
    "SY",
    "TH",
    "TJ",
    "TL",
    "TM",
    "TR",
    "TW",
    "UZ",
    "VN",
    "YE",
)


class PhenomenonsAsiaVwScript(ViewScript):
    """Asian rows of ``geo.union_etl`` (strict M49, no Russia) → ``geo.phenomenons_asia_vw``."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_VIEW = _cfg.target_view
    QUERY = _cfg.query
