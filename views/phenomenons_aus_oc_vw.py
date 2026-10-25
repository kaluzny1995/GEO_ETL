"""``geo.union_etl`` (Australia & Oceania rows) → ``geo.phenomenons_aus_oc_vw``.

The Australia/Oceania twin of :mod:`views.phenomenons_us_vw` — the exact
same denormalized, one-row-per-snapshot display query for **map
visualization in Looker Studio**, filtered to the **strict UN M49
definition of Oceania** (all four sub-regions):

* **Australia and New Zealand**: ``AU``, ``NZ``, ``NF``, ``CX``, ``CC``;
* **Melanesia**: ``FJ``, ``NC``, ``PG``, ``SB``, ``VU``;
* **Micronesia**: ``FM``, ``GU``, ``KI``, ``MH``, ``MP``, ``NR``, ``PW``,
  ``UM``;
* **Polynesia**: ``AS``, ``CK``, ``NU``, ``PF``, ``PN``, ``TK``, ``TO``,
  ``TV``, ``WF``, ``WS``, ``NU`` — 28 codes in total.

Complementary to the other regional twins (disjoint whitelists, pinned by
tests): Indian-Ocean territories stay with Africa (``RE``, ``YT``, ``SH``,
``IO``), ``TL`` stays with Asia, ``RU``/``ZZ`` are excluded, and sub-antarctic
codes (``AQ``, ``GS``) belong to no region view.

``AS AU CC CK CX FJ FM GU KI MH MP NC NF NR NU NZ PF PG PN PW SB TK TO TV UM
VU WF WS``

Everything else — ``initcap`` categories, the ``event_ndesc`` formula,
``danger_rating`` passthrough, metric columns, the ``key: value (unit)``
``other_metrics`` text and the ``event_start``/``file_date`` coalescing — is
byte-identical to the other phenomenons twins (pinned by a shared-shape
test).

The query lives in ``config.json`` (``datasets.view.phenomenons_aus_oc_vw``);
this script is the usual wiring.

Run::

    python run_etl.py VIEW phenomenons_aus_oc_vw

**Load type** — HISTORICAL only, like every view: each run re-proves the
query through Spark JDBC and replaces the whole view definition — see
:func:`utils.view_utils.run_view`.
"""

from model.config.datasets import ViewDatasetConfig, load_dataset
from views.view_script import ViewScript

_cfg = load_dataset(__file__, "view", ViewDatasetConfig)

#: ISO 3166-1 alpha-2 codes of UN M49 Oceania accepted by the view.
COUNTRY_CODES: tuple[str, ...] = (
    "AS",
    "AU",
    "CC",
    "CK",
    "CX",
    "FJ",
    "FM",
    "GU",
    "KI",
    "MH",
    "MP",
    "NC",
    "NF",
    "NR",
    "NU",
    "NZ",
    "PF",
    "PG",
    "PN",
    "PW",
    "SB",
    "TK",
    "TO",
    "TV",
    "UM",
    "VU",
    "WF",
    "WS",
)


class PhenomenonsAusOcVwScript(ViewScript):
    """Australia & Oceania rows of ``geo.union_etl`` (strict M49) → ``geo.phenomenons_aus_oc_vw``."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_VIEW = _cfg.target_view
    QUERY = _cfg.query
