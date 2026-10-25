"""``geo.union_etl`` (European rows) → ``geo.phenomenons_europe_vw``.

The Europe twin of :mod:`views.phenomenons_us_vw` — the exact same
denormalized, one-row-per-snapshot display query for **map visualization in
Looker Studio**, filtered to the **strict UN M49 definition of Europe
without Russia**:

* whitelist = every ISO 3166-1 alpha-2 code of UN M49 **Europe** (sovereign
  states, microstates and territories — ``SM``, ``FO``, ``XK``, ``AX``,
  ``SJ``, …) **minus ``RU``**;
* **excluded** the transcontinental/M49-“Western Asia” edge cases per the
  agreed definition: ``TR``, ``CY``, ``AM``, ``AZ``, ``GE`` (a bare
  “Europe” list would otherwise smuggle them in) and ``ZZ`` (unknown).

``AD AL AT AX BA BE BG BY CH CZ DE DK EE ES FI FO FR GB GG GI GR HR HU IE IM
IS IT JE LI LT LU LV MC MD ME MK MT NL NO PL PT RO RS SE SI SJ SK SM UA VA XK``

Everything else — ``initcap`` categories, the ``event_ndesc`` formula,
``danger_rating`` passthrough, metric columns, the ``key: value (unit)``
``other_metrics`` text and the ``event_start``/``file_date`` coalescing — is
byte-identical to the other phenomenons twins (pinned by a shared-shape
test).

The query lives in ``config.json``
(``datasets.view.phenomenons_europe_vw``); this script is the usual wiring.

Run::

    python run_etl.py VIEW phenomenons_europe_vw

**Load type** — HISTORICAL only, like every view: each run re-proves the
query through Spark JDBC and replaces the whole view definition — see
:func:`utils.view_utils.run_view`.
"""

from model.config.datasets import ViewDatasetConfig, load_dataset
from views.view_script import ViewScript

_cfg = load_dataset(__file__, "view", ViewDatasetConfig)

#: ISO 3166-1 alpha-2 codes of UN M49 Europe (minus RU) accepted by the view.
COUNTRY_CODES: tuple[str, ...] = (
    "AD",
    "AL",
    "AT",
    "AX",
    "BA",
    "BE",
    "BG",
    "BY",
    "CH",
    "CZ",
    "DE",
    "DK",
    "EE",
    "ES",
    "FI",
    "FO",
    "FR",
    "GB",
    "GG",
    "GI",
    "GR",
    "HR",
    "HU",
    "IE",
    "IM",
    "IS",
    "IT",
    "JE",
    "LI",
    "LT",
    "LU",
    "LV",
    "MC",
    "MD",
    "ME",
    "MK",
    "MT",
    "NL",
    "NO",
    "PL",
    "PT",
    "RO",
    "RS",
    "SE",
    "SI",
    "SJ",
    "SK",
    "SM",
    "UA",
    "VA",
    "XK",
)


class PhenomenonsEuropeVwScript(ViewScript):
    """European rows of ``geo.union_etl`` (strict M49, no Russia) → ``geo.phenomenons_europe_vw``."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_VIEW = _cfg.target_view
    QUERY = _cfg.query
