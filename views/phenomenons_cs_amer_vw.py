"""``geo.union_etl`` (Central & South America rows) → ``geo.phenomenons_cs_amer_vw``.

Third twin of :mod:`views.phenomenons_us_vw` — the exact same denormalized,
one-row-per-snapshot display query for **map visualization in Looker
Studio**, filtered to the Americas **without** the United States and Canada:
``country_code IN (<ISO 3166-1 alpha-2 list>)``.

The whitelist is every American country/territory code (UN M49 Northern +
Central + South America and the Caribbean) minus ``US`` and ``CA`` — a bare
``NOT IN ('US','CA')`` would wrongly pull in Europe, Africa, Asia and
Oceania:

``AG AI AR AW BB BL BM BO BQ BR BS BV BZ CL CO CR CU CW DM DO EC FK GD GF GL
GP GS GT GY HN HT JM KN KY LC MF MQ MS MX NI PA PE PM PR PY SR SV SX TC TT
UY VC VE VG VI``

Audited against the UN M49 Americas set (55 codes incl. ``BM``, ``BV``,
``GS``, ``KY``, ``SV`` — some with no records yet, kept proactively).

Everything else — ``initcap`` categories, the ``event_ndesc`` formula,
``danger_rating`` passthrough, metric columns, the ``key: value (unit)``
``other_metrics`` text and the ``event_start``/``file_date`` coalescing — is
byte-identical to the US/CA twins (pinned by a shared-shape test).

The query lives in ``config.json``
(``datasets.view.phenomenons_cs_amer_vw``); this script is the usual wiring.

Run::

    python run_etl.py VIEW phenomenons_cs_amer_vw

**Load type** — HISTORICAL only, like every view: each run re-proves the
query through Spark JDBC and replaces the whole view definition — see
:func:`utils.view_utils.run_view`.
"""

from model.config.datasets import ViewDatasetConfig, load_dataset
from views.view_script import ViewScript

_cfg = load_dataset(__file__, "view", ViewDatasetConfig)

#: ISO 3166-1 alpha-2 codes from ``config.json`` — the Americas (minus US/CA) accepted by the view.
COUNTRY_CODES: tuple[str, ...] = tuple(_cfg.country_codes)


class PhenomenonsCsAmerVwScript(ViewScript):
    """Central & South America rows of ``geo.union_etl`` → ``geo.phenomenons_cs_amer_vw``."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_VIEW = _cfg.target_view
    QUERY = _cfg.query
