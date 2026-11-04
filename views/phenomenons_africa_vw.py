"""``geo.union_etl`` (African rows) → ``geo.phenomenons_africa_vw``.

The Africa twin of :mod:`views.phenomenons_us_vw` — the exact same
denormalized, one-row-per-snapshot display query for **map visualization in
Looker Studio**, filtered to the **strict UN M49 definition of Africa**
(all five sub-regions):

* whitelist = the **54 AU member states** plus the M49 African territories:
  ``EH`` (Western Sahara), ``IO``, ``RE``, ``YT``, ``SH``, ``TF`` — 60 codes;
* complementary to :mod:`views.phenomenons_europe_vw` and
  :mod:`views.phenomenons_asia_vw` (disjoint from both, pinned by tests):
  Egypt and Sudan are M49 **Northern Africa** (so they belong here, not in
  Asia), the M49-“Western Asia” edge cases (``TR``/``CY``/``AM``/``AZ``/
  ``GE``) stay out, Oceania (``AU``/``NZ``) and ``ZZ`` (unknown) are excluded;
* note: Spain's Canary Islands etc. are coded to their parent country
  (``ES``) by ``country_code`` and therefore surface in the Europe view.

``AO BF BI BJ BW CD CF CG CI CM CV DJ DZ EG EH ER ET GA GH GM GN GQ GW IO KE
KM LR LS LY MA MG ML MR MU MW MZ NA NE NG RE RW SC SD SH SL SN SO SS ST SZ TD
TF TG TN TZ UG YT ZA ZM ZW``

Everything else — ``initcap`` categories, the ``event_ndesc`` formula,
``danger_rating`` passthrough, metric columns, the ``key: value (unit)``
``other_metrics`` text and the ``event_start``/``file_date`` coalescing — is
byte-identical to the other phenomenons twins (pinned by a shared-shape
test).

The query lives in ``config.json`` (``datasets.view.phenomenons_africa_vw``);
this script is the usual wiring.

Run::

    python run_etl.py VIEW phenomenons_africa_vw

**Load type** — HISTORICAL only, like every view: each run re-proves the
query through Spark JDBC and replaces the whole view definition — see
:func:`utils.view_utils.run_view`.
"""

from model.config.datasets import ViewDatasetConfig, load_dataset
from views.view_script import ViewScript

_cfg = load_dataset(__file__, "view", ViewDatasetConfig)

#: ISO 3166-1 alpha-2 codes from ``config.json`` — UN M49 Africa accepted by the view.
COUNTRY_CODES: tuple[str, ...] = tuple(_cfg.country_codes)


class PhenomenonsAfricaVwScript(ViewScript):
    """African rows of ``geo.union_etl`` (strict M49) → ``geo.phenomenons_africa_vw``."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_VIEW = _cfg.target_view
    QUERY = _cfg.query
