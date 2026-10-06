"""``geo.avalanche_org_etl`` — Avalanche.org forecast zones in the canonical structure."""

from model.etl.event_etl import EventEtl


class AvalancheOrgEtl(EventEtl):
    """One row per forecast zone per day, transformed from ``geo.avalanche_org``.

    Mapping (see ``single_etl/avalanche_org_etl.py``):

    * ``source_name`` — always ``AVALANCHE_ORG``, ``country_code`` always ``US``.
    * ``subsource_name`` — issuing center (e.g. ``Colorado Avalanche Information Center``).
    * ``event_name`` — ``NULL`` (a forecast has no event name), ``place_location`` — the
      forecast zone, ``state_province`` — the zone's 2-letter state code.
    * ``event_category`` — ``avalanche``; ``event_subcategory`` — ``NULL`` (no subtypes).
    * ``metric_name`` — ``magnitude`` by convention; ``metric_value`` / ``metric_unit`` are
      always ``NULL``: a danger level is a rating, not a measurement.
    * ``danger_rating`` — the zone's rating as text (``low`` … ``extreme``); unrated zones
      (source ``danger_level = -1``, ``danger_rating = "no rating"``) get ``NULL``.
    * ``other_metrics`` — ``{"danger_level": …, "danger_level_unit": "NAPADS level",
      "warning": "warning", "off_season": …}``: the raw scale index (``-1`` = no rating)
      with its ``<name>_unit`` companion, the avalanche-warning product (omitted while
      no warning covers the zone) and the season state. Zone ids and links are not
      metrics.
    * ``latitude``/``longitude`` — centroid of the zone polygon, carried over
      from the source rows (computed by GEO_Download at download time);
      ``NULL`` when the source row has no coordinates.
    * ``event_start``/``event_end`` — forecast validity window
      (``forecast_start``/``forecast_end``).
    """
