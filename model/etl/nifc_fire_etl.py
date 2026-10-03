"""``geo.nifc_fire_etl`` — NIFC wildfire perimeters in the canonical structure."""

from model.etl.event_etl import EventEtl


class NifcFireEtl(EventEtl):
    """One row per NIFC fire perimeter snapshot, transformed from ``geo.nifc_fire``.

    Mapping (see ``single_etl/nifc_fire_etl.py``):

    * ``source_name`` — always ``NIFC``; ``subsource_name`` — always
      ``WFIGS`` (the Wildland Fire Interagency Geospatial Services layer the
      perimeters come from).
    * ``event_name`` — the perimeter ``incident_name``, ``NULL`` when blank.
    * ``event_category`` — always ``fire``; ``event_subcategory`` — from
      ``fire_type``: ``WF`` → ``wildfire``, ``RX`` → ``prescribed fire``,
      ``NULL`` when the source states no type.
    * ``country_code`` — always ``US`` (the layer only publishes US
      perimeters); ``state_province`` — the source ``state`` when present,
      otherwise the reverse-geocoded centroid (``cache/reverse_geocode.csv``,
      covering the ~127 rows whose vintage predates the ``state`` field),
      ``NULL`` when neither resolves.
    * ``place_location`` — the jurisdiction unit's full name as it ships
      with the source row in ``unit_name`` (GEO_Download resolves
      ``attr_POOJurisdictionalUnit`` through its ``data/nifc_unit_names.csv``
      lookup at download time: ``WAOWF`` → ``Okanogan-Wenatchee National
      Forest``, ``CAYNP`` → ``Yosemite National Park``; a code the lookup
      does not know keeps its raw value, a blank one is ``NULL``). The
      agency codes (``attr_POOJurisdictionalAgency``) have no published
      domain, so they are not used.
    * ``metric_name`` — always ``area``; ``metric_value`` — the perimeter
      ``acres``, counting a missing acreage as ``0`` (the pre-2026-09-18
      vintage predates the field); ``metric_unit`` — always ``acres``.
    * ``other_metrics`` — ``{"percent_contained": …,
      "percent_contained_unit": "%"}``, always present: a missing
      ``percent_contained`` counts as ``0`` (nothing reported yet).
    * ``danger_rating`` / ``event_description`` / ``event_end`` /
      ``last_latitude`` / ``last_longitude`` — ``NULL`` (the
      perimeter layer carries no alert level, advisory prose, fire-out
      datetime or latest position).
    * ``event_start`` — ``created_on`` (always present).
    """
