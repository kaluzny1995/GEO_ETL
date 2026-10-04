"""``geo.usgs_earthquake_etl`` — USGS seismic events in the canonical structure."""

from model.etl.event_etl import EventEtl


class UsgsEarthquakeEtl(EventEtl):
    """One row per USGS seismic event, transformed from ``geo.usgs_earthquake``.

    Mapping (see ``single_etl/usgs_earthquake_etl.py``):

    * ``source_name`` — always ``USGS``; ``subsource_name`` — the ``net``
      of the preferred origin (``us``, ``ak``, ``ci``, …, verbatim).
    * ``event_name`` — ``"Earthquake - <country name>"``: ``United States``
      for US-suffixed places, the ``place`` suffix itself for
      international events (``Earthquake - Chile``), the stamped
      ``geocode_country`` for places the suffix does not cover;
      ``"Earthquake"`` alone when no country resolves.
    * ``event_category`` — always ``earthquake``; ``event_subcategory`` —
      the source ``event_type`` (``earthquake``, ``quarry blast``,
      ``explosion``, ``ice quake``).
    * ``event_description`` — ``"<event_type> <status>"``
      (``earthquake reviewed``, ``quarry blast automatic``).
    * ``country_code`` — from the country name in ``place``: a US state
      suffix (2-letter code or full name) → ``US``, an international
      suffix → the 60-entry name→ISO map (``Chile`` → ``CL``, ``MX`` →
      ``MX``, territories → ``US``), otherwise the stamped
      ``geocode_country``; ``ZZ`` when nothing resolves.
    * ``state_province`` — ``net`` uppercased (``ak`` → ``AK``), the
      stamped ``geocode_state`` when ``net`` is blank, ``NULL`` when
      neither resolves.
    * ``place_location`` — the source ``place`` verbatim.
    * ``metric_name`` — always ``magnitude``; ``metric_value`` — ``mag``
      (always reported); ``metric_unit`` — ``magType`` per row (``ml``,
      ``mb``, ``mww``, …): a magnitude is only meaningful with its type.
    * ``other_metrics`` — ``{"depth", "depth_unit": "km", "nst",
      "nst_unit": "stations"}``; the ``nst`` keys are omitted when the
      network reports no station count.
    * ``event_start`` — ``time`` (the rupture origin time, millisecond
      precision, always present); ``event_end`` — ``NULL`` (an instant has
      no end).
    * ``danger_rating`` / ``last_latitude`` /
      ``last_longitude`` — ``NULL`` (``status`` is QC, not severity; the
      epicentre is fixed).
    * ``file_date`` — the snapshot day the event was downloaded for.
    """
