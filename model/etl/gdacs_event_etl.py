"""``geo.gdacs_event_etl`` — GDACS disaster events in the canonical structure."""

from model.etl.event_etl import EventEtl


class GdacsEventEtl(EventEtl):
    """One row per GDACS event snapshot, transformed from ``geo.gdacs_event``.

    Mapping (see ``single_etl/gdacs_event_etl.py``):

    * ``source_name`` — always ``GDACS``; ``subsource_name`` — the contributing
      body (``NEIC``, ``NOAA``, ``JTWC``, ``GWIS``, ``GLOFAS``, ``GDO``, VAAC
      and seismic network names).
    * ``event_category`` — ``earthquake`` / ``storm`` / ``flood`` / ``volcano`` /
      ``fire`` / ``drought`` from ``event_type`` (``EQ`` / ``TC`` / ``FL`` /
      ``VO`` / ``WF`` / ``DR``), lowercased for any other code;
      ``event_subcategory`` — the cyclone's intensity class parsed from
      ``severity_text`` (``tropical depression``, ``tropical storm``,
      ``hurricane``), ``wildfire`` for fires, ``agricultural drought`` for
      droughts, ``NULL`` when the source states no subtype.
    * ``place_location`` — ``country`` (region at the event centroid, or the
      affected-country list), ``NULL`` when the source states none.
    * ``country_code`` — the source's ISO 3166-1 alpha-3 ``country_code``
      translated to alpha-2, else the download-time stamped
      ``geocode_country``, else ``ZZ``; ``state_province`` — the stamped
      ``geocode_state`` (GDACS carries none), ``NULL`` when unknown.
    * ``metric_name`` — ``magnitude`` (EQ/FL/VO), ``wind speed`` (TC), ``area``
      (WF/DR); ``metric_value`` — ``severity``, keeping the ``0`` ("not
      defined" everywhere FL and VO report one) as ``0`` rather than
      ``NULL``; ``metric_unit`` — ``severity_unit`` (``M``, ``km/h``, ``ha``,
      ``km2``), ``NULL`` when blank.
    * ``danger_rating`` — ``alert_level`` (``Green`` / ``Orange`` / ``Red``)
      mapped onto the shared scale (``none`` / ``medium`` / ``high``);
      ``no_rating`` when the row carries no alert level the map knows.
    * ``other_metrics`` / ``event_description`` / ``last_latitude`` /
      ``last_longitude`` — ``NULL`` (GDACS carries none).
    * ``event_start`` — ``from_date``; ``event_end`` —
      ``to_date`` (equal to the start for the instantaneous earthquakes).
    """
