"""``geo.openmeteo_flood_etl`` — Open-Meteo/GloFAS river discharge in the canonical structure."""

from model.etl.event_etl import EventEtl


class OpenMeteoFloodEtl(EventEtl):
    """One row per river discharge value snapshot, transformed from ``geo.openmeteo_flood``.

    Mapping (see ``single_etl/openmeteo_flood_etl.py``):

    * ``source_name`` — always ``OPENMETEO``; ``subsource_name`` — always
      ``GLOFAS`` (the Copernicus GloFAS model the discharge is simulated
      with).
    * ``event_name`` — ``"<river_name> discharge"`` (``Amazon discharge``),
      ``NULL`` when the river name is blank.
    * ``event_category`` — always ``flood``; ``event_subcategory`` /
      ``event_description`` — ``NULL`` (no subtype or prose).
    * ``country_code`` — the source ``country`` name mapped through the
      script's 54-entry ISO map (``USA`` → ``US``, ``UK`` → ``GB``,
      ``DRC`` → ``CD``); a name the map does not know falls back to the
      reverse-geocoded reference point, an empty geocode counts as a miss
      and resolves to ``ZZ``.
    * ``state_province`` — the reverse-geocoded reference point
      (``cache/reverse_geocode.csv``), ``NULL`` when the API found no
      subdivision.
    * ``place_location`` — ``"<river_name> - <country>"``
      (``Amazon - Brazil``); a blank part is dropped, both blank is
      ``NULL``.
    * ``metric_name`` — always ``discharge``; ``metric_value`` — the
      daily mean ``discharge_m3s``, counting a missing value as ``0``
      (the source skips null-discharge days, so none occur);
      ``metric_unit`` — always ``m³/s``.
    * ``event_start_date`` / ``event_start`` — ``date`` at midnight /
      ``date`` itself: the GMT+0 day of the GloFAS 24-h mean (always
      present); ``event_end`` — ``NULL`` (a daily value has no end).
    * ``danger_rating`` / ``statement`` / ``other_metrics`` /
      ``last_latitude`` / ``last_longitude`` — ``NULL`` (no alert level,
      no advisory prose, a single measurement, a fixed reference point).
    * ``file_date`` — the snapshot day the value was downloaded for.
    """
