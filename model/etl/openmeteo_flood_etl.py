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
      download-time stamped ``geocode_country``, an empty stamp counts as a
      miss and resolves to ``ZZ``.
    * ``state_province`` — the stamped ``geocode_state`` of the reference
      point, ``NULL`` when the stamp found no subdivision.
    * ``place_location`` — ``"<river_name> - <country>"``
      (``Amazon - Brazil``); a blank part is dropped, both blank is
      ``NULL``.
    * ``metric_name`` — always ``discharge``; ``metric_value`` — the
      daily mean ``discharge_m3s``, counting a missing value as ``0``
      (the source skips null-discharge days, so none occur);
      ``metric_unit`` — always ``m³/s``.
    * ``event_start`` — ``date`` itself at midnight: the GMT+0 day of the
      GloFAS 24-h mean (always present); ``event_end`` — ``NULL`` (a daily
      value has no end).
    * ``danger_rating`` — the highest reference level the discharge
      exceeded (``Q99``, then ``Q95`` down to the level
      ``threshold_percentile`` names in ``config.json``), ``NULL`` at or
      below it: the flood rating of the day against the river's own
      baseline, and a strict one — a flat baseline never rates a day.
    * ``other_metrics`` — the threshold reference the script joins on
      ``river_name`` + ``country`` (``discharge_p50_m3s`` …
      ``discharge_p99_m3s``, ``threshold_percentile`` /
      ``threshold_m3s`` / ``threshold_unit`` / ``exceedance_ratio`` /
      ``threshold_exceeded`` and the ``baseline_days`` /
      ``baseline_start`` / ``baseline_end`` provenance) plus the GloFAS
      ensemble statistics of the day (``ensemble_mean_m3s``,
      ``ensemble_median_m3s``, ``ensemble_p25_m3s``,
      ``ensemble_p75_m3s`` — forecast days only); every key the row cannot
      answer is omitted, and a row with nothing to report gets ``NULL``.
      Without the reference file both stay ``NULL``.
    * ``last_latitude`` / ``last_longitude`` — ``NULL`` (a fixed reference
      point).
    * ``file_date`` — the snapshot day the value was downloaded for.
    """
