"""``geo.ibtracs_cyclone_etl`` — IBTrACS cyclone track points in the canonical structure."""

from model.etl.event_etl import EventEtl


class IbtracsCycloneEtl(EventEtl):
    """One row per IBTrACS best-track position, transformed from ``geo.ibtracs_cyclone``.

    Mapping (see ``single_etl/ibtracs_cyclone_etl.py``):

    * ``source_name`` — always ``IBTRACS``; ``subsource_name`` — the storm's
      basin code (``NA``, ``EP``, ``WP``, ``NI``, …), ``NULL`` when blank.
    * ``event_name`` — the storm ``name`` (``UNNAMED`` for unnamed systems),
      ``NULL`` when blank.
    * ``event_category`` — always ``storm``; ``event_subcategory`` — the
      ATCF system status of the position (``usa_status``: ``hurricane``,
      ``typhoon``, ``tropical storm``, ``tropical depression``,
      ``disturbance``, ``low``, …), falling back to the cross-agency
      ``nature`` (``tropical``, ``disturbance``, ``extratropical``,
      ``subtropical``, ``contradictory``) when no US agency reported a
      status, ``NULL`` when neither states a type (``nature = NR`` in
      non-US basins).
    * ``place_location`` — the IBTrACS region name: the basin's full name
      extended with the sub-basin (``North Atlantic - Gulf of Mexico``,
      ``Eastern North Pacific - Central Pacific``, …); just the basin name
      for the unnamed sub-basins.
    * ``country_code`` / ``state_province`` — the reverse-geocoded position
      of every point within 100 km of land (``cache/reverse_geocode.csv``),
      else ``ZZ`` / ``NULL`` (open water); the source carries no country.
    * ``metric_name`` — ``wind speed``; ``metric_value`` —
      ``coalesce(wmo_wind, usa_wind)`` (the WMO agency value when present,
      the US one otherwise — WMO values are absent in real-time data);
      ``metric_unit`` — ``kts``.
    * ``danger_rating`` — the Saffir-Simpson label of ``usa_sshs``
      (``category 4``, ``tropical storm``, ``tropical depression``,
      ``disturbance``).
    * ``other_metrics`` — JSON object pairing every measurement with its
      unit companion key: ``pressure`` / ``pressure_unit`` (``usa_pres``,
      mb), ``speed`` / ``speed_unit`` (``storm_speed``, kts), ``direction``
      / ``direction_unit`` (``storm_dir``, degrees), ``gust`` /
      ``gust_unit`` (``usa_gust``, kts), ``dist2land`` /
      ``dist2land_unit`` and ``landfall`` / ``landfall_unit`` (km); a
      measurement the point does not report is omitted together with its
      unit, ``NULL`` when it reports none.
    * ``statement`` / ``event_description`` — ``NULL`` (a best track carries
      no advisory prose).
    * ``event_start_date`` / ``event_start`` / ``event_end`` — the position's
      ``iso_time`` (one row = one track point).
    * ``last_latitude`` / ``last_longitude`` — the storm's latest position
      (maximum ``iso_time`` per ``sid``; the newest points always arrive in
      the newest ``file_date``, so incremental loads stay correct).
    """
