"""``geo.eonet_event_etl`` — NASA EONET events in the canonical structure."""

from model.etl.event_etl import EventEtl


class EonetEventEtl(EventEtl):
    """One row per EONET event, transformed from ``geo.eonet_event``.

    Mapping (see ``single_etl/eonet_event_etl.py``):

    * ``source_name`` — always ``EONET``; ``subsource_name`` — the reporting feed
      (``EO``, ``JTWC``, ``IRWIN``).
    * ``event_category`` — ``storm`` / ``fire`` / ``iceberg`` from ``category_id``
      (``severeStorms`` / ``wildfires`` / ``seaLakeIce``), snake_cased for any
      other category; ``event_subcategory`` — the storm type parsed from
      ``event_name`` (``hurricane``, ``typhoon``, ``tropical storm``,
      ``tropical cyclone``, ``cyclone``), ``wildfire`` for fires,
      ``sea_lake_ice`` for ice, ``NULL`` when neither is known.
    * ``place_location`` — ``event_name`` with its type prefix stripped
      (``Wildfire``, ``Prescribed Fire``, ``Incident Complex``,
      ``Emergency Stabilization BAER``); ``NULL`` when the remainder is not a
      place (``Hurricane Polo``, ``Iceberg D32``).
    * ``country_code`` / ``state_province`` — the US state at the end of
      ``description`` (fires: ``… from Miles City, MT``), else the
      reverse-geocoded coordinate pair (``cache/reverse_geocode.csv``), else
      ``ZZ`` / ``NULL`` for open water.
    * ``metric_name`` — ``wind speed`` (storms), ``area`` (fires), ``surface``
      (ice), ``magnitude`` for any other category; ``metric_value`` /
      ``metric_unit`` — ``magnitude_value`` / ``magnitude_unit``.
    * ``other_metrics`` — ``{"geometry_count": n}`` (the event's geometry points).
    * ``danger_rating`` — ``NULL`` (EONET carries no rating).
    * ``event_start`` — ``event_date``; ``event_end`` —
      ``last_date``; ``last_latitude`` / ``last_longitude`` — the latest position.
    """
