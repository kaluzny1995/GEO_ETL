"""``geo.usgs_nwis_etl`` — USGS stream gauge readings in the canonical structure."""

from model.etl.event_etl import EventEtl


class UsgsNwisEtl(EventEtl):
    """One row per gauge per measurement day per snapshot, from ``geo.usgs_nwis``.

    Mapping (see ``single_etl/usgs_nwis_etl.py``):

    * ``source_name`` — always ``USGS_NWIS``; ``subsource_name`` — ``site_id``
      verbatim (``USGS-01646500``).
    * ``event_name`` — ``"Streamflow - <river name>"``: the ``site_name``
      up to the first ``NEAR``/``AT`` marker, lower-cased with only its
      first letter upper-cased (``Streamflow - Potomac river``);
      ``"Streamflow"`` alone when no usable name exists.
    * ``event_category`` — always ``flood`` (the same category the
      OpenMeteo discharge snapshots use); ``event_subcategory`` — ``NULL``.
    * ``event_description`` — the ``qualifiers`` expanded to their full
      names (``P`` → ``provisional``, ``A,e`` → ``approved for
      publication, estimated``); an unknown code is kept verbatim, a
      blank qualifier is ``NULL``.
    * ``country_code`` — always ``US`` (the network publishes US gauges);
      ``state_province`` — the stamped ``geocode_state`` of the gauge
      point, ``NULL`` when the stamp found no subdivision.
    * ``place_location`` — ``"<river name>, <place text>"``, both parts
      extracted from ``site_name`` around its first ``NEAR``/``AT``
      marker (``Potomac river, WASH, DC LITTLE FALLS PUMP STA``); without
      a marker only the sentence-cased name remains.
    * ``metric_name`` — always ``discharge``; ``metric_value`` — ``value``
      (the daily mean for parameter ``00060``), counting a missing reading
      as ``0``; ``metric_unit`` — ``unit`` verbatim (``ft3/s``, or
      ``m³/s`` if a metric parameter is configured).
    * ``event_start`` — ``date`` at midnight (the
      source reports a day, not a datetime); ``event_end`` — ``NULL`` (a
      daily mean has no end).
    * ``danger_rating`` — the NWS flood category of the day's peak stage
      mapped onto the shared scale (``no_flooding`` → ``none``, ``action`` →
      ``low``, ``minor`` → ``medium``, ``moderate`` → ``high``, ``major`` →
      ``extreme``); ``no_rating`` for an empty or unknown category — an
      uncovered gauge and a day without a stage.
    * ``other_metrics`` — ``gauge_height`` (the day's peak) beside the
      gauge's NWS ``action``/``minor``/``moderate``/``major`` stage
      thresholds and its ``nws_gauge_id``, from the flood-category
      reference joined on ``site_id``; every measurement carries its own
      ``<name>_unit`` companion key (``ft``) instead of the unit in the
      key name, ``NULL`` when neither side has an answer.
    * ``last_latitude`` / ``last_longitude`` — ``NULL`` (the gauge is
      fixed).
    * ``file_date`` — the snapshot day the reading was downloaded for.
    """
