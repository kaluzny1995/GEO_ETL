"""``geo.usgs_volcano_etl`` — USGS volcanic activity notices in the canonical structure."""

from model.etl.event_etl import EventEtl


class UsgsVolcanoEtl(EventEtl):
    """One row per volcano per notice per snapshot, from ``geo.usgs_volcano``.

    Mapping (see ``single_etl/usgs_volcano_etl.py``):

    * ``source_name`` — always ``USGS``; ``subsource_name`` — the source
      ``observatory_name`` verbatim (``Alaska Volcano Observatory``,
      ``Hawaiian Volcano Observatory``, ``Northern Mariana Islands``).
    * ``event_name`` — ``"Volcano - <volcano_name>"``
      (``Volcano - Great Sitkin``); ``"Volcano"`` alone when the name is
      blank.
    * ``event_category`` — always ``volcano``; ``event_subcategory`` — the
      notice type's full name the downloader stamped as ``notice_type_name``
      (``Daily Update``, ``Weekly Update``, ``Volcano Activity
      Notification``), falling back to the feed's own code when the name
      is missing.
    * ``event_description`` — the stamped ``synopsis`` status line
      (``AVO Great Sitkin ORANGE/WATCH - Slow eruption of lava …``), ``NULL``
      when blank.
    * ``latitude`` / ``longitude`` — the volcano's coordinates the
      downloader stamped into the CSV (the HANS feed itself carries no
      location).
    * ``country_code`` — always ``US`` (the three observatories are US:
      Alaska, Hawaii, Northern Mariana Islands); ``state_province`` — the
      stamped ``geocode_state`` of the notice point, ``ZZ`` when the stamp
      found no subdivision (the Ahyi Seamount is an ocean point).
    * ``place_location`` — ``"<volcano_name>, <region>"``
      (``Great Sitkin, Aleutians``); without a region only the name
      remains, a blank name is ``NULL``.
    * ``metric_name`` — always ``alert_level``; ``metric_value`` — the USGS
      alert scale as a number (``NORMAL`` 0, ``ADVISORY`` 1, ``WATCH`` 2,
      ``WARNING`` 3); an unknown code stays ``NULL`` (an unknown level is
      not ``NORMAL``); ``metric_unit`` — ``NULL`` (a scale index has no
      unit); ``danger_rating`` — the source ``alert_level`` text verbatim.
    * ``event_start`` — ``notice_date`` (when the notice was issued, full
      precision); ``event_end`` — ``NULL`` (a notice is an instant).
    * ``other_metrics`` / ``last_latitude`` / ``last_longitude`` — ``NULL``
      (no extra metrics; the volcano is fixed).
    * ``file_date`` — the snapshot day the notice was downloaded for.
    """
