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
    * ``metric_name`` — always ``vei``; ``metric_value`` — the volcano's
      largest significant-eruption VEI as a number (Kīlauea → ``4``), read
      from the plain table's NCEI/WDS eruption record; ``NULL`` for a
      volcano NCEI does not list (e.g. Great Sitkin); ``metric_unit`` —
      ``VEI`` alongside a value, ``NULL`` without one; ``danger_rating`` —
      the same VEI as a readable class (``VEI 4``), ``NULL`` when NCEI has
      no record.
    * ``event_start`` — ``notice_date`` (when the notice was issued, full
      precision); ``event_end`` — ``NULL`` (a notice is an instant).
    * ``other_metrics`` — ``{"color_code": …, "alert_level": …, "vnum":
      …, "vnum_unit": "Smithsonian GVP", "notice_identifier": …,
      "casualties": 5411, "casualties_unit": "people", "damage": 385.5,
      "damage_unit": "million USD"}``: the Aviation Color Code and the
      USGS alert level (both live here now that ``danger_rating`` carries
      the VEI), the Smithsonian GVP volcano number with its
      ``<name>_unit`` companion, the unique notice id and the NCEI/WDS
      eruption totals — total deaths and total damage in millions of USD —
      each with its ``<name>_unit`` companion; a value the source does not
      report is omitted together with its unit. ``last_latitude`` /
      ``last_longitude`` — ``NULL`` (the volcano is fixed).
    * ``file_date`` — the snapshot day the notice was downloaded for.
    """
