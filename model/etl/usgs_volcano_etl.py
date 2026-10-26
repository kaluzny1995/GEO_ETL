"""``geo.usgs_volcano_etl`` — USGS volcanic activity notices in the canonical structure."""

from model.etl.event_etl import EventEtl


class UsgsVolcanoEtl(EventEtl):
    """One row per volcano per notice per snapshot, from ``geo.usgs_volcano``.

    Mapping (see ``single_etl/usgs_volcano_etl.py``):

    * ``source_name`` — always ``USGS_VOLCANO``; ``subsource_name`` — the source
      ``observatory_name`` verbatim (``Alaska Volcano Observatory``,
      ``Hawaiian Volcano Observatory``, ``Northern Mariana Islands``).
    * ``event_name`` — ``"Volcano - <volcano_name>"``
      (``Volcano - Great Sitkin``); ``"Volcano"`` alone when the name is
      blank.
    * ``event_category`` — always ``volcano``; ``event_subcategory`` — the
      same word ``volcano``: a notice is a volcano report, not a subtype of
      one, so the notice type (``Daily Update``, ``Weekly Update``,
      ``Volcano Activity Notification``) stays in the plain table.
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
      largest significant-eruption VEI (Volcanic Explosivity Index) as a
      number (Kīlauea → ``4``), read from the plain table's NCEI/WDS
      eruption record, ``0`` for a volcano NCEI does not list (e.g. Great
      Sitkin); ``metric_unit`` — always ``VEI``.
    * ``danger_rating`` — the notice's alert level and aviation color code
      mapped onto the shared scale: ``no_rating`` (``NORMAL`` / ``GREEN``,
      or a notice carrying neither a known alert level nor a known color
      code), ``low`` (``ADVISORY`` / ``YELLOW``), ``high`` (``WATCH`` /
      ``ORANGE``), ``extreme`` (``WARNING`` / ``RED``) — the harsher of the
      two answers wins.
    * ``event_start`` — ``notice_date`` (when the notice was issued, full
      precision); ``event_end`` — ``NULL`` (a notice is an instant).
    * ``other_metrics`` — ``{"color_code": …, "alert_level": …, "vnum":
      …, "vnum_unit": "Smithsonian GVP", "notice_identifier": …,
      "casualties": 5411, "casualties_unit": "people", "damage": 385.5,
      "damage_unit": "million USD"}``: the Aviation Color Code and the
      USGS alert level — the feed's raw words, kept next to the harmonized
      ``danger_rating`` they are mapped from, the Smithsonian GVP volcano number with its
      ``<name>_unit`` companion, the unique notice id and the NCEI/WDS
      eruption totals — total deaths and total damage in millions of USD —
      each with its ``<name>_unit`` companion; a value the source does not
      report is omitted together with its unit. ``last_latitude`` /
      ``last_longitude`` — ``NULL`` (the volcano is fixed).
    * ``file_date`` — the snapshot day the notice was downloaded for.
    """
