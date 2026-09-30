"""Canonical event structure shared by all ``*_etl`` tables.

Field conventions (identical for every source, so the tables can be merged):

* ``source_name`` — producing system in upper case (``AVALANCHE_ORG``, ``EONET``, ``GDACS``, …).
* ``subsource_name`` — the subsystem that emitted the event (issuing center, agency, basin).
* ``event_name`` — name of the event itself; ``NULL`` for sources that only describe a
  place or a standing state of affairs (a forecast zone has no event name).
* ``event_category`` / ``event_subcategory`` — catastrophe class and its subtype
  (e.g. ``cyclone`` / ``hurricane``); ``event_subcategory`` stays ``NULL`` when the
  source has no subtype.
* ``country_code`` — ISO 3166-1 alpha-2 (2 letters); a multi-country event is exploded
  into one row per country. ``state_province`` — ISO 3166-2 subdivision code (2 letters).
* ``place_location`` — the named place the row is about (forecast zone, city, basin),
  free text.
* ``metric_name`` — the single most important measurement of the source (``magnitude``,
  …); ``metric_value`` / ``metric_unit`` stay ``NULL`` until the source actually
  reports a value.
* ``danger_rating`` — the same measurement expressed as a severity label;
  ``NULL`` when the source has no rating.
* ``statement`` — advisory prose: travel advice, warning text, notice.
* ``other_metrics`` — remaining metrics as a JSON object string, e.g.
  ``{"affected_area": "120 km2", "deaths": "3 persons"}``.
* ``event_start_date`` — date-only start (``YYYY-MM-DD``), when the source partitions
  by calendar day; ``event_start`` / ``event_end`` — when the event (or forecast)
  started and ended as timestamps, ``event_end`` optional for open-ended events.
* ``latitude``/``longitude`` — current position; ``last_latitude``/``last_longitude``
  — the most recent one when the event moves (track sources).
* ``file_date`` — ``YYYY-MM-DD`` partition of the run, used for incremental loads.
"""

import datetime as dt

from pydantic import BaseModel


class EventEtl(BaseModel):
    """One event row in the canonical structure shared by all ``*_etl`` tables."""

    source_name: str
    subsource_name: str | None = None
    event_name: str | None = None
    event_category: str
    event_subcategory: str | None = None
    event_description: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    country_code: str
    state_province: str | None = None
    place_location: str | None = None
    metric_name: str
    metric_value: float | None = None
    metric_unit: str | None = None
    danger_rating: str | None = None
    statement: str | None = None
    other_metrics: str | None = None
    event_start_date: dt.date | None = None
    event_start: dt.datetime | None = None
    event_end: dt.datetime | None = None
    last_latitude: float | None = None
    last_longitude: float | None = None
    file_date: str
