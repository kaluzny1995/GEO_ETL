import datetime as dt

from pydantic import BaseModel


class EonetEvent(BaseModel):
    """Typed NASA EONET event data (float, int, datetime, date)."""

    event_id: str
    event_name: str
    category_id: str
    category_name: str
    description: str | None = None
    source_id: str
    source_url: str | None = None
    latitude: float
    longitude: float
    event_date: dt.datetime
    magnitude_value: float | None = None
    magnitude_unit: str | None = None
    closed: dt.date | None = None
    geometry_count: int
    last_latitude: float
    last_longitude: float
    last_date: dt.datetime
    file_date: str
