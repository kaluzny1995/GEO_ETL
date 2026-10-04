import datetime as dt

from pydantic import BaseModel, ConfigDict


class UsgsEarthquake(BaseModel):
    """Typed USGS earthquake data (float, int, datetime)."""

    model_config = ConfigDict(json_schema_extra={})

    time: dt.datetime | None = None
    latitude: float | None = None
    longitude: float | None = None
    depth: float | None = None
    mag: float | None = None
    mag_type: str | None = None
    nst: int | None = None
    gap: int | None = None
    dmin: float | None = None
    rms: float | None = None
    net: str | None = None
    event_id: str | None = None
    updated: dt.datetime | None = None
    place: str | None = None
    event_type: str | None = None
    horizontal_error: float | None = None
    depth_error: float | None = None
    mag_error: float | None = None
    mag_nst: int | None = None
    status: str | None = None
    location_source: str | None = None
    mag_source: str | None = None
    geocode_country: str | None = None
    geocode_state: str | None = None
    file_date: str
