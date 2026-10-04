import datetime as dt

from pydantic import BaseModel, ConfigDict


class UsgsNwis(BaseModel):
    """Typed USGS NWIS stream gauge data (date, float, double)."""

    model_config = ConfigDict(json_schema_extra={})

    site_id: str | None = None
    site_name: str | None = None
    parameter_code: str | None = None
    date: dt.date | None = None
    value: float | None = None
    unit: str | None = None
    qualifiers: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    geocode_state: str | None = None
    file_date: str
