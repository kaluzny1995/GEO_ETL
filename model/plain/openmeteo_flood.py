import datetime as dt

from pydantic import BaseModel, ConfigDict


class OpenMeteoFlood(BaseModel):
    """Typed OpenMeteo flood data (float, date)."""

    model_config = ConfigDict(json_schema_extra={})

    river_name: str | None = None
    country: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    date: dt.date | None = None
    discharge_m3s: float | None = None
    file_date: str
