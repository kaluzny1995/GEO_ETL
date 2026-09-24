import datetime as dt

from pydantic import BaseModel, ConfigDict
from typing import Optional


class OpenMeteoFlood(BaseModel):
    """Typed OpenMeteo flood data (float, date)."""
    model_config = ConfigDict(json_schema_extra={})

    river_name: Optional[str] = None
    country: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    date: Optional[dt.date] = None
    discharge_m3s: Optional[float] = None
    file_date: str
