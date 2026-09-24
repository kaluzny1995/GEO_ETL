import datetime as dt

from pydantic import BaseModel, ConfigDict
from typing import Optional


class UsgsNwis(BaseModel):
    """Typed USGS NWIS stream gauge data (date, float, double)."""
    model_config = ConfigDict(json_schema_extra={})

    site_id: Optional[str] = None
    site_name: Optional[str] = None
    parameter_code: Optional[str] = None
    date: Optional[dt.date] = None
    value: Optional[float] = None
    unit: Optional[str] = None
    qualifiers: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    file_date: str
