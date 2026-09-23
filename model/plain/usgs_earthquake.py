import datetime as dt

from pydantic import BaseModel, ConfigDict
from typing import Optional


class UsgsEarthquake(BaseModel):
    model_config = ConfigDict(json_schema_extra={})

    time: Optional[dt.datetime] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    depth: Optional[float] = None
    mag: Optional[float] = None
    mag_type: Optional[str] = None
    nst: Optional[int] = None
    gap: Optional[int] = None
    dmin: Optional[float] = None
    rms: Optional[float] = None
    net: Optional[str] = None
    event_id: Optional[str] = None
    updated: Optional[dt.datetime] = None
    place: Optional[str] = None
    event_type: Optional[str] = None
    horizontal_error: Optional[float] = None
    depth_error: Optional[float] = None
    mag_error: Optional[float] = None
    mag_nst: Optional[int] = None
    status: Optional[str] = None
    location_source: Optional[str] = None
    mag_source: Optional[str] = None
    file_date: str
