from pydantic import BaseModel
from typing import Optional


class UsgsNwisRaw(BaseModel):
    """Raw USGS NWIS stream gauge data — all fields as TEXT."""
    site_id: Optional[str] = None
    site_name: Optional[str] = None
    parameter_code: Optional[str] = None
    date: Optional[str] = None
    value: Optional[str] = None
    unit: Optional[str] = None
    qualifiers: Optional[str] = None
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    file_date: str
