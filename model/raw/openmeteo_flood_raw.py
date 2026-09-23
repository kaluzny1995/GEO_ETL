from pydantic import BaseModel
from typing import Optional


class OpenMeteoFloodRaw(BaseModel):
    river_name: Optional[str] = None
    country: Optional[str] = None
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    date: Optional[str] = None
    discharge_m3s: Optional[str] = None
    file_date: str
