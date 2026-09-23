from pydantic import BaseModel
from typing import Optional


class UsgsEarthquakeRaw(BaseModel):
    time: Optional[str] = None
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    depth: Optional[str] = None
    mag: Optional[str] = None
    magType: Optional[str] = None
    nst: Optional[str] = None
    gap: Optional[str] = None
    dmin: Optional[str] = None
    rms: Optional[str] = None
    net: Optional[str] = None
    id: Optional[str] = None
    updated: Optional[str] = None
    place: Optional[str] = None
    type: Optional[str] = None
    horizontalError: Optional[str] = None
    depthError: Optional[str] = None
    magError: Optional[str] = None
    magNst: Optional[str] = None
    status: Optional[str] = None
    locationSource: Optional[str] = None
    magSource: Optional[str] = None
    file_date: str
