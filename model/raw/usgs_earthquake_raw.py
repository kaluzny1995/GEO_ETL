from pydantic import BaseModel


class UsgsEarthquakeRaw(BaseModel):
    """Raw USGS earthquake event data — all fields as TEXT."""

    time: str | None = None
    latitude: str | None = None
    longitude: str | None = None
    depth: str | None = None
    mag: str | None = None
    magType: str | None = None
    nst: str | None = None
    gap: str | None = None
    dmin: str | None = None
    rms: str | None = None
    net: str | None = None
    id: str | None = None
    updated: str | None = None
    place: str | None = None
    type: str | None = None
    horizontalError: str | None = None
    depthError: str | None = None
    magError: str | None = None
    magNst: str | None = None
    status: str | None = None
    locationSource: str | None = None
    magSource: str | None = None
    file_date: str
