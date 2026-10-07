from pydantic import BaseModel


class UsgsNwisRaw(BaseModel):
    """Raw USGS NWIS stream gauge data — all fields as TEXT."""

    site_id: str | None = None
    site_name: str | None = None
    parameter_code: str | None = None
    date: str | None = None
    value: str | None = None
    unit: str | None = None
    qualifiers: str | None = None
    gauge_height: str | None = None
    flood_category: str | None = None
    latitude: str | None = None
    longitude: str | None = None
    geocode_state: str | None = None
    file_date: str
