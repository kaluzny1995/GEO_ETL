from pydantic import BaseModel


class OpenMeteoFloodRaw(BaseModel):
    """Raw OpenMeteo flood monitoring data — all fields as TEXT."""

    river_name: str | None = None
    country: str | None = None
    latitude: str | None = None
    longitude: str | None = None
    date: str | None = None
    discharge_m3s: str | None = None
    geocode_country: str | None = None
    geocode_state: str | None = None
    file_date: str
