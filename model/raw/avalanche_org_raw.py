from pydantic import BaseModel


class AvalancheOrgRaw(BaseModel):
    """Raw Avalanche forecast data — all fields as TEXT."""

    zone_id: str
    zone_name: str
    center_id: str
    center_name: str
    state: str
    danger_level: str
    danger_rating: str
    travel_advice: str
    off_season: str
    forecast_start: str | None = None
    forecast_end: str | None = None
    warning: str | None = None
    link: str
    date: str
    file_date: str
