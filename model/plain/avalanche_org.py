import datetime as dt

from pydantic import BaseModel


class AvalancheOrg(BaseModel):
    """Typed Avalanche forecast data (int, bool, datetime, date)."""

    zone_id: int
    zone_name: str
    center_id: str
    center_name: str
    state: str
    danger_level: int
    danger_rating: str
    travel_advice: str
    off_season: bool
    forecast_start: dt.datetime | None = None
    forecast_end: dt.datetime | None = None
    warning: str | None = None
    link: str
    latitude: float | None = None
    longitude: float | None = None
    date: dt.date
    file_date: str
