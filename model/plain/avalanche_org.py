import datetime as dt

from pydantic import BaseModel


class AvalancheOrg(BaseModel):
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
    date: dt.date
    file_date: str
