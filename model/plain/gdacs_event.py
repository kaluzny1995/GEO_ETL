import datetime as dt

from pydantic import BaseModel


class GdacsEvent(BaseModel):
    event_type: str
    event_id: int
    event_name: str
    alert_level: str
    country: str | None = None
    country_code: str | None = None
    latitude: float
    longitude: float
    from_date: dt.datetime
    to_date: dt.datetime
    date_modified: dt.datetime
    severity: float
    severity_text: str | None = None
    severity_unit: str | None = None
    source: str
    report_url: str
    is_current: bool
    file_date: str
