from pydantic import BaseModel


class GdacsEventRaw(BaseModel):
    """Raw GDACS disaster event data — all fields as TEXT."""
    event_type: str
    event_id: str
    event_name: str
    alert_level: str
    country: str | None = None
    country_code: str | None = None
    latitude: str
    longitude: str
    from_date: str
    to_date: str
    date_modified: str
    severity: str
    severity_text: str | None = None
    severity_unit: str | None = None
    source: str
    report_url: str
    is_current: str
    file_date: str
