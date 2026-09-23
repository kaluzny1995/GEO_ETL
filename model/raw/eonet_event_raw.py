from pydantic import BaseModel


class EonetEventRaw(BaseModel):
    event_id: str
    event_name: str
    category_id: str
    category_name: str
    description: str | None = None
    source_id: str
    source_url: str | None = None
    latitude: str
    longitude: str
    event_date: str
    magnitude_value: str | None = None
    magnitude_unit: str | None = None
    closed: str | None = None
    geometry_count: str
    last_latitude: str
    last_longitude: str
    last_date: str
    file_date: str
