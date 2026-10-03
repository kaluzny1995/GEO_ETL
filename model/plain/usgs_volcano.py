import datetime as dt

from pydantic import BaseModel, ConfigDict


class UsgsVolcano(BaseModel):
    """Typed USGS volcanic activity data (int, timestamp)."""

    model_config = ConfigDict(json_schema_extra={})

    volcano_name: str | None = None
    vnum: int | None = None
    observatory: str | None = None
    observatory_name: str | None = None
    alert_level: str | None = None
    color_code: str | None = None
    notice_type: str | None = None
    notice_identifier: str | None = None
    notice_date: dt.datetime | None = None
    notice_url: str | None = None
    notice_data_url: str | None = None
    notice_type_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    region: str | None = None
    synopsis: str | None = None
    file_date: str
