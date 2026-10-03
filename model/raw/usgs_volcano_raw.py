from pydantic import BaseModel


class UsgsVolcanoRaw(BaseModel):
    """Raw USGS volcanic activity data — all fields as TEXT."""

    volcano_name: str | None = None
    vnum: str | None = None
    observatory: str | None = None
    observatory_name: str | None = None
    alert_level: str | None = None
    color_code: str | None = None
    notice_type: str | None = None
    notice_identifier: str | None = None
    notice_date: str | None = None
    notice_url: str | None = None
    notice_data_url: str | None = None
    notice_type_name: str | None = None
    latitude: str | None = None
    longitude: str | None = None
    region: str | None = None
    synopsis: str | None = None
    file_date: str
