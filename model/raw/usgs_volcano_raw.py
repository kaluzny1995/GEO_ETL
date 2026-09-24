from pydantic import BaseModel
from typing import Optional


class UsgsVolcanoRaw(BaseModel):
    """Raw USGS volcanic activity data — all fields as TEXT."""
    volcano_name: Optional[str] = None
    vnum: Optional[str] = None
    observatory: Optional[str] = None
    observatory_name: Optional[str] = None
    alert_level: Optional[str] = None
    color_code: Optional[str] = None
    notice_type: Optional[str] = None
    notice_identifier: Optional[str] = None
    notice_date: Optional[str] = None
    notice_url: Optional[str] = None
    notice_data_url: Optional[str] = None
    file_date: str
