import datetime as dt

from pydantic import BaseModel, ConfigDict
from typing import Optional


class UsgsVolcano(BaseModel):
    model_config = ConfigDict(json_schema_extra={})

    volcano_name: Optional[str] = None
    vnum: Optional[int] = None
    observatory: Optional[str] = None
    observatory_name: Optional[str] = None
    alert_level: Optional[str] = None
    color_code: Optional[str] = None
    notice_type: Optional[str] = None
    notice_identifier: Optional[str] = None
    notice_date: Optional[dt.datetime] = None
    notice_url: Optional[str] = None
    notice_data_url: Optional[str] = None
    file_date: str
