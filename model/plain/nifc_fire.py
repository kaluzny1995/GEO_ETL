import datetime as dt

from pydantic import BaseModel, ConfigDict
from typing import Optional


class NifcFire(BaseModel):
    """Typed NIFC wildfire data (float, str, datetime)."""
    model_config = ConfigDict(json_schema_extra={})

    incident_name: Optional[str] = None
    irwin_id: Optional[str] = None
    fire_number: Optional[str] = None
    acres: Optional[float] = None
    percent_contained: Optional[float] = None
    fire_type: Optional[str] = None
    fire_type_label: Optional[str] = None
    jurisdiction: Optional[str] = None
    jurisdiction_label: Optional[str] = None
    state: Optional[str] = None
    po_method: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    poly_date: Optional[dt.datetime] = None
    created_on: Optional[dt.datetime] = None
    modified_on: Optional[dt.datetime] = None
    file_date: str
