from pydantic import BaseModel
from typing import Optional


class NifcFireRaw(BaseModel):
    """Raw NIFC wildfire incident data — all fields as TEXT."""
    incident_name: Optional[str] = None
    irwin_id: Optional[str] = None
    fire_number: Optional[str] = None
    acres: Optional[str] = None
    percent_contained: Optional[str] = None
    fire_type: Optional[str] = None
    fire_type_label: Optional[str] = None
    jurisdiction: Optional[str] = None
    jurisdiction_label: Optional[str] = None
    state: Optional[str] = None
    po_method: Optional[str] = None
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    poly_date: Optional[str] = None
    created_on: Optional[str] = None
    modified_on: Optional[str] = None
    file_date: str
