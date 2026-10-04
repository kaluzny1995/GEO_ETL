import datetime as dt

from pydantic import BaseModel, ConfigDict


class NifcFire(BaseModel):
    """Typed NIFC wildfire data (float, str, datetime)."""

    model_config = ConfigDict(json_schema_extra={})

    incident_name: str | None = None
    irwin_id: str | None = None
    fire_number: str | None = None
    acres: float | None = None
    percent_contained: float | None = None
    fire_type: str | None = None
    fire_type_label: str | None = None
    jurisdiction: str | None = None
    jurisdiction_label: str | None = None
    unit_name: str | None = None
    state: str | None = None
    po_method: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    poly_date: dt.datetime | None = None
    created_on: dt.datetime | None = None
    modified_on: dt.datetime | None = None
    geocode_state: str | None = None
    file_date: str
