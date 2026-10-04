from pydantic import BaseModel


class NifcFireRaw(BaseModel):
    """Raw NIFC wildfire incident data — all fields as TEXT."""

    incident_name: str | None = None
    irwin_id: str | None = None
    fire_number: str | None = None
    acres: str | None = None
    percent_contained: str | None = None
    fire_type: str | None = None
    fire_type_label: str | None = None
    jurisdiction: str | None = None
    jurisdiction_label: str | None = None
    unit_name: str | None = None
    state: str | None = None
    po_method: str | None = None
    latitude: str | None = None
    longitude: str | None = None
    poly_date: str | None = None
    created_on: str | None = None
    modified_on: str | None = None
    geocode_state: str | None = None
    file_date: str
