import datetime as dt

from pydantic import BaseModel, ConfigDict


class IbtacsCyclone(BaseModel):
    """Typed IBTrACS cyclone track data (float, int, datetime)."""

    model_config = ConfigDict(json_schema_extra={})

    sid: str
    season: int
    number: int
    basin: str
    subbasin: str | None = None
    name: str | None = None
    iso_time: dt.datetime
    nature: str | None = None
    lat: float
    lon: float
    wmo_wind: int | None = None
    wmo_pres: int | None = None
    wmo_agency: str | None = None
    track_type: str | None = None
    dist2land: int | None = None
    landfall: int | None = None
    iflag: str | None = None
    usa_agency: str | None = None
    usa_atcf_id: str | None = None
    usa_lat: float | None = None
    usa_lon: float | None = None
    usa_record: str | None = None
    usa_status: str | None = None
    usa_wind: int | None = None
    usa_pres: int | None = None
    usa_sshs: int | None = None
    usa_r34_ne: int | None = None
    usa_r34_se: int | None = None
    usa_r34_sw: int | None = None
    usa_r34_nw: int | None = None
    usa_r50_ne: int | None = None
    usa_r50_se: int | None = None
    usa_r50_sw: int | None = None
    usa_r50_nw: int | None = None
    usa_r64_ne: int | None = None
    usa_r64_se: int | None = None
    usa_r64_sw: int | None = None
    usa_r64_nw: int | None = None
    usa_poci: int | None = None
    usa_roci: int | None = None
    usa_rmw: int | None = None
    usa_eye: int | None = None
    storm_speed: int | None = None
    storm_dir: int | None = None
    usa_gust: int | None = None
    geocode_country: str | None = None
    geocode_state: str | None = None
    file_date: str
