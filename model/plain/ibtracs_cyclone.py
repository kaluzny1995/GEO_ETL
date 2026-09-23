import datetime as dt

from pydantic import BaseModel, ConfigDict
from typing import Optional


class IbtacsCyclone(BaseModel):
    model_config = ConfigDict(json_schema_extra={})

    sid: str
    season: int
    number: int
    basin: str
    subbasin: Optional[str] = None
    name: Optional[str] = None
    iso_time: dt.datetime
    nature: Optional[str] = None
    lat: float
    lon: float
    wmo_wind: Optional[int] = None
    wmo_pres: Optional[int] = None
    wmo_agency: Optional[str] = None
    track_type: Optional[str] = None
    dist2land: Optional[int] = None
    landfall: Optional[int] = None
    iflag: Optional[str] = None
    usa_agency: Optional[str] = None
    usa_atcf_id: Optional[str] = None
    usa_lat: Optional[float] = None
    usa_lon: Optional[float] = None
    usa_record: Optional[str] = None
    usa_status: Optional[str] = None
    usa_wind: Optional[int] = None
    usa_pres: Optional[int] = None
    usa_sshs: Optional[int] = None
    usa_r34_ne: Optional[int] = None
    usa_r34_se: Optional[int] = None
    usa_r34_sw: Optional[int] = None
    usa_r34_nw: Optional[int] = None
    usa_r50_ne: Optional[int] = None
    usa_r50_se: Optional[int] = None
    usa_r50_sw: Optional[int] = None
    usa_r50_nw: Optional[int] = None
    usa_r64_ne: Optional[int] = None
    usa_r64_se: Optional[int] = None
    usa_r64_sw: Optional[int] = None
    usa_r64_nw: Optional[int] = None
    usa_poci: Optional[int] = None
    usa_roci: Optional[int] = None
    usa_rmw: Optional[int] = None
    usa_eye: Optional[int] = None
    storm_speed: Optional[int] = None
    storm_dir: Optional[int] = None
    usa_gust: Optional[int] = None
    file_date: str
