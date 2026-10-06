import datetime as dt

from pydantic import BaseModel, ConfigDict


class OpenMeteoFlood(BaseModel):
    """Typed OpenMeteo flood data (float, date).

    ``discharge_mean_m3s`` / ``discharge_median_m3s`` / ``discharge_p25_m3s``
    / ``discharge_p75_m3s`` are the GloFAS ensemble statistics of the day —
    the downloader stamps them on forecast days only, so they are ``NULL``
    for the analysis day of every file and for all consolidated history.
    """

    model_config = ConfigDict(json_schema_extra={})

    river_name: str | None = None
    country: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    date: dt.date | None = None
    discharge_m3s: float | None = None
    discharge_mean_m3s: float | None = None
    discharge_median_m3s: float | None = None
    discharge_p25_m3s: float | None = None
    discharge_p75_m3s: float | None = None
    geocode_country: str | None = None
    geocode_state: str | None = None
    file_date: str
