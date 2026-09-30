import datetime as dt

from pydantic import BaseModel


class DataQuality(BaseModel):
    """One data quality metric value for one file_date of one table.

    Stored long-format (one row per metric) so history, trends and
    threshold queries are plain SQL against ``geo.data_quality``.
    """

    table_name: str
    file_date: str
    metric: str
    value: float
    computed_at: dt.datetime
