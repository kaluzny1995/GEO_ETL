"""Predicted daily metric values for missing snapshot dates.

The table is deliberately separate from the event ``*_etl`` tables. A row here
means: "for this phenomenon/metric/date, the observed metric views have no
observation, and PySpark MLlib estimated this daily mean value." The metric
views union these rows in with the observed rows and expose ``is_predicted``.
"""

import datetime as dt

from pydantic import BaseModel


class PredictionEtl(BaseModel):
    """One ML-predicted value for a missing metric/date combination.

    Attributes:
        event_category: Phenomenon category, e.g. ``avalanche``.
        metric_name: Metric being predicted, e.g. ``size``.
        metric_unit: Unit of the metric, or ``None`` for unit-less metrics.
        file_date: Predicted daily snapshot date.
        predicted_value: Model estimate of the daily mean metric value.
        is_predicted: Always ``True`` in this table; the metrics views add
            ``False`` for observed rows so consumers get one shared flag.
        model_name: Selected PySpark MLlib model type.
        training_observations: Number of observed daily points used for training.
        validation_rmse: Hold-out RMSE at the last training run, when available.
        predicted_at: When the prediction row was produced.
    """

    event_category: str
    metric_name: str
    metric_unit: str | None = None
    file_date: dt.date
    predicted_value: float
    is_predicted: bool
    model_name: str
    training_observations: int
    validation_rmse: float | None = None
    predicted_at: dt.datetime
