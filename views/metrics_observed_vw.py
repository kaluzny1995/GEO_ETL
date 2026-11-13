"""``geo.union_etl`` → ``geo.metrics_observed_vw`` (all observed metrics).

This base view flattens the row's own metric plus every numeric
``other_metrics`` entry for all eight phenomenon categories. It is the shared
observed-metric contract for both the PREDICTION job and the per-phenomenon
metrics views.
"""

from model.config.datasets import ViewDatasetConfig, load_dataset
from views.view_script import ViewScript

_cfg = load_dataset(__file__, "view", ViewDatasetConfig)


class MetricsObservedVwScript(ViewScript):
    """Every observed numeric phenomenon metric in ``geo.union_etl``."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_VIEW = _cfg.target_view
    QUERY = _cfg.query
