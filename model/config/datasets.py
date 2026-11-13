"""Dataset constants loaded from the ``datasets`` section of ``config.json``.

Every upload, single-ETL and view script keeps its wiring (table names,
dataset folders) and transformation constants (source names, category maps,
lookup tables, view queries) in ``config.json``. Scripts load their own entry
at import time, keyed by the script's file stem::

    _cfg = load_dataset(__file__, "single_etl", GdacsEtlConfig)

The models are frozen and reject unknown keys, so a typo in ``config.json``
fails at import instead of silently producing wrong data.
"""

import json
from functools import cache
from pathlib import Path
from typing import Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, field_validator

from model.config.app_config import CONFIG_PATH
from model.enums import DangerRating

T = TypeVar("T", bound=BaseModel)

DatasetSection = Literal["upload", "single_etl", "view", "prediction"]


class UploadDatasetConfig(BaseModel):
    """Wiring for one script in ``ingestion/upload/``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    table_name: str
    dataset_folder: str


class SingleEtlDatasetConfig(BaseModel):
    """Fields every source script in ``single_etl/`` shares.

    Attributes:
        dedup_keys: Canonical columns that identify one natural event in the
            source. The runner adds ``file_date`` to the key set, so the list
            must be non-empty, unique and must not contain ``file_date``.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_table: str
    target_table: str
    source_name: str
    dedup_keys: list[str]

    @field_validator("dedup_keys")
    @classmethod
    def _validate_dedup_keys(cls, keys: list[str]) -> list[str]:
        if not keys:
            raise ValueError("dedup_keys must not be empty")
        if any(not key.strip() for key in keys):
            raise ValueError("dedup_keys must not contain blank column names")
        if len(set(keys)) != len(keys):
            raise ValueError("dedup_keys must be unique")
        if "file_date" in keys:
            raise ValueError("dedup_keys must not include file_date — the runner adds it")
        return keys


class DangerRatedEtlConfig(SingleEtlDatasetConfig):
    """Base config for a script that rates its rows on the shared severity scale.

    Attributes:
        danger_rating_map: The source's native value → one of the
            :class:`~model.enums.DangerRating` words (``no_rating`` … ``extreme``).
            A value outside the scale fails validation, so a typo in
            ``config.json`` fails at import; a native value the map does not
            list is rated ``no_rating`` by the script.
    """

    danger_rating_map: dict[str, DangerRating]


class AvalancheOrgEtlConfig(DangerRatedEtlConfig):
    """Constants for ``single_etl/avalanche_org_etl.py``."""

    country_code: str
    event_category: str
    metric_name: str
    metric_unit: str
    danger_level_unit: str


class EonetEventEtlConfig(SingleEtlDatasetConfig):
    """Constants for ``single_etl/eonet_event_etl.py``."""

    category_map: dict[str, str]


class GdacsEventEtlConfig(DangerRatedEtlConfig):
    """Constants for ``single_etl/gdacs_event_etl.py``."""

    category_map: dict[str, str]
    country_code_map: dict[str, str]


class IbtracsCycloneEtlConfig(DangerRatedEtlConfig):
    """Constants for ``single_etl/ibtracs_cyclone_etl.py``."""

    event_category: str
    metric_name: str
    metric_unit: str
    basin_names: dict[str, str]
    subbasin_names: dict[str, str]
    status_words: dict[str, str]
    nature_words: dict[str, str]
    sshs_labels: dict[str, str]


class NifcFireEtlConfig(SingleEtlDatasetConfig):
    """Constants for ``single_etl/nifc_fire_etl.py``."""

    subsource_name: str
    country_code: str
    event_category: str
    metric_name: str
    metric_unit: str
    percent_unit: str
    fire_type_map: dict[str, str]


class OpenMeteoFloodEtlConfig(DangerRatedEtlConfig):
    """Constants for ``single_etl/openmeteo_flood_etl.py``."""

    subsource_name: str
    event_category: str
    metric_name: str
    metric_unit: str
    country_code_map: dict[str, str]
    thresholds_file: str
    threshold_percentile: float
    percentile_levels: list[int]
    ensemble_levels: list[str]


class UsgsEarthquakeEtlConfig(SingleEtlDatasetConfig):
    """Constants for ``single_etl/usgs_earthquake_etl.py``."""

    event_category: str
    metric_name: str
    unit_km: str
    unit_stations: str
    event_name_prefix: str
    event_name_fallback: str
    us_state_names: str
    us_state_codes: list[str]
    us_state_full_names: list[str]
    country_code_map: dict[str, str]


class UsgsNwisEtlConfig(DangerRatedEtlConfig):
    """Constants for ``single_etl/usgs_nwis_etl.py``."""

    event_category: str
    metric_name: str
    country_code: str
    event_name_prefix: str
    event_name_fallback: str
    name_split_pattern: str
    qualifier_names: dict[str, str]
    flood_categories_file: str
    flood_levels: list[str]
    stage_unit: str


class UsgsVolcanoEtlConfig(DangerRatedEtlConfig):
    """Constants for ``single_etl/usgs_volcano_etl.py``.

    Attributes:
        color_danger_rating_map: Aviation color code → the shared severity
            scale, the companion of ``danger_rating_map`` (the USGS alert
            level). The script rates a notice with the harsher of the two —
            the scales are independent and either one alone can be missing.
    """

    event_category: str
    metric_name: str
    metric_unit: str
    country_code: str
    event_name_prefix: str
    event_name_fallback: str
    vnum_unit: str
    casualties_unit: str
    damage_unit: str
    color_danger_rating_map: dict[str, DangerRating]


class UnionEtlConfig(BaseModel):
    """Constants for ``single_etl/union_etl.py`` (the union script).

    Unlike the source scripts it has no single source table and stamps no
    ``source_name`` — it stacks rows the other ``*_etl`` scripts already
    produced.

    Attributes:
        source_tables: Source ``*_etl`` tables stacked into *target_table* in
            this order (the nine registered ``<source>_etl`` tables).
        target_table: Table the stacked rows are appended to (``union_etl``).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    target_table: str
    source_tables: list[str]


class ViewDatasetConfig(BaseModel):
    """Wiring for one script in ``views/``.

    Attributes:
        source_table: ``*_etl`` table the view is defined over
            (e.g. ``"union_etl"``).
        target_view: View name the script creates, always ``*_vw``
            (e.g. ``"union_vw"``).
        query: Complete ``SELECT`` statement the view exposes — it must read
            *source_table* (schema-qualified, e.g. ``geo.union_etl``). The
            runner executes it through Spark JDBC first to prove it returns
            rows, then stores it as ``CREATE OR REPLACE VIEW
            geo.<target_view> AS <query>``.
        country_codes: Sorted ISO 3166-1 alpha-2 whitelist for the regional
            phenomenon views; empty for views that filter on a single country
            or do not filter by country. The script exposes it as
            ``COUNTRY_CODES`` and the query must use the same list.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_table: str
    target_view: str
    query: str
    country_codes: list[str] = Field(default_factory=list)

    @field_validator("country_codes")
    @classmethod
    def _validate_country_codes(cls, codes: list[str]) -> list[str]:
        if not codes:
            return codes
        if len(set(codes)) != len(codes):
            raise ValueError("country_codes must be unique")
        if codes != sorted(codes):
            raise ValueError("country_codes must be sorted")
        if any(len(code) != 2 or not code.isascii() or not code.isalpha() or not code.isupper() for code in codes):
            raise ValueError("country_codes must be two-letter upper-case ISO 3166-1 alpha-2 codes")
        return codes


class PredictionConfig(BaseModel):
    """Configuration for the PySpark MLlib PREDICTION job.

    Attributes:
        source_view: Observed metrics view over ``geo.union_etl``.
        table_name: Prediction table written by the job.
        model_directory: Directory for persisted Spark MLlib models and metadata.
        exclude_metrics: Metrics that are categorical/danger-derived or
            otherwise meaningless to predict.
        min_observations: Minimum observed daily points required to train.
        test_fraction: Fraction of the latest observed dates held out for model
            selection and drift monitoring.
        retrain_rmse_relative_tolerance: Retrain when current hold-out RMSE is
            more than this fraction above the stored model RMSE.
        retrain_rmse_absolute_tolerance: Additional absolute RMSE slack; the
            effective threshold is ``max(stored * (1 + relative), stored + absolute)``.
        force_retrain: Ignore persisted models and retrain every metric.
        polynomial_degree: Polynomial expansion degree for the linear model.
        linear_reg_param: LinearRegression L2 regularisation.
        gbt_max_iter: GBTRegressor boosting iterations.
        gbt_max_depth: GBTRegressor tree depth.
        gbt_step_size: GBTRegressor learning rate.
        random_seed: Shared MLlib random seed.
        integer_metrics: Metrics rounded to whole numbers after prediction.
        metric_minimums: Optional lower bounds by metric name.
        metric_maximums: Optional upper bounds by metric name.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_view: str = "geo.metrics_observed_vw"
    table_name: str = "prediction_etl"
    model_directory: str = "ml_models"
    exclude_metrics: list[str] = Field(default_factory=list)
    min_observations: int = 5
    test_fraction: float = 0.2
    retrain_rmse_relative_tolerance: float = 0.25
    retrain_rmse_absolute_tolerance: float = 0.0
    force_retrain: bool = False
    polynomial_degree: int = 2
    linear_reg_param: float = 0.1
    gbt_max_iter: int = 50
    gbt_max_depth: int = 4
    gbt_step_size: float = 0.1
    random_seed: int = 42
    integer_metrics: list[str] = Field(default_factory=list)
    metric_minimums: dict[str, float] = Field(default_factory=dict)
    metric_maximums: dict[str, float] = Field(default_factory=dict)

    @field_validator("test_fraction")
    @classmethod
    def _validate_test_fraction(cls, value: float) -> float:
        if not 0.0 < value < 1.0:
            raise ValueError("test_fraction must be between 0 and 1")
        return value

    @field_validator("min_observations")
    @classmethod
    def _validate_min_observations(cls, value: int) -> int:
        if value < 2:
            raise ValueError("min_observations must be at least 2")
        return value


@cache
def _read_datasets(path: str) -> dict:
    with open(path) as f:
        return json.load(f)["datasets"]


def load_prediction_config(path: str = CONFIG_PATH) -> PredictionConfig:
    """Load the single ``datasets.prediction`` configuration block."""
    return PredictionConfig(**_read_datasets(path)["prediction"])


def load_dataset(script_file: str, section: DatasetSection, model: type[T], *, path: str = CONFIG_PATH) -> T:
    """Return the dataset configuration for one script module.

    Args:
        script_file: ``__file__`` of the calling script; the entry is keyed
            by its file stem (e.g. ``"gdacs_event_etl"``).
        section: Layer the script belongs to (``"upload"``, ``"single_etl"``
            or ``"view"``).
        model: Pydantic model validating the entry.
        path: Config file to read. Defaults to the project ``config.json``.

    Returns:
        A frozen instance of *model*.

    Raises:
        KeyError: When *section* or the script's key is missing.
        ValidationError: When the entry does not match *model*.
    """
    key = Path(script_file).stem
    try:
        entry = _read_datasets(path)[section][key]
    except KeyError as exc:
        raise KeyError(f"No dataset entry '{key}' under 'datasets.{section}' in {path}") from exc
    return model(**entry)
