"""Dataset constants loaded from the ``datasets`` section of ``config.json``.

Every upload, raw2plain, single-ETL and view script keeps its wiring (table
names, dataset folders) and transformation constants (source names, category
maps, lookup tables, view queries) in ``config.json``. Scripts load their own
entry at import time, keyed by the script's file stem::

    _cfg = load_dataset(__file__, "single_etl", GdacsEtlConfig)

The models are frozen and reject unknown keys, so a typo in ``config.json``
fails at import instead of silently producing wrong data.
"""

import json
from functools import cache
from pathlib import Path
from typing import Literal, TypeVar

from pydantic import BaseModel, ConfigDict

from model.config.app_config import CONFIG_PATH
from model.enums import DangerRating

T = TypeVar("T", bound=BaseModel)

DatasetSection = Literal["upload", "raw2plain", "single_etl", "view"]


class UploadDatasetConfig(BaseModel):
    """Wiring for one script in ``ingestion/upload/``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    table_name: str
    dataset_folder: str


class Raw2PlainDatasetConfig(BaseModel):
    """Wiring for one script in ``ingestion/raw2plain/``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    raw_table: str
    plain_table: str


class SingleEtlDatasetConfig(BaseModel):
    """Fields every script in ``single_etl/`` shares."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_table: str
    target_table: str
    source_name: str


class DangerRatedEtlConfig(SingleEtlDatasetConfig):
    """Base config for a script that rates its rows on the shared severity scale.

    Attributes:
        danger_rating_map: The source's native value → one of the
            :class:`~model.enums.DangerRating` words (``none`` … ``extreme``).
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
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_table: str
    target_view: str
    query: str


@cache
def _read_datasets(path: str) -> dict:
    with open(path) as f:
        return json.load(f)["datasets"]


def load_dataset(script_file: str, section: DatasetSection, model: type[T], *, path: str = CONFIG_PATH) -> T:
    """Return the dataset configuration for one script module.

    Args:
        script_file: ``__file__`` of the calling script; the entry is keyed
            by its file stem (e.g. ``"gdacs_event_etl"``).
        section: Layer the script belongs to (``"upload"``, ``"raw2plain"``,
            ``"single_etl"`` or ``"view"``).
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
