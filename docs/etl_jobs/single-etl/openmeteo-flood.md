← [SINGLE_ETL job](../single-etl.md)

# openmeteo_flood_etl

`single_etl/openmeteo_flood_etl.py`: `geo.openmeteo_flood` (Open-Meteo/GloFAS river discharge, one row per river per `date` per `file_date`) → `geo.openmeteo_flood_etl` (the 21-column structure from `model/etl/event_etl.py`).

| Target column | Source |
|---------------|--------|
| `source_name` | always `OPENMETEO` |
| `subsource_name` | always `GLOFAS` (the Copernicus GloFAS model the discharge is simulated with) |
| `event_name` | `"<river_name> discharge"` (`Amazon discharge`), `NULL` when the river name is blank |
| `event_category` | always `flood` |
| `event_subcategory` | `water discharge` — the shared flood default (a daily river value carries no subtype of its own) |
| `event_description` | `NULL` — the daily value carries no description |
| `danger_rating` | the flood danger of the day on the shared severity scale, judged against the river's own baseline percentiles (the reference's `discharge_p99_m3s` / `discharge_p95_m3s` columns): `extreme` strictly above `discharge_p99_m3s`, `high` strictly above `discharge_p95_m3s`, `no_rating` at or below — `threshold_percentile` (0.95) is the floor, so the ladder never fires for a day the flood flag does not fire for — as well as for a row that cannot be rated at all: a river the threshold reference does not cover, a run before the file was built, or a row without a discharge. The comparison is strict, so a flat baseline (every day sitting exactly on its own `Q95`, e.g. an all-zero series) rates `no_rating` |
| `latitude` / `longitude` | the `rivers.json` reference point of the river (stable per river; the GloFAS cell actually used is the nearest 0.05°/~5 km grid cell) |
| `country_code` | the source `country` name mapped through the script's 54-entry ISO map (`USA` → `US`, `UK` → `GB`, `DRC` → `CD`); an unknown name falls back to the stamped `geocode_country` column, an empty value resolves to `ZZ` |
| `state_province` | the stamped `geocode_state` column of the river's reference point, `NULL` when empty |
| `place_location` | `"<river_name> - <country>"` (`Amazon - Brazil`); a blank part is dropped, both blank is `NULL` |
| `metric_name` | always `discharge` |
| `metric_value` | `discharge_m3s` — the GloFAS 24-h mean in m³/s, counting a missing value as `0` (the source skips null-discharge days, so none occur) |
| `metric_unit` | always `m³/s` |
| `event_start` | `date` itself at midnight — the GMT+0 day of the 24-h mean |
| `event_end` | `NULL` — a daily value has no end |
| `last_latitude` / `last_longitude` | `NULL` — the reference point is fixed, there is no latest position |
| `other_metrics` | the river's threshold reference — `{"discharge_p50": …, "discharge_p50_unit": "m³/s", …, "discharge_p99": …, "discharge_p99_unit": "m³/s", "threshold_percentile": 0.95, "threshold", "threshold_unit": "m³/s", "exceedance_ratio", "threshold_exceeded", "exceedance_level", "baseline_days", "baseline_start", "baseline_end"}` — plus, on forecast days, the GloFAS ensemble spread `{"ensemble_mean": …, "ensemble_mean_unit": "m³/s", "ensemble_median", "ensemble_p25", "ensemble_p75"}` with their own `_unit` companions; `exceedance_level` keeps the source's own wording of the level the discharge reached (`Q95` / `Q99`), omitted when the day exceeded none, while `danger_rating` holds the same fact on the shared scale; every measurement carries its unit in a separate `<name>_unit` key (`m³/s`) rather than inside the key name, and a unit key never outlives its value; keys the row cannot answer are dropped together with their group (no reference row → no percentiles, baseline or threshold; analysis day → no ensemble) and a row with nothing to say is `NULL` rather than `{}` (the lead time `date - file_date` remains derivable) |
| `file_date` | `file_date` |

Each downloaded file holds the analysis day plus the next day's forecast (222 rows = 111 rivers × 2 days — the current CSVs hold exactly that: 111 rivers, two `date` values each, out of the candidates listed in `rivers.json`; only rivers that returned discharge data are written), so a `(river, date)` pair appears in two files — yesterday's forecast is today's analysis — and the ETL keeps every snapshot as its own row; `(river, date, file_date)` is unique, so the shared [deduplication step](../single-etl.md#deduplication) finds nothing to remove (its river-day key stays as a safety net). The country map runs in-script; the country fallback and the subdivision come from the `geocode_country` / `geocode_state` columns GEO_Download stamps into every downloaded file at download time, so this ETL performs no lookup and needs no network.

Besides the source, the script left-joins the threshold reference CSV `data/openmeteo_discharge_thresholds.csv` on `river_name` + `country` (broadcast — one row per river, built by GEO_Download's `run_openmeteo_thresholds.py` from the GloFAS baseline, which `run_download.py` creates when the file is missing) and drops the reference's own `latitude`/`longitude`, so the source row keeps its coordinates; a river missing from the reference — or a run before the file was built — rates `danger_rating` `no_rating` and leaves the threshold keys `NULL` instead of failing. The GloFAS ensemble statistics ship only on forecast days (`date` after the analysis day), so `other_metrics` on an analysis day carries the reference alone. The percentile levels come from `percentile_levels` in `config.json` (`50 / 90 / 95 / 99`) and the ensemble keys from `ensemble_levels` (`mean / median / p25 / p75`); `threshold_percentile` picks which of those levels starts the danger ladder and must name one of them.
