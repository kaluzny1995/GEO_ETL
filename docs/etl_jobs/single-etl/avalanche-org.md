← [SINGLE_ETL job](../single-etl.md)

# avalanche_org_etl

`single_etl/avalanche_org_etl.py` is the reference script: `geo.avalanche_org` (one row per forecast zone per day) → `geo.avalanche_org_etl` (the 21-column structure from `model/etl/event_etl.py`).

| Target column | Source |
|---------------|--------|
| `subsource_name` | `center_name` (issuing center) |
| `event_category` / `event_subcategory` | `avalanche` — a forecast zone carries no subtype |
| `place_location` | `zone_name`; `event_name` stays `NULL` — a forecast has no event name |
| `latitude` / `longitude` | the source's `latitude` / `longitude` columns — polygon centroid of the zone computed by GEO_Download at download time |
| `metric_name` | always `size` |
| `metric_value` | `avalanche_size` — the zone's expected avalanche size, the largest destructive size class (`D1` … `D5`, half steps allowed) the forecast publishes; `NULL` while the forecast publishes no problems (off-season, unrated, or a day outside the live product's window) |
| `metric_unit` | always `destructive size class` alongside a value, `NULL` without one |
| `danger_rating` | the zone's NAPADS level mapped onto the shared severity scale — `low` (1) … `extreme` (5); unrated zones (`danger_level = -1`, `danger_rating = "no rating"`) and a level the map does not know rate `no_rating` |
| `event_start` / `event_end` | `forecast_start` / `forecast_end` |
| `other_metrics` | `{"danger_level": …, "danger_level_unit": "NAPADS level", "warning": "warning", "off_season": …}` — the raw scale index (`-1` = no rating) with its `<name>_unit` companion key, the avalanche-warning product (omitted while no warning covers the zone) and the season state; zone ids and links are not metrics |

The coordinates ship with every source row, so this ETL performs no lookup and needs no network: GEO_Download computes each zone's polygon centroid when the daily CSVs are written (and consolidates them in `GEO_Download/data/avalanche_org_notices.csv`). A row without coordinates (`latitude` / `longitude` empty — the download found no usable geometry) maps to `NULL`.

Each source file holds the analysis day plus the next day's forecast (the shared `[D, D+1]` download range), and `date` is not part of the canonical structure — while both rows share one forecast window (the off-season "no rating" product), they project to the same dedup key. The shared [deduplication step](../single-etl.md#deduplication) (`DEDUP_KEYS` + `file_date`, applied after `_convert`) collapses such copies — and rows differing only in payload, e.g. a conflicting `off_season` flag — so the target keeps one row per forecast statement per `file_date` (one row per zone per file); rows that differ — e.g. the next day's own validity window in season — keep a different key and stay their own snapshot.
