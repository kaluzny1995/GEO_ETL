← [SINGLE_ETL job](../single-etl.md)

# ibtracs_cyclone_etl

`single_etl/ibtracs_cyclone_etl.py`: `geo.ibtracs_cyclone` (IBTrACS best track, one row per storm position per `file_date`) → `geo.ibtracs_cyclone_etl` (the 21-column structure from `model/etl/event_etl.py`).

| Target column | Source |
|---------------|--------|
| `subsource_name` | `basin` — the storm's basin (`NA`, `EP`, `WP`, `NI`), `NULL` when blank |
| `event_name` | `name` (e.g. `EDOUARD`; the source writes `UNNAMED` for unnamed systems), `NULL` when blank |
| `event_category` | always `storm` |
| `event_subcategory` | the ATCF system status `usa_status` (`hurricane`, `typhoon`, `tropical storm`, `tropical depression`, `disturbance`, `low`, …), falling back to the cross-agency `nature` (`tropical`, `disturbance`, `extratropical`, `subtropical`, `contradictory`) when no US agency reported a status, then the category word `storm` itself — every row is a cyclone (`nature = NR` in non-US basins) |
| `event_description` | `NULL` — a best track carries no advisory prose |
| `latitude` / `longitude` | `lat` / `lon` — the position itself |
| `country_code` / `state_province` | the stamped `geocode_country` / `geocode_state` columns, else `ZZ` / `NULL` (open water) — the source carries no country at all |
| `place_location` | full basin name extended with the sub-basin (`North Atlantic - Gulf of Mexico`, `Eastern North Pacific - Central Pacific`, `North Indian - Bay of Bengal`, …); just the basin name for the unnamed sub-basins (`NA`, `MM`) |
| `metric_name` | always `wind speed` |
| `metric_value` | `coalesce(wmo_wind, usa_wind)` — WMO values are absent in real-time data, so the US agency's knots win today — converted to `km/h` (× 1.852); a point with no wind at all stays `NULL` |
| `metric_unit` | the canonical `wind speed` unit `km/h` |
| `danger_rating` | the Saffir-Simpson class of `usa_sshs` mapped onto the shared severity scale — categories 4–5 → `extreme`, 3 → `high`, 1–2 → `medium`, a tropical storm and every weaker system → `low`; a `NULL` or unmapped class (including `unknown`, `-5`) rates `no_rating`. The source's own wording (`category 5` … `category 1`, `tropical storm`, `tropical depression`, `disturbance`, `subtropical`, `post-tropical`) stays available as `other_metrics.saffir_simpson` |
| `other_metrics` | `{"pressure": …, "pressure_unit": "mb", "speed": …, "speed_unit": "km/h", "direction": …, "direction_unit": "degrees", "gust": …, "gust_unit": "km/h", "dist2land": …, "dist2land_unit": "km", "landfall": …, "landfall_unit": "km"}` — `usa_pres`, `storm_speed`, `storm_dir`, `usa_gust`, `dist2land` / `landfall`; every measurement is paired with its `<name>_unit` companion key, and one the position does not report is omitted together with its unit; the two wind measurements (`speed`, `gust`) are the source's knots converted to the canonical `km/h` (× 1.852) so they agree with `metric_value`; `NULL` when it reports none |
| `event_start` / `event_end` | `iso_time` — one row = one track position |
| `last_latitude` / `last_longitude` | `lat` / `lon` of the maximum `iso_time` per `sid` — the storm's latest position |
| `file_date` | `file_date` |

The source knows no country, so GEO_Download answers it at download time: every row carries the stamped `geocode_country` / `geocode_state` columns (its fetch set is the positions within 100 km of land; far-ocean points keep an already-cached answer or stay empty), and this ETL only reads them, resolving an empty answer to `ZZ` / `NULL` (open water). `last_*` is a window maximum over `iso_time` per `sid`, which stays correct under `INCREMENTAL`: the newest points always arrive in the newest `file_date`, and storms that stopped reporting keep their previous `last_*` values.
