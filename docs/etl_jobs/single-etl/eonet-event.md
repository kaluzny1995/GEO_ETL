← [SINGLE_ETL job](../single-etl.md)

# eonet_event_etl

`single_etl/eonet_event_etl.py`: `geo.eonet_event` (NASA EONET hazard events) → `geo.eonet_event_etl` (the 21-column structure from `model/etl/event_etl.py`).

| Target column | Source |
|---------------|--------|
| `subsource_name` | `source_id` (reporting feed: `IRWIN`, `JTWC`, `NATICE`, `NOAA_NHC`, `EO`, `BYU_ICE`) |
| `event_name` | `event_name` |
| `event_category` | `category_id` → `storm` (`severeStorms`), `fire` (`wildfires`), `iceberg` (`seaLakeIce`); unknown ids are snake-cased (`volcanicAsh` → `volcanic_ash`) |
| `event_subcategory` | storm type parsed from `event_name` (`hurricane`, `typhoon`, `tropical storm`, `tropical cyclone`, `cyclone`), the category word `storm` when the name carries no type keyword; `wildfire` for fires; `sea_lake_ice` for ice; `NULL` for a category the ETL does not harmonise (`volcanic_ash`, `landslides`, …) |
| `event_description` | `description`, `NULL` when blank |
| `latitude` / `longitude` | `latitude` / `longitude` — the event's own position |
| `country_code` / `state_province` | the US state at the end of `description` (fires: `… from Miles City, MT`), else the stamped `geocode_country` / `geocode_state` columns, else `ZZ` / `NULL` (open water) |
| `place_location` | `event_name` minus its type prefix (`Wildfire`, `Prescribed Fire`, `Incident Complex`, `Emergency Stabilization BAER`) when a place is left, e.g. `Wildfire Snow, Custer, Montana` → `Snow, Custer, Montana`; `NULL` for `Hurricane Polo` / `Iceberg D32` |
| `metric_name` | `wind speed` (storms), `area` (fires), `surface` (ice), `magnitude` for any other category |
| `metric_value` | `magnitude_value`, counting a missing magnitude as `0` |
| `metric_unit` | the source's `magnitude_unit` converted to the canonical unit of the metric (`kts` → `km/h` for `wind speed`, `acres` → `km²` for `area`), restated as `N/m²` (newton per square metre) for ice in place of the source's square-nautical-mile `NM^2`, `NULL` when blank |
| `danger_rating` | `no_rating` — EONET carries no rating |
| `other_metrics` | `{"geometry_count": n}` — the event's geometry points |
| `event_start` | `event_date` |
| `event_end` | `last_date` |
| `last_latitude` / `last_longitude` | `last_latitude` / `last_longitude` — the latest position |
| `file_date` | `file_date` |

Both answers arrive with every row: GEO_Download stamps `geocode_country` / `geocode_state` into the daily CSVs at download time (its own cached Nominatim / OpenStreetMap lookup), and this ETL only reads the two plain-table columns and applies the precedence in the table — the description's US state first, the stamped answer second, `ZZ` / `NULL` when neither resolves (open water). GEO_ETL performs no lookup and no API call of its own.
