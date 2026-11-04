← [SINGLE_ETL job](../single-etl.md)

# usgs_nwis_etl

`single_etl/usgs_nwis_etl.py`: `geo.usgs_nwis` (USGS NWIS streamflow, one row per gauge per `date` per `file_date`) → `geo.usgs_nwis_etl` (the 21-column structure from `model/etl/event_etl.py`).

| Target column | Source |
|---------------|--------|
| `source_name` | always `USGS_NWIS` |
| `subsource_name` | `site_id` verbatim (`USGS-01646500`) |
| `event_name` | `"Streamflow - <river name>"` — the `site_name` up to its first `NEAR`/`AT` marker, lower-cased with only the first letter upper-cased (`Streamflow - Potomac river`); `Streamflow` alone when the name is missing or blank |
| `event_category` | always `flood` (the same category the OpenMeteo discharge snapshots use) |
| `event_subcategory` | `water discharge` — the shared flood default (a gauge day has no subtype of its own) |
| `event_description` | the `qualifiers` expanded to their full names (`P` → `provisional`, `A,e` → `approved for publication, estimated`); an unknown code is kept verbatim, a blank qualifier is `NULL` |
| `latitude` / `longitude` | the gauge point (WGS84 decimal degrees) |
| `country_code` | always `US` — the network publishes US gauges |
| `state_province` | the stamped `geocode_state` column of the gauge point, `NULL` when empty |
| `place_location` | `"<river name>, <place text>"` — both parts extracted from `site_name` around its first `NEAR`/`AT` marker (`Potomac river, WASH, DC LITTLE FALLS PUMP STA`); without a marker only the sentence-cased name remains, a missing name is `NULL` |
| `metric_name` | always `discharge` (parameter `00060`) |
| `metric_value` | `value` — the daily mean converted to `m³/s` (ft³/s × 0.028316846592), counting a missing reading as `0` (none occur) |
| `metric_unit` | the canonical `discharge` unit `m³/s` |
| `event_start` | `date` at midnight — the source reports a day, not a datetime |
| `event_end` | `NULL` — a daily mean has no end |
| `last_latitude` / `last_longitude` | `NULL` — the gauge is fixed |
| `danger_rating` | the NWS flood category of the day's peak stage mapped onto the shared severity scale — `no_flooding` → `no_rating`, `action` → `low`, `minor` → `medium`, `moderate` → `high`, `major` → `extreme`; an empty, `NULL` or unknown category also rates `no_rating` |
| `other_metrics` | the gauge's peak against the NWS reference — `{"gauge_height": …, "gauge_height_unit": "ft", "nws_gauge_id": "BRKM2", "action_stage": …, "action_stage_unit": "ft", …, "major_stage": …, "major_stage_unit": "ft"}` — the day's peak gauge height, the NWS gauge id and the four flood-stage thresholds, every measurement carrying its unit in a separate `<name>_unit` key (`ft`) rather than inside the key name; `gauge_height` (with its unit) is omitted without a peak reading, the thresholds without a covering reference row, and a row with nothing to say is `NULL` |
| `file_date` | `file_date` |

Besides the source, the script left-joins the flood-category reference `data/usgs_flood_categories.csv` on `site_id` (broadcast — one row per gauge, kept present by GEO_Download's `USGS_NWIS` downloader from the NWS NWPS gauges) and picks only the join key, the NWS gauge id and the four stage thresholds out of it, so the source row keeps its own `site_name` and point; a gauge the reference does not cover — or a run before the file was built — leaves those keys `NULL` instead of failing. The rating itself comes from the source row: the downloader already classified the day's peak, so `danger_rating` is filled even when the reference has no thresholds for the gauge.

Dropped from the ETL: `parameter_code` (its value *is* the discharge metric) and `gauge_height` / `flood_category` (they live in `other_metrics` / `danger_rating`). Each downloaded file holds the measurement day plus sometimes the next day (54 rows over 33 files for one gauge), so a `(site, date)` pair can appear in two files — yesterday's next-day row is today's reading — and the ETL keeps every snapshot as its own row; `(site, date, file_date)` is unique, so the shared [deduplication step](../single-etl.md#deduplication) finds nothing to remove (its gauge-day key stays as a safety net). The subdivision comes from the `geocode_state` column GEO_Download stamped into the CSVs at download time — this ETL performs no lookup of its own.
