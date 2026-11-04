← [SINGLE_ETL job](../single-etl.md)

# usgs_earthquake_etl

`single_etl/usgs_earthquake_etl.py`: `geo.usgs_earthquake` (USGS/FDSN seismic events, one row per event — `event_id` is globally unique) → `geo.usgs_earthquake_etl` (the 21-column structure from `model/etl/event_etl.py`).

| Target column | Source |
|---------------|--------|
| `source_name` | always `USGS_EARTHQUAKE` |
| `subsource_name` | `net` verbatim — the preferred origin's network (`ak`, `nc`, `ci`, …) |
| `event_name` | `"Earthquake - <country name>"` — `United States` for a US state suffix, the `place` suffix itself for international events (`Earthquake - Chile`), the stamped `geocode_country` for places the suffix does not cover; `Earthquake` alone when no country resolves |
| `event_category` | always `earthquake` |
| `event_subcategory` | `event_type`: `earthquake`, `quarry blast`, `explosion`, `ice quake`; `other` when the feed states no type |
| `event_description` | `"<event_type> <status>"` (`earthquake reviewed`, `quarry blast automatic`) |
| `latitude` / `longitude` | the epicentre (WGS84 decimal degrees) |
| `country_code` | from the `place` suffix: a US state (2-letter code or full name) → `US`, an international suffix → the 60-entry name→ISO map (`Chile` → `CL`, `MX` → `MX`, `Japan region` → `JP`, `Guam` → `US`), otherwise the stamped `geocode_country` column; `ZZ` when nothing resolves |
| `state_province` | `net` uppercased (`ak` → `AK`, `ci` → `CI`), the stamped `geocode_state` column when `net` is blank, `NULL` when neither resolves |
| `place_location` | `place` verbatim (`9 km WSW of Idyllwild, CA`) |
| `metric_name` | always `magnitude` |
| `metric_value` | `mag`, counting a missing magnitude as `0` (none occur) |
| `metric_unit` | `magType` per row, unified as `Mw` (Moment Magnitude Scale): the moment-magnitude family (`mw`, `mww`, `mwr`, …) already is `Mw` and only gets relabelled; the local, body-wave, duration and Lg scales (`ml`, `md`, `mb`, `mb_lg`) are converted first with the empirical rule USGS OF 1997/464 (p. 4) publishes for each — `Mw* = 0.67 * (value + 1.5)` when `ml`/`md` is below 4.0 or `mb`/`mb_lg` (the Nuttli `Mn` scale) is below 3.0, the reported value otherwise (so every `mb` ≥ 3.0 is left as it stands); `mh` (USGS gives its equation as `N/A` — a non-standard, temporary designation) and `mun` (unknown magnitude, not a USGS magnitude type) have no published conversion and keep their own scale label |
| `other_metrics` | `{"depth", "depth_unit": "km", "nst", "nst_unit": "stations"}`; the `nst` keys are omitted when the network reports no station count |
| `event_start` | `time` — the rupture origin time (millisecond precision) |
| `event_end` | `NULL` — an instant has no end |
| `last_latitude` / `last_longitude` | `NULL` — the epicentre is fixed |
| `danger_rating` | `no_rating` — `status` is QC, not severity; the feed carries no alert level |
| `file_date` | `file_date` |

Dropped from the ETL: `gap`, `dmin`, `rms`, `horizontalError`, `depthError`, `magError`, `magNst`, `updated`, `locationSource`, `magSource`. The country parse runs in-script and needs no network for ~99% of rows — the ~93 places without a usable suffix (and the rows with a blank `net`) read the `geocode_country` / `geocode_state` columns GEO_Download stamped into the CSVs at download time, so GEO_ETL performs no lookup of its own.
