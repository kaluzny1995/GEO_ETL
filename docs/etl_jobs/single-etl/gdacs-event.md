← [SINGLE_ETL job](../single-etl.md)

# gdacs_event_etl

`single_etl/gdacs_event_etl.py`: `geo.gdacs_event` (GDACS disaster events, one row per event snapshot per `file_date`) → `geo.gdacs_event_etl` (the 21-column structure from `model/etl/event_etl.py`).

| Target column | Source |
|---------------|--------|
| `subsource_name` | `source` (contributing body: `NEIC`, `NOAA`, `JTWC`, `GWIS`, `GLOFAS`, `GDO`, VAAC and seismic network names) |
| `event_name` | `event_name` |
| `event_category` | `event_type` → `earthquake` (`EQ`), `storm` (`TC`), `flood` (`FL`), `volcano` (`VO`), `fire` (`WF`), `drought` (`DR`); unknown codes are lowercased (`XX` → `xx`) |
| `event_subcategory` | cyclone intensity class from `severity_text` (`tropical depression`, `tropical storm`, `hurricane` — GDACS's `Hurricane/Typhoon` counts as `hurricane`, matching the EONET keyword; the category word `storm` when GDACS states no class); `wildfire` for fires; `agricultural drought` for droughts; the shared defaults for the categories without a source subtype — `other` (earthquakes), `water discharge` (floods), `volcano` (volcanic events); `NULL` for an unqualified drought or an unknown event type |
| `event_description` | `NULL` — GDACS carries no event description |
| `latitude` / `longitude` | `latitude` / `longitude` — epicentre (EQ) or affected-area centroid (the rest) |
| `country_code` | the source's ISO 3166-1 alpha-3 `country_code` translated to alpha-2 (`_cfg.country_code_map`: `USA` → `US`, `DEU` → `DE`, …), else the stamped `geocode_country` column, else `ZZ` |
| `state_province` | the stamped `geocode_state` column (GDACS has none), `NULL` when empty |
| `place_location` | `country` — region at the centroid or affected-country list (e.g. `Brazil, Colombia`), `NULL` when blank |
| `metric_name` | `magnitude` (EQ/FL/VO), `wind speed` (TC), `area` (WF/DR) |
| `metric_value` | `severity`, keeping the `0` ("not defined" — every FL and VO row) as `0` rather than `NULL` |
| `metric_unit` | `severity_unit` converted to the canonical unit of the metric — `ha` and `km2` → `km²` for `area`, `km/h` already canonical for `wind speed`; the `M` of an `earthquake` magnitude restated as the standard `Mw` (Moment Magnitude Scale); `NULL` when blank |
| `danger_rating` | `alert_level` mapped onto the shared severity scale — `Green` → `no_rating`, `Orange` → `medium`, `Red` → `high`; a blank or unknown level also rates `no_rating` |
| `other_metrics` / `last_latitude` / `last_longitude` | `NULL` — a snapshot has no metrics beyond the measurement and no track |
| `event_start` | `from_date` |
| `event_end` | `to_date` (= `from_date` for the instantaneous earthquakes) |
| `file_date` | `file_date` |

The source's alpha-3 country code wins because GDACS knows the affected country better than a centroid lookup; blank and unknown codes fall through to the stamped `geocode_country`, and `state_province` comes from the stamped `geocode_state` (GDACS carries no subdivision of its own). GEO_Download fills both columns for every distinct coordinate when the daily CSVs are written, so this ETL reads them off the plain table and applies only its maps and fallbacks — no lookup file, no API call, no driver-side map.
