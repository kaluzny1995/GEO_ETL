← [SINGLE_ETL job](../single-etl.md)

# nifc_fire_etl

`single_etl/nifc_fire_etl.py`: `geo.nifc_fire` (NIFC/WFIGS fire perimeters, one row per perimeter per `file_date`) → `geo.nifc_fire_etl` (the 21-column structure from `model/etl/event_etl.py`).

| Target column | Source |
|---------------|--------|
| `subsource_name` | always `WFIGS` (the Wildland Fire Interagency Geospatial Services layer the perimeters come from) |
| `event_name` | `incident_name`, `NULL` when blank |
| `event_category` | always `fire` |
| `event_subcategory` | `fire_type`: `WF` → `wildfire`, `RX` → `prescribed fire`; the category word `fire` when the source states no type |
| `event_description` | `NULL` — the perimeter layer carries no advisory prose |
| `danger_rating` | `no_rating` — the perimeter layer carries no alert level |
| `latitude` / `longitude` | the feature centroid of the perimeter |
| `country_code` | always `US` — the layer only publishes US perimeters |
| `state_province` | `state` when present (the 2026-09-18 vintage on), else the stamped `geocode_state` column, `NULL` when neither resolves |
| `place_location` | the full name as it ships with the source row in `unit_name` (GEO_Download resolves `jurisdiction_label` through its `data/nifc_unit_names.csv` lookup when the daily CSVs are written: `WAOWF` → `Okanogan-Wenatchee National Forest`, `CAYNP` → `Yosemite National Park`, `TXTXS` → `Texas A & M Forest Service`, …); a code the lookup does not know keeps its raw value, a blank one is `NULL` — the agency codes in `jurisdiction` have no published domain and are not used |
| `metric_name` | always `area` |
| `metric_value` | `acres` converted to `km²` (× 0.0040468564224), counting a missing acreage as `0` (the pre-2026-09-18 vintage predates the field) |
| `metric_unit` | the canonical `area` unit `km²` |
| `other_metrics` | `{"percent_contained": …, "percent_contained_unit": "%"}` — always present: a missing `percent_contained` counts as `0` (nothing reported yet), so both keys stay valid |
| `event_start` | `created_on` |
| `event_end` | `NULL` — the layer carries no fire-out datetime |
| `last_latitude` / `last_longitude` | `NULL` — the layer carries no latest position |
| `file_date` | `file_date` |

The jurisdiction's full name ships with every source row — GEO_Download resolves it at download time through its consolidated `data/nifc_unit_names.csv` lookup, so this ETL performs no unit lookup and no network request for it. The subdivision follows the same pass-through pattern: the source's own `state` always wins, and only the rows with a blank `state` (the pre-state vintage) fall back to the `geocode_state` column GEO_Download stamped into the CSVs at download time — GEO_ETL runs no lookup of its own, and a row neither answers resolves to `NULL`.
