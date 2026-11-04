← [SINGLE_ETL job](../single-etl.md)

# usgs_volcano_etl

`single_etl/usgs_volcano_etl.py`: `geo.usgs_volcano` (USGS HANS volcanic activity notices, one row per volcano per notice per `file_date`) → `geo.usgs_volcano_etl` (the 21-column structure from `model/etl/event_etl.py`).

| Target column | Source |
|---------------|--------|
| `source_name` | always `USGS_VOLCANO` |
| `subsource_name` | `observatory_name` verbatim (`Alaska Volcano Observatory`, `Hawaiian Volcano Observatory`, `Northern Mariana Islands`) |
| `event_name` | `"Volcano - <volcano_name>"` (`Volcano - Great Sitkin`); `Volcano` alone when the name is missing or blank |
| `event_category` | always `volcano` |
| `event_subcategory` | the category word `volcano` — a notice is a volcano report, not a subtype of one; the notice type (`Daily Update`, `Weekly Update`, `Volcano Activity Notification`) stays in the plain table |
| `event_description` | the stamped `synopsis` status line (`AVO Great Sitkin ORANGE/WATCH - Slow eruption of lava …`), `NULL` when blank |
| `latitude` / `longitude` | the volcano's coordinates the downloader stamped into the CSV — the HANS feed itself carries no location |
| `country_code` | always `US` — the three observatories are US |
| `state_province` | the stamped `geocode_state` column of the volcano point, `ZZ` when it is empty (the Ahyi Seamount is an ocean point) |
| `place_location` | `"<volcano_name>, <region>"` (`Great Sitkin, Aleutians`); without a region only the name remains, a blank name is `NULL` |
| `metric_name` | always `vei` |
| `metric_value` | the Volcanic Explosivity Index of the volcano's largest significant eruption in the NCEI/WDS Significant Volcanic Eruptions Database (Kīlauea → `4`); `0` for a volcano NCEI does not list (e.g. Great Sitkin) |
| `metric_unit` | always `VEI` |
| `danger_rating` | the notice's alert level **and** aviation color code, both mapped onto the shared severity scale — `NORMAL`/`GREEN` → `no_rating`, `ADVISORY`/`YELLOW` → `low`, `WATCH`/`ORANGE` → `high`, `WARNING`/`RED` → `extreme` — and the harsher of the two answers wins; a notice with neither a known alert level nor a known color code also rates `no_rating`. The VEI is **not** a rating of the notice; it stays in `metric_value` |
| `event_start` | `notice_date` — when the notice was issued |
| `event_end` | `NULL` — a notice is an instant |
| `last_latitude` / `last_longitude` | `NULL` — the volcano is fixed |
| `other_metrics` | `{"color_code": "ORANGE", "alert_level": "WATCH", "vnum": 311120, "vnum_unit": "Smithsonian GVP", "notice_identifier": "DOI-USGS-AVO-2026-09-29T18:26:19+00:00", "casualties": 5411, "casualties_unit": "people", "damage": 385.5, "damage_unit": "million USD"}` — the Aviation Color Code, the USGS alert level, the Smithsonian GVP volcano number with its `<name>_unit` companion key, the globally unique notice id, and the NCEI eruption totals (total deaths, total damage in millions of USD) with their `<name>_unit` companions; a value the source does not report is omitted together with its unit |
| `file_date` | `file_date` |

Dropped from the ETL: `observatory`, `notice_type`, `notice_type_name`, `notice_url`, `notice_data_url` — the stamped detail columns carry the enrichment instead (the notice type no longer travels: the row already says `volcano`), and the URLs are document references rather than data.

`color_code`, `alert_level`, `casualties` and `damage` are **not** dropped: they stay in `other_metrics` as the feed's own words, next to the harmonized `danger_rating` they are mapped from. The alert-level terms and the Aviation Color Code are **two independent scales** (USGS: a `NORMAL`…`WARNING` change issues a VAN, a `GREEN`…`RED` change issues a VONA), so both are read from `other_metrics` to detect an escalation, and the NCEI death toll and damage total travel with their units (`people`, `million USD`) so neither can be misread as a bare number. The volcano number and the notice id give the row a stable volcano key and a globally unique notice key.

The coordinates, region, status line **and the NCEI eruption numbers** (`vei`, `casualties`, `damage` — the maximum VEI, total deaths and total damage in millions of USD over all of the volcano's significant eruptions) arrive with every row: the GEO_Download downloader resolves the notice details through `data/usgs_volcano_notices.csv` and the eruption record through `data/usgs_volcano_ncei_eruptions.csv` at download time and stamps them into the CSVs, and it stamps the `geocode_state` column the same way (strict on fetch failure — a failed lookup raises and fails the download instead of leaving holes, successful answers are kept for every later run). This ETL passes all of it straight through — the notice type apart (dropped above): no lookup file, no API call, no driver-side map.
