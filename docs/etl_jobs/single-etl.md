← [ETL jobs](../etl-jobs.md)

# SINGLE_ETL job

`jobs/single_etl.py` runs **one standalone PySpark script** from `single_etl/` — a complete read → transform → write pipeline for a single dataset, independent of the CREATE/INGESTION table pairs:

```mermaid
flowchart TD
    CLI["CLI<br/>python run_etl.py SINGLE_ETL {script}"] -->|JobFactory, script name| JOB["jobs/single_etl.py"]
    JOB --> CFG["AppConfig + SupabaseConnectionConfig<br/>build jdbc_url / properties"]
    JOB --> FACT["SingleEtlFactory.get<br/>registry in jobs/__init__.py"]
    CFG --> SCRIPT["single_etl/{script}.py<br/>run(spark, config, db_config, load_type, logger)"]
    FACT --> SCRIPT
    SCRIPT --> MAX["MAX(file_date) in the target table<br/>get_max_file_date"]
    MAX --> LT{"Load type<br/>(--load-type forces one)"}
    LT -->|"target empty or missing"| H["HISTORICAL<br/>every source row"]
    LT -->|"target has data"| I["INCREMENTAL<br/>file_date newer than the stored maximum"]
    H --> READ["spark.read.jdbc<br/>geo.{source}"]
    I --> READ
    READ --> FILTER["keep file_date newer than the maximum<br/>(incremental only)"]
    FILTER --> EMPTY{"any rows?"}
    EMPTY -->|"no"| SKIP["log warning → return 'nothing to do'<br/>run_etl.py exits 0"]
    EMPTY -->|"yes"| CV["_convert()<br/>map the source columns onto the<br/>23-column canonical structure"]
    CV --> WRITE["write.jdbc append"]
    WRITE --> DB[("geo.{source}_etl")]
    WRITE --> OK["return 'HISTORICAL / INCREMENTAL:<br/>wrote to geo.{source}_etl'"]
```

How the data flows:

1. `run_etl.py` resolves `JobType.SINGLE_ETL` → `jobs/single_etl.py` and passes the second positional argument as the **script name** — the file stem of a module in `single_etl/` (e.g. `avalanche_org_etl`).
2. `SingleEtlFactory` (`factory/single_etl_factory.py`) looks that name up in its registry; scripts are registered by `jobs/__init__.py`.
3. The job loads both configs and calls the script's `run(spark, config, db_config, load_type=..., logger=...)`.
4. The script resolves its [load type](load-types.md), reads its source table, applies its own `_convert()` transformation and appends the result to its target table.

```bash
python run_etl.py SINGLE_ETL avalanche_org_etl                    # auto load type
python run_etl.py SINGLE_ETL avalanche_org_etl --load-type historical
python run_etl.py SINGLE_ETL avalanche_org_etl --load-type incremental
```

`--load-type` accepts `auto` (default), `historical` or `incremental` and is rejected on any other job; the logs go to `logs/single_etl/single_etl_<script>_<timestamp>.log`. A run that finds nothing new warns and still exits `0` — the DATA_QUALITY behaviour, not the INGESTION one.

## avalanche_org_etl

`single_etl/avalanche_org_etl.py` is the reference script: `geo.avalanche_org` (one row per forecast zone per day) → `geo.avalanche_org_etl` (the 23-column structure from `model/etl/event_etl.py`).

| Target column | Source |
|---------------|--------|
| `subsource_name` | `center_name` (issuing center) |
| `place_location` | `zone_name`; `event_name` stays `NULL` — a forecast has no event name |
| `latitude` / `longitude` | polygon centroid of the zone from `cache/avalanche_zone_centroids.csv`, keyed `place_location` + `state_province` |
| `metric_name` | always `magnitude`; `metric_value` / `metric_unit` stay `NULL` — a danger level is a rating, not a measurement |
| `danger_rating` | zone rating as text, `NULL` while unrated (`danger_level = -1`, `danger_rating = "no rating"`) |
| `statement` | `travel_advice`, with `warning` appended when present |
| `event_start_date` | `date` (the forecast's calendar day) |
| `event_start` / `event_end` | `forecast_start` / `forecast_end` |
| `other_metrics` | `NULL` (zone ids and links are not metrics) |

When `cache/avalanche_zone_centroids.csv` is missing, the ETL builds it itself through `utils/fetch_avalanche_zones.py` — fetching today plus 10 and 30 days back, so zones that have left the avalanche.org feed stay covered — and only fails when that build fails (no network). To refresh the lookup or to pick the days yourself, run `python -m utils.fetch_avalanche_zones --day <day> [--day <day> …]` and rerun it whenever a zone is added or renamed. The CSV is parsed on the driver and embedded as literal columns, because the Spark containers only mount `GEO_Download/data`.

## Adding a new script

1. Copy `single_etl/avalanche_org_etl.py` to `single_etl/<name>_etl.py` and rewrite `SOURCE_TABLE`, `TARGET_TABLE` and `_convert()` for the new source (the module docstring walks through it).
2. Register it in `jobs/__init__.py`:

```python
from single_etl.<name>_etl import run as <name>_etl_run

SingleEtlFactory.register("<name>_etl", <name>_etl_run)
```

3. Run it: `python run_etl.py SINGLE_ETL <name>_etl`.

