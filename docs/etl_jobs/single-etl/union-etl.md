← [SINGLE_ETL job](../single-etl.md)

# union_etl

`single_etl/union_etl.py`: every source `*_etl` table → `geo.union_etl` (the canonical 21 columns, one row appended for every row the source scripts produced).

The nine tables to stack and the target come from `datasets.single_etl.union_etl` in `config.json`:

```json
"union_etl": {
  "target_table": "union_etl",
  "source_tables": [
    "avalanche_org_etl", "eonet_event_etl", "gdacs_event_etl",
    "ibtracs_cyclone_etl", "nifc_fire_etl", "openmeteo_flood_etl",
    "usgs_earthquake_etl", "usgs_nwis_etl", "usgs_volcano_etl"
  ]
}
```

`SOURCE_TABLE` is that tuple and the shared runner stacks the frames with `unionByName` before `_convert()` sees them — so the script's `_convert()` is the **identity**: nothing is remapped, merged or re-stamped, the rows already carry the structure of `model/etl/event_etl.py`, and `DEDUP_KEYS` is `None` — the stacked rows were deduplicated by their own scripts, so the runner skips the [deduplication step](../single-etl.md#deduplication) for the union. What distinguishes a row is what its own script put in it:

| Column | In the union table |
|--------|--------------------|
| `source_name` | which of the nine tables the row came from — one distinct label per table (`AVALANCHE_ORG`, `EONET`, `GDACS`, `IBTRACS`, `NIFC`, `OPENMETEO`, `USGS_EARTHQUAKE`, `USGS_NWIS`, `USGS_VOLCANO`); this is also the per-source incremental key |
| `event_category`, `metric_name` | the source's own labels — kept per source, not harmonised |
| `event_subcategory` | the source's own subtype where it reports one (`hurricane`, `wildfire`, `agricultural drought`, …), otherwise the shared default of the category each script writes (`avalanche`, `other` for an untyped earthquake, `fire`, `water discharge`, `storm`, `volcano`) — no source leaves the column `NULL` for one of the six categories |
| `metric_unit` | the canonical unit of the metric where one exists (`wind speed` → `km/h`, `area` → `km²`, `discharge` → `m³/s`, applied by each source's `_convert`); the earthquake `magnitude` unified as `Mw` — the moment family (`mw`/`mww`/`mwr`, GDACS `M`) relabelled and `ml`/`md`/`mb`/`mb_lg` converted to `Mw*` first, with USGS OF 1997/464's rule; otherwise the source's own label (`VEI`, the avalanche size class, `mh`, `mun`, …) |
| `danger_rating`, `other_metrics` | the shared severity scale every source is mapped onto (`no_rating` … `extreme`) and the source's own JSON metrics (units included) |
| the remaining columns | unchanged from the source row |

Load types behave as in the [table above](../single-etl.md#single_etl-job): the target's latest `file_date` decides between `HISTORICAL` and `INCREMENTAL`, but the `INCREMENTAL` filter is **per source table** — each of the nine sources is filtered with its own `file_date > <latest file_date among the target rows carrying that source's own source_name>`. The labels are unique per table (the three USGS tables used to share `USGS`; they are now `USGS_EARTHQUAKE`, `USGS_NWIS` and `USGS_VOLCANO`), which is what makes the per-source threshold exact. A source that catches up after an outage therefore still delivers its older files, and a source with no rows in the union yet is stacked in full. When no source row is newer the run loads nothing:

```
WARNING No records loaded into union_etl — no rows to process in 9 source tables
ERROR ETL job failed   (RuntimeError)   → exit code 1
```

> **Note:** every `*_etl` table must stamp a **distinct** `source_name` — the union's per-source incremental threshold is keyed on it (the three USGS tables were renamed from the shared `USGS` to `USGS_EARTHQUAKE`, `USGS_NWIS` and `USGS_VOLCANO`). A `union_etl` that predates that rename has to be rebuilt once: `python run_drop_table.py union_etl`, `python run_etl.py CREATE union_etl`, then one `SINGLE_ETL union_etl` run (it auto-detects `HISTORICAL` and rebuilds the table from the current `*_etl` tables).

```bash
python run_etl.py CREATE <table_name>                      # once — builds the target table
python run_etl.py SINGLE_ETL <table_name>                  # auto load type
python run_etl.py SINGLE_ETL <table_name> --load-type historical
python run_etl.py SINGLE_ETL <table_name> --load-type incremental
```
