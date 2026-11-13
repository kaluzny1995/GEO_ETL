← [VIEW job](../view.md)

# Metrics views

One `metrics_*` view per `event_category` of `geo.union_etl` (all eight categories),
plus the shared `metrics_observed_vw` they all start from. The eight twins follow the
same shape — own measured metric + numeric `other_metrics` explosion + trailing
`file_date`, then MLlib predictions for missing dates — with the per-view differences
documented below.

Every metrics row carries `is_predicted`:

- `FALSE` — an observed value from `geo.union_etl`.
- `TRUE` — a PySpark MLlib estimate from `geo.prediction_etl` for a date where that
  metric group has no observation. The predicted row has
  `event_subcategory = NULL` and its `metric_value` is the model estimate.

The views use `UNION ALL`, not a join: event rows are never multiplied, and
observed-only consumers (counts, quality metrics, maps) keep reading
`geo.union_etl`. See [PREDICTION job](../prediction.md).

## metrics_observed_vw

`views/metrics_observed_vw.py` (`datasets.view.metrics_observed_vw`): the shared
observed-metric extraction for all eight categories. It applies the category
whitelist plus the per-category filters that used to live in each view:

- earthquake: `metric_unit NOT IN ('mh', 'Mi', 'mun', 'ms_vx')` — only `Mw` survives;
- flood: `metric_name IN ('discharge', 'threshold', 'baseline_days', 'exceedance_ratio', 'magnitude')`.

It selects the row's own measured metric and the numeric `other_metrics` entries
(with companion `<name>_unit` units) and emits `FALSE AS is_predicted` in both
branches. The PREDICTION job reads this view; the eight combined views filter it
by `event_category`.

```bash
python run_etl.py VIEW metrics_observed_vw
```

## metrics_avalanche_vw

`views/metrics_avalanche_vw.py` (`datasets.view.metrics_avalanche_vw`): every avalanche-category metric of `geo.union_etl` flattened into one row per (name, value, unit) triple:

| Column | Content |
|--------|---------|
| `event_category`, `event_subcategory` | carried over from the source row (the category is always `avalanche`); predicted rows have a NULL subcategory |
| `metric_name`, `metric_value`, `metric_unit` | the row's own canonical metric — kept only when it carries a value (bare names with NULL value/unit, such as `size`, are skipped) — and, **appended** as extra rows, one row per numeric entry of its `other_metrics` JSON, with the unit taken from the companion `<name>_unit` key; predicted rows replace `metric_value` with `predicted_value` |
| `file_date` | the source row's `YYYY-MM-DD` snapshot stamp, cast to `timestamp` |
| `is_predicted` | `FALSE` for observed rows, `TRUE` for rows read from `geo.prediction_etl` |

So a zone publishing `metric_name = 'size'` with no value alongside `{"danger_level": 3, "danger_level_unit": "NAPADS level", "off_season": false}` yields one row: `danger_level` / `3` / `NAPADS level` (the unmeasured `size` name is dropped). In today's data every avalanche row's own metric is exactly such a bare `size` name, so the view currently carries the exploded `other_metrics` rows only — 4,426 `danger_level` rows. Companion `*_unit` keys are units, not metrics, and non-numeric entries (free text / booleans such as `warning`, `off_season`) carry no measurement and are skipped — the explosion happens in the database (`jsonb_each` + `jsonb_typeof(...) = 'number'` inside the configured query).

```bash
python run_etl.py VIEW metrics_avalanche_vw
```

## metrics_drought_vw

`views/metrics_drought_vw.py` (`datasets.view.metrics_drought_vw`): the drought twin of the avalanche view — the same query shape, filtered to `event_category = 'drought'` and reading `geo.union_etl`. Every drought row keeps its measured own metric (GDACS publishes `area` / `km²`), `event_category` / `event_subcategory` (e.g. `agricultural drought`), and trailing `file_date` (`timestamp`), with the numeric `other_metrics` explosion appended when present.

GDACS currently writes `other_metrics = NULL` for drought rows, so the explosion branch yields no rows today — it is kept so the view behaves identically if other sources start publishing drought rows with `other_metrics`. Live shape: 403 rows, one per drought event/date, `area` in `km²` over `2026-09-01` → `2026-10-20`.

```bash
python run_etl.py VIEW metrics_drought_vw
```

## metrics_earthquake_vw

`views/metrics_earthquake_vw.py` (`datasets.view.metrics_earthquake_vw`): the earthquake twin of the avalanche view — same query shape, filtered to `event_category = 'earthquake'` over `geo.union_etl`. Unlike drought, its `other_metrics` explosion is fully active: USGS rows carry numeric `depth` / `km` and `nst` / `stations` entries there (NULL `other_metrics` on ~1.5k rows contribute only their own metric). The rare magnitude scales `mh`, `Mi`, `mun` and `ms_vx` (1-14 records each) are excluded from both branches via `metric_unit NOT IN (...)` — only `Mw` remains.

Live shape: 47,698 rows — 16,877 `magnitude` / `Mw` plus 15,411 `depth` / `km` and 15,410 `nst` / `stations` from the explosion; every row measured, every unit populated.

```bash
python run_etl.py VIEW metrics_earthquake_vw
```

## metrics_fire_vw

`views/metrics_fire_vw.py` (`datasets.view.metrics_fire_vw`): the fire twin of the avalanche view — same query shape, filtered to `event_category = 'fire'` over `geo.union_etl`. The `other_metrics` explosion mixes two sources: NIFC contributes `percent_contained` / `%` (its `_other_metrics()` always writes the companion), and EONET fire rows contribute `geometry_count` (the event's geometry points — a count with **no companion unit**, so those rows carry `metric_unit = NULL`; drop them with a `metric_unit IS NOT NULL` predicate if unwanted).

Live shape: 12,374 rows — 11,942 own `area` / `km²` rows (all fire sources publish it), 343 `percent_contained` / `%` and 89 `geometry_count` / NULL from the explosion.

```bash
python run_etl.py VIEW metrics_fire_vw
```

## metrics_flood_vw

`views/metrics_flood_vw.py` (`datasets.view.metrics_flood_vw`): the flood twin of the avalanche view — same query shape, filtered to `event_category = 'flood'` over `geo.union_etl`. Both branches carry a five-metric whitelist (`metric_name IN ('discharge', 'threshold', 'baseline_days', 'exceedance_ratio', 'magnitude')`), so only those metrics ever reach the view: the OPENMETEO percentile/ensemble block and the USGS_NWIS stage metrics found in its rich `other_metrics` JSON are filtered out, as are any future keys. Units come from the companion `<name>_unit` keys (`discharge` and `threshold` in `m³/s`); `baseline_days`, `exceedance_ratio` and GDACS's own `magnitude` land with `metric_unit = NULL`. Non-numeric entries (`threshold_exceeded`, dates, gauge ids, `exceedance_level`) are skipped by the numeric filter.

Live shape: 45,589 rows — 11,635 `discharge` / `m³/s` + 570 `magnitude` / NULL own metrics, and 11,544 `threshold` / `m³/s`, 11,544 `baseline_days` / NULL, 10,296 `exceedance_ratio` / NULL from the explosion.

```bash
python run_etl.py VIEW metrics_flood_vw
```

## metrics_iceberg_vw

`views/metrics_iceberg_vw.py` (`datasets.view.metrics_iceberg_vw`): the iceberg twin of the avalanche view — same query shape, filtered to `event_category = 'iceberg'` over `geo.union_etl`. All 61 rows are EONET `sea_lake_ice` events: the own metric is `surface` / `N/m²`, and every row's `other_metrics` holds `geometry_count` (the event's geometry points — a count with no companion unit, so those rows carry `metric_unit = NULL`, as with fire).

Live shape: 122 rows — 61 `surface` / `N/m²` own metrics + 61 `geometry_count` / NULL from the explosion.

```bash
python run_etl.py VIEW metrics_iceberg_vw
```

## metrics_storm_vw

`views/metrics_storm_vw.py` (`datasets.view.metrics_storm_vw`): the storm twin of the avalanche view — same query shape, filtered to `event_category = 'storm'` over `geo.union_etl`. Every source publishes the own metric `wind speed` / `km/h` (IBTRACS 1,593, GDACS 209, EONET 123; GDACS rows have NULL `other_metrics` and contribute only it). The explosion adds the IBTRACS block — `pressure` / `mb`, `speed` / `km/h`, `direction` / `degrees`, `gust` / `km/h` (444 rows) and `dist2land` / `landfall` / `km` — plus EONET's unit-less `geometry_count` (NULL unit). The `saffir_simpson` class is a string and is skipped by the numeric filter.

Live shape: 10,372 rows — 1,925 own `wind speed` rows + 8,447 exploded rows across 7 metric names.

```bash
python run_etl.py VIEW metrics_storm_vw
```

## metrics_volcano_vw

`views/metrics_volcano_vw.py` (`datasets.view.metrics_volcano_vw`): the volcano twin of the avalanche view — same query shape, filtered to `event_category = 'volcano'` over `geo.union_etl`. Own metrics: USGS's `vei` / `VEI` (80 rows) and GDACS's `magnitude` / NULL (8 rows, NULL `other_metrics`). The explosion adds USGS notice numbers `vnum` / `Smithsonian GVP` (a numeric identifier; the PREDICTION job now fills its missing dates too) and, on damage-affected volcanoes, `casualties` / `people` and `damage` / `million USD` (21 rows each). `color_code`, `alert_level` and `notice_identifier` are strings and are skipped by the numeric filter.

Live shape: 210 rows — 88 own metrics + 80 `vnum` + 21 `casualties` + 21 `damage`.

```bash
python run_etl.py VIEW metrics_volcano_vw
```
