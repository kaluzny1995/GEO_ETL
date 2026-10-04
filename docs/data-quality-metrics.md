← [README](../README.md)

# Data Quality metrics

Every run of the [`DATA_QUALITY` job](etl_jobs/data-quality.md) stores one row per `(table_name, file_date, metric)` in `geo.data_quality`. This page documents what the metrics mean, which rules are applied and how to query the results with plain SQL.

The job itself — CLI, flow diagram, [load type detection](etl_jobs/load-types.md#data_quality), [`--overall` mode](etl_jobs/data-quality.md#overall-mode) and expected log file content — is documented under [ETL jobs](etl-jobs.md).

## Metric families

Each row of `geo.data_quality` is one metric value for one `file_date` (or for the [`overall` snapshot](etl_jobs/data-quality.md#overall-mode)):

| Metric | Meaning |
|--------|---------|
| `total_records` | Number of rows in the `file_date`. |
| `null_pct__<column>` | Share of rows where `<column>` is NULL — one metric per column. |
| `bad_pct__<rule>` | Share of rows violating a rule — one metric per rule. |
| `bad_row_pct` | Share of rows violating *any* rule. |
| `dup_row_pct` | Share of full-row duplicates inside the `file_date`. |
| `unique_row_pct` | Share of distinct rows inside the `file_date`. |
| `distinct__<column>` | Number of distinct non-NULL values — one metric per column (`file_date` excluded). |
| `min__<column>`, `max__<column>`, `range__<column>` | Observed value range of a numeric column (`range` = `max − min`). |
| `mean__<column>`, `stddev__<column>` | Mean and population standard deviation of a numeric column. |
| `p05__<column>` … `p95__<column>` | Approximate 5th/25th/50th/75th/95th percentile of a numeric column — one metric per point. |

Nulls are reported by `null_pct__*` only. A rule that cannot be evaluated because of a NULL does not count the row as incorrect, so the two metric families never double count. The range and distribution statistics simply ignore NULL values.

## Rules

### Model rules (`model_rules`)

Derived from the Pydantic model of the table:

| Rule | Condition |
|------|-----------|
| `not_blank__<column>` | Required `str` columns (other than `file_date`) must not be blank after trimming. |
| `file_date_iso` | `file_date` matches `YYYY-MM-DD`. |

Columns missing from the DataFrame are skipped, so the metric set always matches the actual table.

### Dataset rules (`dataset_rules`)

| Table | Rules |
|-------|-------|
| `eonet_event`, `gdacs_event`, `nifc_fire`, `openmeteo_flood`, `usgs_earthquake`, `usgs_nwis` | `latitude_range`, `longitude_range` |
| `avalanche_org` | `danger_level_domain`, `forecast_order` |
| `ibtracs_cyclone` | `latitude_range` (on `lat`), `longitude_range` (on `lon`), `wmo_wind_non_negative` |
| `nifc_fire` | `acres_non_negative`, `percent_contained_0_100` |
| `openmeteo_flood` | `discharge_non_negative` |
| `eonet_event` | `geometry_count_non_negative` |
| `gdacs_event` | `alert_level_domain` |
| `usgs_volcano` | *(model rules only)* |

Rules whose column is missing from the table are skipped.

`danger_level_domain` accepts the source's `-1` ("no rating") sentinel together with `1`…`5`, so a period where no ratings are published (off-season) is reported as clean instead of 100% bad rows.

## Range and distribution metrics

How much a field *varies* inside one `file_date` — its observed range, its spread and the shape of its distribution:

- **Numeric columns** — a column whose Pydantic annotation is `int`, `float` or `bool` (`Optional` unwrapped) gets `min__`, `max__`, `range__`, `mean__`, `stddev__` and the five `p05__`…`p95__` percentiles. Columns missing from the table are skipped, so the metric set always matches the actual table.
- **Every column** gets `distinct__` — the number of distinct non-NULL values, which is the direct measure of how varied a field is (a string column stuck on one value, or a numeric column collapsing to a single reading, shows up immediately). `file_date` is the grouping key and is excluded.

Notes:

- All of these are plain aggregates, so they are computed in the same single `groupBy("file_date").agg(...)` pass as the rates.
- Percentiles use Spark's `percentile_approx`: approximate, and always an *observed* value — they never interpolate between two observations.
- Statistics are computed over the non-NULL values of that `file_date`. When a statistic is undefined — `min` of a column that is entirely NULL that day — the metric is **omitted** rather than stored as NULL, because `geo.data_quality.value` is `NOT NULL`; `null_pct__<column> = 1` already tells you why. `distinct__` of an all-NULL column is stored as `0`.
- Booleans are numeric: `mean__off_season` is the share of `true` rows.

Example metric names for `avalanche_org` (numeric columns `zone_id`, `danger_level`, `off_season`): `min__danger_level`, `max__danger_level`, `range__danger_level`, `mean__danger_level`, `stddev__danger_level`, `p50__danger_level`, plus `distinct__zone_name` for the string column.

## Querying the results

Latest quality snapshot for a table:

```sql
SELECT metric, round(value::numeric, 4) AS value
FROM geo.data_quality
WHERE table_name = 'avalanche_org' AND file_date = '2026-09-20'
ORDER BY metric;
```

Whole-table snapshot (`--overall`):

```sql
SELECT metric, round(value::numeric, 4) AS value
FROM geo.data_quality
WHERE table_name = 'avalanche_org' AND file_date = 'overall'
ORDER BY metric;
```

Trend of one metric over time:

```sql
SELECT file_date, round(value::numeric, 4) AS bad_row_pct
FROM geo.data_quality
WHERE table_name = 'avalanche_org' AND metric = 'bad_row_pct'
ORDER BY file_date;
```

Tables breaching a threshold:

```sql
SELECT DISTINCT table_name
FROM geo.data_quality
WHERE metric = 'bad_row_pct' AND value > 0.05;
```

Observed range of a column over time (one row per `file_date`):

```sql
SELECT file_date,
       max(value) FILTER (WHERE metric = 'min__mag')    AS min_mag,
       max(value) FILTER (WHERE metric = 'max__mag')    AS max_mag,
       max(value) FILTER (WHERE metric = 'range__mag')  AS range_mag,
       max(value) FILTER (WHERE metric = 'mean__mag')   AS mean_mag
FROM geo.data_quality
WHERE table_name = 'usgs_earthquake'
GROUP BY file_date
ORDER BY file_date;
```

Distribution shape of a column — quantiles plus spread:

```sql
SELECT file_date,
       max(value) FILTER (WHERE metric = 'p05__mag')    AS p05,
       max(value) FILTER (WHERE metric = 'p50__mag')    AS p50,
       max(value) FILTER (WHERE metric = 'p95__mag')    AS p95,
       max(value) FILTER (WHERE metric = 'stddev__mag') AS stddev
FROM geo.data_quality
WHERE table_name = 'usgs_earthquake'
GROUP BY file_date
ORDER BY file_date;
```

Fields that stopped varying — one distinct value for the whole `file_date`:

```sql
SELECT table_name, file_date, metric, value AS distinct_values
FROM geo.data_quality
WHERE metric LIKE 'distinct__%' AND value = 1
ORDER BY table_name, file_date;
```

Cardinality drift of one field (e.g. a code column collapsing to fewer values):

```sql
SELECT file_date, value AS distinct_values
FROM geo.data_quality
WHERE table_name = 'gdacs_event' AND metric = 'distinct__alert_level'
ORDER BY file_date;
```

## Related code

| File | Role |
|------|------|
| `jobs/data_quality.py` | Load type detection, purge of removed `file_date`s, measure and replace |
| `utils/dq_metrics.py` | Rules, metric computation and unpivoting |
| `model/other/data_quality.py` | `DataQuality` model — the row layout of `geo.data_quality` |
| `utils/jdbc_retry.py` | Bounded retry for transient pooler connection failures |
| `utils/ddl_generator.py` | `execute_statement` runs the `DELETE`s over JDBC |
| `test/jobs/test_data_quality.py` | Job flow for both load types, `--overall` mode, purge, empty/no-new-data paths |
| `test/util/test_dq_metrics.py` | Metrics verified against a real local Spark session |
