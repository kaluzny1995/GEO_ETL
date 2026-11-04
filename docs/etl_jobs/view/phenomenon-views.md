← [VIEW job](../view.md)

# Phenomenon views

The denormalized per-country/region map views over `geo.union_etl`, shaped for Looker
Studio. Each shares the same 15-column display query as `phenomenons_us_vw`; the
differences are the country filters and, for the water view, the open-water heuristic.

## phenomenons_us_vw

`views/phenomenons_us_vw.py` (`datasets.view.phenomenons_us_vw`): **not** a metrics twin — a denormalized, one-row-per-snapshot view of the United States rows (`country_code = 'US'`) of `geo.union_etl`, shaped for **map visualization in Looker Studio**. Every reshape happens in the SQL query:

| Column | Content |
|--------|---------|
| `event_category`, `event_subcategory` | `initcap()`-capitalized (`earthquake` → `Earthquake`) |
| `event_ndesc` | display name: `event_name` (fallback: capitalized category) + `":\n\n" + event_description` when a description exists |
| `latitude`, `longitude`, `country_code`, `state_province`, `place_location` | map pin + tooltip fields |
| `danger_rating` | the row's categorical rating passed through (`low` / `medium` / `high` / `extreme` / `no_rating`) |
| `metric_name`, `metric_value`, `metric_unit` | the row's own metric, passed through |
| `other_metrics` | Looker-friendly text: `key: value (unit)` pairs joined by `"; "` (e.g. `danger_level: -1 (NAPADS level); off_season: true`), `*_unit` companions consumed |
| `event_start_date` | `event_start`, falling back to midnight of `file_date` when NULL |
| `event_end_date` | `event_end` as stored (NULL until an event ends) |

Live shape: 20,187 rows (earthquake 14,098, avalanche 4,426, flood 1,057, fire 452, volcano 80, storm 49, drought 25 — no US icebergs).

```bash
python run_etl.py VIEW phenomenons_us_vw
```

## phenomenons_ca_vw

`views/phenomenons_ca_vw.py` (`datasets.view.phenomenons_ca_vw`): the Canada twin — the **identical** display query as [`phenomenons_us_vw`](#phenomenons_us_vw) (same 15 columns and reshapes) with `WHERE country_code = 'CA'`. A test pins that invariant: swapping `US` → `CA` in the US query must produce the CA query exactly.

Live shape: 809 rows (flood 737, earthquake 69, storm 2, fire 1).

```bash
python run_etl.py VIEW phenomenons_ca_vw
```

## phenomenons_cs_amer_vw

`views/phenomenons_cs_amer_vw.py` (`datasets.view.phenomenons_cs_amer_vw`): the third map twin — **byte-identical** `SELECT` to [`phenomenons_us_vw`](#phenomenons_us_vw), filtered to **both Americas without the United States and Canada**:

```sql
WHERE country_code IN ('AG','AI','AR','AW','BB','BL','BM','BO','BQ','BR','BS','BV','BZ','CL','CO','CR','CU','CW',
                       'DM','DO','EC','FK','GD','GF','GL','GP','GS','GT','GY','HN','HT','JM','KN','KY','LC','MF',
                       'MQ','MS','MX','NI','PA','PE','PM','PR','PY','SR','SV','SX','TC','TT','UY','VC','VE','VG','VI')
```

55 codes: every ISO 3166-1 alpha-2 code of the Americas (UN M49 Northern + Central + South America and the Caribbean — countries *and* territories, since GDACS/seismic catalogs assign them) minus `US`/`CA`. Audited against the UN M49 Americas set (region `019`): the audit found and added `BM`, `BV`, `GS`, `KY`, `SV` — the last two carry records (8 + 3 rows), the rest are kept proactively for future data. A bare `NOT IN ('US','CA')` would wrongly include Europe, Africa, Asia and Oceania. The same tuple is exported as `COUNTRY_CODES` from the script, and tests pin: query `SELECT` == US view's, whitelist == `COUNTRY_CODES`, `US`/`CA`/`ZZ` absent, the five audited codes present.

Live shape: 4,506 rows across 29 country codes (largest: BR, MX, CO, AR, PA).

```bash
python run_etl.py VIEW phenomenons_cs_amer_vw
```

## phenomenons_ru_vw

`views/phenomenons_ru_vw.py` (`datasets.view.phenomenons_ru_vw`): the Russia twin — the **identical** display query as [`phenomenons_us_vw`](#phenomenons_us_vw) (same 15 columns and reshapes) with `WHERE country_code = 'RU'`. A test pins that invariant: swapping `US` → `RU` in the US query must produce the RU query exactly.

Live shape: 995 rows (flood 728, fire 135, earthquake 131, volcano 1).

```bash
python run_etl.py VIEW phenomenons_ru_vw
```

## phenomenons_europe_vw

`views/phenomenons_europe_vw.py` (`datasets.view.phenomenons_europe_vw`): the Europe twin — **byte-identical** `SELECT` to [`phenomenons_us_vw`](#phenomenons_us_vw), filtered to **strict UN M49 Europe without Russia**:

```sql
WHERE country_code IN ('AD','AL','AT','AX','BA','BE','BG','BY','CH','CZ','DE','DK','EE','ES','FI','FO','FR',
                       'GB','GG','GI','GR','HR','HU','IE','IM','IS','IT','JE','LI','LT','LU','LV','MC','MD',
                       'ME','MK','MT','NL','NO','PL','PT','RO','RS','SE','SI','SJ','SK','SM','UA','VA','XK')
```

51 codes: every UN M49 European state/territory/microstate (`SM`, `FO`, `XK`, `AX`, `SJ`, …) **minus `RU`**. Per the agreed definition the transcontinental M49-“Western Asia” edge cases are **excluded** — `TR`, `CY`, `AM`, `AZ`, `GE` (tests pin `RU`/`TR`/`CY`/`AM`/`AZ`/`GE`/`ZZ`/`US`/`CA` absent). Same tuple exported as `COUNTRY_CODES`.

Live shape: 2,614 rows across 22 country codes (largest: FR 431, RO 420, DE 315, ES 238).

```bash
python run_etl.py VIEW phenomenons_europe_vw
```

## phenomenons_asia_vw

`views/phenomenons_asia_vw.py` (`datasets.view.phenomenons_asia_vw`): the Asia twin — **byte-identical** `SELECT` to [`phenomenons_us_vw`](#phenomenons_us_vw), filtered to **strict UN M49 Asia without Russia**:

```sql
WHERE country_code IN ('AE','AF','AM','AZ','BD','BH','BN','BT','CN','CY','GE','HK','ID','IL','IN','IQ','IR','JO',
                       'JP','KG','KH','KP','KR','KW','KZ','LA','LB','LK','MM','MN','MO','MV','MY','NP','OM','PH',
                       'PK','PS','QA','SA','SG','SY','TH','TJ','TL','TM','TR','TW','UZ','VN','YE')
```

51 codes: all five M49 Asian sub-regions (Eastern, Southern, South-eastern, Western, Central) minus `RU`, including `HK`/`MO` micro-territories. **Complementary to the Europe view** — the whitelists are disjoint (pinned by a cross-view test): the M49-“Western Asia” edge cases excluded from Europe (`TR`, `CY`, `AM`, `AZ`, `GE`) live **here**, Oceania (`AU`, `NZ`, `PG`) and `ZZ` do not.

Live shape: 4,601 rows across 33 country codes (flood 2,499, fire 1,172, earthquake 803, drought 80, storm 40, volcano 7).

```bash
python run_etl.py VIEW phenomenons_asia_vw
```

## phenomenons_africa_vw

`views/phenomenons_africa_vw.py` (`datasets.view.phenomenons_africa_vw`): the Africa twin — **byte-identical** `SELECT` to [`phenomenons_us_vw`](#phenomenons_us_vw), filtered to **strict UN M49 Africa** (no exclusions):

```sql
WHERE country_code IN ('AO','BF','BI','BJ','BW','CD','CF','CG','CI','CM','CV','DJ','DZ','EG','EH','ER','ET','GA',
                       'GH','GM','GN','GQ','GW','IO','KE','KM','LR','LS','LY','MA','MG','ML','MR','MU','MW','MZ',
                       'NA','NE','NG','RE','RW','SC','SD','SH','SL','SN','SO','SS','ST','SZ','TD','TF','TG','TN',
                       'TZ','UG','YT','ZA','ZM','ZW')
```

60 codes: the **54 AU member states** (pinned by test) plus M49 African territories `EH`, `IO`, `RE`, `YT`, `SH`, `TF`. **Disjoint from the Europe and Asia whitelists** (pinned): Egypt/Sudan are M49 Northern Africa (→ here), the Western-Asia edge cases stay with Asia, Oceania and `ZZ` are excluded. Caveat: island territories coded to their parent country (`ES` Canary Islands, `FR` Réunion is `RE` here but Guadeloupe is `GP` → cs_amer) follow `country_code`.

Live shape: 7,968 rows across 34 country codes (fire 6,174, flood 1,623, drought 147, earthquake 22, storm 2).

```bash
python run_etl.py VIEW phenomenons_africa_vw
```

## phenomenons_aus_oc_vw

`views/phenomenons_aus_oc_vw.py` (`datasets.view.phenomenons_aus_oc_vw`): the Australia/Oceania twin — **byte-identical** `SELECT` to [`phenomenons_us_vw`](#phenomenons_us_vw), filtered to **strict UN M49 Oceania** (all four sub-regions):

```sql
WHERE country_code IN ('AS','AU','CC','CK','CX','FJ','FM','GU','HM','KI','MH','MP','NC','NF','NR','NU','NZ','PF',
                       'PG','PN','PW','SB','TK','TO','TV','UM','VU','WF','WS')
```

29 codes: **Australia & New Zealand** (`AU CC CX HM NF NZ`), **Melanesia** (`FJ NC PG SB VU`), **Micronesia** (`FM GU KI MH MP NR PW UM`), **Polynesia** (`AS CK NU PF PN TK TO TV WF WS`) — all four pinned by test. Disjoint from the Europe/Asia/Africa whitelists (pinned): Indian-Ocean territories (`RE`/`YT`/`SH`/`IO`) stay with Africa, `TL` with Asia, `AQ`/`GS` belong to no region view.

Completeness (audited against the UN M49/ISO-3166 dataset): every Oceania code is present **even with zero records today** (e.g. `HM` Heard & McDonald Islands) so new events are picked up automatically.

Live shape: 3,890 rows across 11 country codes (fire 2,504, earthquake 525, flood 832, drought 16, storm 13) — AU 2,601, PG 869, VU 138, NC 88, TO 64.

```bash
python run_etl.py VIEW phenomenons_aus_oc_vw
```

## phenomenons_waters_vw

`views/phenomenons_waters_vw.py` (`datasets.view.phenomenons_waters_vw`): the **water** twin — **byte-identical** `SELECT` to [`phenomenons_us_vw`](#phenomenons_us_vw), filtered to events **on oceans and seas**. With no PostGIS in the database, water-ness is a pure-SQL heuristic (chosen over the extension/polygon alternative):

```sql
WHERE latitude IS NOT NULL AND longitude IS NOT NULL          -- geocode guard
  AND ( country_code = 'ZZ'                                   -- provider's no-country/open-water marker
     OR country_code = 'AQ'                                   -- Antarctica: no continent view -> water/ice domain
     OR event_subcategory = 'sea_lake_ice'                    -- iceberg & sea-ice events
     OR (coalesce(event_name,'') || ' ' || coalesce(place_location,'') || ' ' || coalesce(event_description,''))
        ~* '\y(ocean|sea|gulf|strait|offshore|coast)\y|off the coast|open water'
  )
```

`bay`/`maritime` were dropped after auditing case-insensitive matches: they hit land places (`Pedro Bay`, `Cruz Bay`, Alaska's `Maritime` region) — pinned by test. `AQ` routes *every* Antarctic event here so no `country_code` goes uncovered (a coverage audit against all 135 codes in `union_etl` showed only `ZZ`, `AQ`, `GS`, `SV` outside the region whitelists; `GS`/`SV` moved to `cs_amer`).

Live shape: 2,392 rows (storm 1,774, earthquake 557, iceberg 61). **Known trade-off:** water events carrying a *real* country code with no maritime wording (e.g. `114 km E of ..., Japan` offshore quakes) are missed — that would need `CREATE EXTENSION postgis` + a land-polygon table (`ST_Intersects(land, ST_MakePoint(lon,lat))`), deliberately avoided for now; a few land places named after water (`Ocean City`, `Sea Ranch`) still match.

```bash
python run_etl.py VIEW phenomenons_waters_vw
```
