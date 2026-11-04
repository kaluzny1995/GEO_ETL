← [Configuration](../configuration.md)

# Supabase Credentials

> **Do the Supabase setup in GEO_E2E first.** The five values below are shared by the whole pipeline — GEO_E2E uses them for Airflow's metadata DB, this project for its JDBC writes — and their setup lives in **GEO_E2E**: run [`../GEO_E2E/setup.sh`](../../../GEO_E2E/README.md#1-configure-credentials--mandatory-before-docker-compose-up) (or fill `GEO_E2E/.env` — full walkthrough in [GEO_E2E — Supabase metadata DB](../../../GEO_E2E/docs/configuration.md#supabase-metadata-db-env)) and the script mirrors them into this project's gitignored `.env`. What follows documents what the variables mean, how the JDBC URL is built and how to rotate or scope the credentials — keep it for standalone runs, CI and troubleshooting.

Supabase database credentials are read from **environment variables**. A `.env` file in the project root is loaded automatically; `.env` is gitignored and only the `.env.example` template is committed. Variables already present in the shell (or CI) environment take precedence over `.env`.

| Variable | Example | Description |
|----------|---------|-------------|
| `SUPABASE_DB_USER` | `postgres.abcdefghijklmno` | Database user — `postgres.` followed by the project ref |
| `SUPABASE_DB_PASSWORD` | — | Database password (see [How to obtain the credentials](#how-to-obtain-the-credentials)) |
| `SUPABASE_DB_HOST` | `aws-1-eu-west-1.pooler.supabase.com` | Transaction-pooler hostname |
| `SUPABASE_DB_PORT` | `6543` | Transaction-pooler port |
| `SUPABASE_DB_NAME` | `postgres` | Database name |

Setup (normally already done by GEO_E2E — the manual fallback for standalone runs):

```bash
cd ../GEO_E2E && ./setup.sh   # writes this project's .env with the SUPABASE_DB_* values
# or, by hand:
cp .env.example .env          # then edit .env with your values
```

> **Note:** `6543` is Supabase's **transaction pooler** port (the direct primary listener uses `5432`; session mode on `6543` was deprecated in February 2025); the recommended value is `6543`, and every JDBC URL in this documentation uses the same value.

`SupabaseConnectionConfig` (`model/config/supabase_config.py`) reads these variables via `from_env()` and builds the JDBC URL and connection properties used by every upload, transform and DDL execution:

```
jdbc:postgresql://aws-1-eu-west-1.pooler.supabase.com:6543/postgres
```

The properties also carry `prepareThreshold=0`, which disables server-side prepared statements. Supabase's pooler shares server connections between clients, so named statements (`S_1`, …) can otherwise be prepared twice and the write fails with `prepared statement "S_1" already exists`.

See [Data tables](../data-tables.md) for the tables it points at and [Usage — Verify Supabase connectivity](../usage/maintenance.md#verify-supabase-connectivity) for a connectivity test.

## How to obtain the credentials

1. Open the [Supabase dashboard](https://supabase.com/dashboard) and select your project.
2. Click **Connect** (top right). Under *Connect to your database*, open the **Connection pooling** → **Transaction pooler** section (port `6543`) and copy the **Host**, **Port**, **Database** (`postgres`) and **User** (`postgres.<project-ref>`).
3. The **password is never shown** in the dashboard. If you saved it when the project was created, use that. If not, go to **Project Settings → Database** and click **Generate password** (formerly *Reset database password*) — this immediately invalidates the old password, so every client still using it must be updated.

## Migrating from supabase_config.json

Earlier versions kept the same values in a gitignored `supabase_config.json`. Convert an existing file once, then delete it:

```bash
python -c "import json; c = json.load(open('supabase_config.json')); print('\n'.join(f'SUPABASE_DB_{k}={v}' for k, v in [('USER', c['username']), ('PASSWORD', c['password']), ('HOST', c['host']), ('PORT', c['port']), ('NAME', c['database'])]))" > .env
rm supabase_config.json
```

## Can the credentials be obtained automatically?

Supabase deliberately never returns the database password through its API — it can only be **set**, never read back. The only programmatic path is therefore *rotation*:

1. Create a Personal Access Token (PAT) at <https://supabase.com/dashboard/account/tokens> (fine-grained tokens need the `database_config_write` scope).
2. Generate a strong password and push it to the project:

```bash
NEW_PASSWORD="$(openssl rand -base64 24)"
curl -X PATCH "https://api.supabase.com/v1/projects/<PROJECT_REF>/database/password" \
  -H "Authorization: Bearer $SUPABASE_PAT" \
  -H "Content-Type: application/json" \
  -d "{\"password\": \"$NEW_PASSWORD\"}"
```

3. Export the same value as `SUPABASE_DB_PASSWORD` (shell profile, CI secret, secrets manager). Note that the rotation immediately invalidates the previous password — on first provisioning this doubles as the bootstrap: rotate, capture `$NEW_PASSWORD` in the same step, and you never need the dashboard.

Treat the PAT as a secret of equal weight to the database password. In CI, store `SUPABASE_DB_USER` / `SUPABASE_DB_PASSWORD` (and the rest) as repository or organization secrets and export them before running jobs — no `.env` file is needed there.

## Least-privilege role (recommended)

The `postgres.<project-ref>` user is a superuser. For a pipeline that only reads and writes the `geo` schema, create a dedicated role instead (run once in the SQL editor or via `psql`):

```sql
CREATE ROLE geo_etl LOGIN PASSWORD '<strong-password>';
GRANT USAGE ON SCHEMA geo TO geo_etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA geo TO geo_etl;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA geo TO geo_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA geo GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO geo_etl;
```

Then set `SUPABASE_DB_USER=geo_etl` and `SUPABASE_DB_PASSWORD=<strong-password>` in `.env`. The role can be dropped and recreated with a new password at any time without touching the superuser credential.

