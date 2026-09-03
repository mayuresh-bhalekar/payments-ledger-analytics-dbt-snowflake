# Airflow orchestration

Orchestrates the full pipeline as one DAG, `payments_ledger_analytics`
([dags/payments_ledger_dag.py](dags/payments_ledger_dag.py)):

```
dbt_deps -> dbt_seed -> dbt_run_staging -> dbt_snapshot -> dbt_run_marts -> dbt_test
```

Same order as [`.github/workflows/dbt_ci.yml`](../.github/workflows/dbt_ci.yml)
and for the same reason: `snap_merchants` snapshots a staging view
(`stg_payments__merchants`), so staging has to exist first.

**This is deployed and running**, not just documented — verified live against
`~/airflow_home` (Airflow 3.3.1, the same instance the sibling
`workday-fivetran-dbt-snowflake` repo's `workday_hr_analytics` DAG runs on) on
2026-09-03. Both the manually-triggered run and the first real scheduled run
(fired the moment the DAG was unpaused) completed all 6 tasks successfully
against the live `PAYMENTS_LEDGER` database.

## Reuses the existing Airflow image — no rebuild needed

`~/airflow_home` already runs a custom image (`workday-hr-airflow:latest`,
built by the workday repo's own `airflow/Dockerfile`) with an isolated
`dbt-snowflake` virtualenv at `/opt/dbt_venv`, extending
`apache/airflow:3.3.1`. That venv's `dbt-snowflake==1.8.4` satisfies this
project's own pin (`dbt-snowflake==1.8.*` in `.github/workflows/dbt_ci.yml`),
so this DAG runs on the same image and venv without any changes to it — only
a new bind mount and a new Connection were needed. See the workday repo's
`airflow/README.md` for why a custom image exists at all (dbt's own
dependency pins conflicting with Airflow's).

## One-time setup

All commands below assume Airflow lives at `~/airflow_home` and this repo is
checked out at
`~/claude_code_playground/payments-ledger-analytics-dbt-snowflake` (adjust if
yours differs).

### 1. Mount this repo into the Airflow containers

In `~/airflow_home/docker-compose.yaml`, `x-airflow-common.volumes`:

```yaml
  volumes:
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
    - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
    - /Users/mayureshbhalekar/claude_code_playground/workday-fivetran-dbt-snowflake:/opt/airflow/workday-fivetran-dbt-snowflake
    # add this line:
    - /Users/mayureshbhalekar/claude_code_playground/payments-ledger-analytics-dbt-snowflake:/opt/airflow/payments-ledger-analytics-dbt-snowflake
```

### 2. Symlink the DAG into Airflow's dags folder

```bash
ln -s /opt/airflow/payments-ledger-analytics-dbt-snowflake/airflow/dags/payments_ledger_dag.py \
      ~/airflow_home/dags/payments_ledger_dag.py
```

The symlink target is the **container** path (matching step 1), not a host
path — same pattern as the workday repo's own symlink; it only resolves once
mounted, which is expected.

### 3. Create a project-local `profiles.yml`

The DAG points `DBT_PROFILES_DIR` at the dbt project directory itself (inside
the container), not `~/.dbt` — so it needs its own `dbt_project/profiles.yml`
(gitignored, never committed):

```bash
cp dbt_project/profiles_example.yml dbt_project/profiles.yml
```

The `airflow` target in that file is what actually gets used; it reads
`SNOWFLAKE_AIRFLOW_USER`/`SNOWFLAKE_AIRFLOW_PASSWORD`/etc., which the DAG sets
at task-execution time from the Connection created in step 4 — no credentials
live in this file.

### 4. Create the `snowflake_payments_ledger` Airflow Connection

**Its own connection, deliberately not `snowflake_default`** — this shared
Airflow instance also runs the workday DAG, whose `snowflake_default`
connection points at `HR_ANALYTICS`. Reusing it here would silently point
this pipeline at the wrong database.

```bash
docker exec -it airflow_home-airflow-scheduler-1 airflow connections add snowflake_payments_ledger \
  --conn-type snowflake \
  --conn-login "<your Snowflake username>" \
  --conn-password "<your Snowflake password>" \
  --conn-extra '{
      "account": "VHWFFGT-SZ96994",
      "warehouse": "WH_DBT_TRANSFORM",
      "database": "PAYMENTS_LEDGER",
      "role": "DBT_TRANSFORMER",
      "schema": "STAGING"
    }'
```

`"schema": "STAGING"` is deliberate, not a placeholder default — schema
names here match exactly what `snowflake/00_setup_database_warehouse.sql`
already created, because `DBT_TRANSFORMER` has no `CREATE SCHEMA` grant (see
main README §3) and `macros/create_schema.sql` no-ops implicit schema
creation. `macros/generate_schema_name.sql` skips its dev-prefix behavior for
the `prod`, `ci`, **and** `airflow` targets for exactly this reason — the
first version of this DAG run hit `Schema 'PAYMENTS_LEDGER.STAGING_STAGING'
does not exist` before that macro fix landed.

(Run the connection command yourself — I won't type your password for you.)

### 5. (Optional) Override the dbt project path

Defaults to `/opt/airflow/payments-ledger-analytics-dbt-snowflake/dbt_project`,
matching the mount in step 1:

```bash
docker exec -it airflow_home-airflow-scheduler-1 airflow variables set dbt_project_dir \
  "/opt/airflow/payments-ledger-analytics-dbt-snowflake/dbt_project"
```

### 6. Restart

```bash
cd ~/airflow_home && docker compose up -d
```

### 7. Unpause and trigger

```bash
docker exec -it airflow_home-airflow-scheduler-1 airflow dags unpause payments_ledger_analytics
docker exec -it airflow_home-airflow-scheduler-1 airflow dags trigger payments_ledger_analytics
```

Or from the UI at `http://localhost:8080`. Unpausing alone enables the daily
`0 5 * * *` schedule — Airflow will also immediately run the most recent past
interval once unpaused (this is normal `catchup=False` behavior, not a bug).

## What's not wired in here

- **Triggering the actual settlement-file ingestion.** This DAG starts from
  `dbt seed` standing in for a real ingestion job landing rows in `RAW` (see
  main README §9 "Known simplifications"). A production version would add an
  ingestion task ahead of `dbt_deps` and swap the seeds for `source()`
  references.
- **Source freshness gating**, unlike the workday DAG's
  `check_workday_source_freshness` — there's no live, continuously-syncing
  source here to check freshness against yet.
