-- ============================================================================
-- 00_setup_database_warehouse.sql
-- Run as a role with CREATE DATABASE / CREATE WAREHOUSE / CREATE ROLE privileges
-- (e.g. ACCOUNTADMIN on a trial account, or SYSADMIN + SECURITYADMIN in prod)
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Roles (least-privilege separation between ingestion, transformation, BI)
-- ---------------------------------------------------------------------------
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS PAYMENTS_LOADER;   -- ingestion service role: write RAW only
CREATE ROLE IF NOT EXISTS DBT_TRANSFORMER;   -- dbt service role: read RAW, write STAGING/MARTS
CREATE ROLE IF NOT EXISTS BI_READER;         -- Metabase / analyst role: read MARTS_FINANCE only
CREATE ROLE IF NOT EXISTS FINANCE_ADMIN;     -- Finance leadership: read all marts, unmasked PII

USE ROLE SYSADMIN;

-- ---------------------------------------------------------------------------
-- Warehouses (workload isolation for clean cost attribution + no contention)
-- ---------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS WH_PAYMENTS_LOAD
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Settlement-file ingestion into RAW. Small + fast-suspend: short bursty loads.';

CREATE WAREHOUSE IF NOT EXISTS WH_DBT_TRANSFORM
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 3
  SCALING_POLICY = 'STANDARD'
  COMMENT = 'Scheduled dbt build/test runs. Auto-scale for parallel model execution.';

CREATE WAREHOUSE IF NOT EXISTS WH_BI_QUERY
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 4
  SCALING_POLICY = 'ECONOMY'
  COMMENT = 'Metabase dashboards + Finance ad-hoc queries. Longer suspend to preserve dashboard cache hits.';

-- ---------------------------------------------------------------------------
-- Database + schemas
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS PAYMENTS_LEDGER
  COMMENT = 'Payment-processor settlement -> ledger -> financial semantic layer platform';

USE DATABASE PAYMENTS_LEDGER;

CREATE SCHEMA IF NOT EXISTS RAW
  COMMENT = 'Ingestion-owned. 1:1 replica of processor settlement/dispute exports. Append-only, never hand-edited.';

CREATE SCHEMA IF NOT EXISTS STAGING
  COMMENT = 'dbt staging layer: stg_ views, typed/renamed/masked, no business logic.';

CREATE SCHEMA IF NOT EXISTS SNAPSHOTS
  COMMENT = 'dbt snapshots: SCD2 history on slowly-changing entities (merchant status).';

CREATE SCHEMA IF NOT EXISTS INTERMEDIATE
  COMMENT = 'dbt intermediate layer: transaction/settlement/FX joins, not queried directly.';

CREATE SCHEMA IF NOT EXISTS MARTS_CORE
  COMMENT = 'Shared dimensions: merchant, processor, date.';

CREATE SCHEMA IF NOT EXISTS MARTS_LEDGER
  COMMENT = 'The financial ledger: revenue recognition, FX gain/loss, bad-debt write-offs.';

CREATE SCHEMA IF NOT EXISTS MARTS_FINANCE
  COMMENT = 'The financial semantic layer + merchant metrics — what Metabase and Finance query.';
