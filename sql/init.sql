-- =====================================================================
-- ETL pipeline - DB initialisation
-- Schemas: raw_schema (CSV source) -> staging_schema (target)
-- =====================================================================

-- Schemas
CREATE SCHEMA IF NOT EXISTS raw_schema;
CREATE SCHEMA IF NOT EXISTS staging_schema;

-- ============================================================
-- raw_schema.users
-- Populated by: CSV → DB pipeline
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_schema.users (
    user_id         INTEGER         PRIMARY KEY,
    email           TEXT            NOT NULL,
    first_name      TEXT            NOT NULL,
    last_name       TEXT            NOT NULL,
    avatar          TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ============================================================
-- staging_schema.users
-- Populated by: API → DB  and  DB → DB pipelines
-- ============================================================

CREATE TABLE IF NOT EXISTS staging_schema.users (
    user_id         INTEGER         PRIMARY KEY,
    email           TEXT            NOT NULL,
    first_name      TEXT            NOT NULL,
    last_name       TEXT            NOT NULL,
    avatar          TEXT,
    loaded_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ============================================================
-- etl_watermarks
-- Tracks last successful run per pipeline for incremental loads
-- ============================================================

CREATE TABLE IF NOT EXISTS public.etl_watermarks (
    pipeline        TEXT            PRIMARY KEY,
    last_run_at     TIMESTAMPTZ     NOT NULL DEFAULT '1970-01-01T00:00:00Z'
);

-- Seed watermark rows so UPDATE always finds a record
INSERT INTO public.etl_watermarks (pipeline) VALUES
    ('api'),
    ('csv'),
    ('db')
ON CONFLICT (pipeline) DO NOTHING;