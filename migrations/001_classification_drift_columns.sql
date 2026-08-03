-- Plan #8 Phase 2 — classification/attribution drift columns on the
-- Spark-owned aiqg.event_metrics hypertable (TimescaleDB, tas_events db).
--
-- Additive + nullable ONLY (mirrors the prior agent/flow attribution ALTER
-- pattern). The aggregator writes these via INSERT ... ON CONFLICT DO NOTHING;
-- older columns are untouched, so this is safe to apply before OR after the
-- aggregator image ships (missing columns just mean the new SELECT fails until
-- applied — apply this FIRST). aiqg-dashboard-be reads them TS-first with a
-- Loki fallback, so the dashboard works before this runs.
--
-- Apply out-of-band (the hypertable is not created by this repo):
--   psql "$JDBC_URL_equivalent" -f migrations/001_classification_drift_columns.sql
--
-- Contract: aether-shared/data-models/aiqg/classification-drift.md §3/§4.

-- Classification drift (Axis-1) — workflow declared-vs-inferred.
ALTER TABLE aiqg.event_metrics ADD COLUMN IF NOT EXISTS workflow_declared     TEXT;
ALTER TABLE aiqg.event_metrics ADD COLUMN IF NOT EXISTS workflow_declared_op  TEXT;
ALTER TABLE aiqg.event_metrics ADD COLUMN IF NOT EXISTS workflow_inferred     TEXT;
ALTER TABLE aiqg.event_metrics ADD COLUMN IF NOT EXISTS workflow_drift        BOOLEAN;
ALTER TABLE aiqg.event_metrics ADD COLUMN IF NOT EXISTS otel_map_version      TEXT;

-- Attribution drift (Axis-1) — agent declared-vs-inferred.
ALTER TABLE aiqg.event_metrics ADD COLUMN IF NOT EXISTS agent_declared        TEXT;
ALTER TABLE aiqg.event_metrics ADD COLUMN IF NOT EXISTS agent_inferred        TEXT;
ALTER TABLE aiqg.event_metrics ADD COLUMN IF NOT EXISTS agent_drift           BOOLEAN;
ALTER TABLE aiqg.event_metrics ADD COLUMN IF NOT EXISTS drift_source          TEXT;

-- Partial index to keep the drift-rate query cheap (only events with a declared
-- signal participate in the rate denominator).
CREATE INDEX IF NOT EXISTS event_metrics_workflow_drift_idx
    ON aiqg.event_metrics (tenant_id, time)
    WHERE workflow_declared IS NOT NULL;
