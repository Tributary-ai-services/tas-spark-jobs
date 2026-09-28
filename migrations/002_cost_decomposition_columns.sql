-- AIQG-3 / aiqg-dashboard-be#72 — CLEAR v0.2 (Contract v1) cost-decomposition
-- columns on the Spark-owned aiqg.event_metrics hypertable (TimescaleDB,
-- tas_events db), plus the bound-invariant CHECK.
--
-- Why now. The gateway has emitted these fields since Plan #7 Phase 1, and
-- aiqg-dashboard-be already serves projected savings TS-first — but it reads
-- them out of raw->'data'->'token_accounting', because the columns were never
-- added. Measured 2026-09-28 on the live hypertable, same tenant and window
-- (2,590 rows): summing a native column takes 2.6 ms, one JSONB extraction
-- 55 ms, and the real six-field query 305 ms. That is ~21x per field, and it
-- grows linearly with row count while `raw` is TOASTed.
--
-- Additive + nullable ONLY (mirrors 001). The aggregator writes these via
-- INSERT ... ON CONFLICT DO NOTHING; older columns are untouched, so this is
-- safe to apply before OR after the aggregator image ships — but apply it
-- FIRST, because the new SELECT fails on missing columns. aiqg-dashboard-be
-- reads the columns with a raw-> COALESCE fallback, so the dashboard serves
-- correct numbers before, during and after this runs.
--
-- Apply out-of-band (the hypertable is not created by this repo):
--   psql "$JDBC_URL_equivalent" -f migrations/002_cost_decomposition_columns.sql
--
-- Contract: tas-llm-router/pkg/clear/cost_decomposer.go (Decomposition),
-- aether-shared/pkg/aiqg/events/event.go (TokenAccounting json tags).

-- NULLABLE, not NOT NULL DEFAULT 0, on purpose. The gateway tags these
-- `omitempty` on a float64, so a genuine zero is omitted from the event --
-- which is why genuine_post_model_waste_usd appears on only 9 of 2,624
-- decomposed rows. Defaulting 0 would make the 3,562 rows that were never
-- decomposed at all indistinguishable from decomposed-and-zero. reduction_mode
-- is the discriminator, and is what the existing dashboard queries already
-- filter on.
--
-- DOUBLE PRECISION, not the NUMERIC(12,6) the issue proposed. Measured on the
-- live corpus: per-row projected_direct_payload_waste_usd ranges from 5.8e-8 to
-- 0.056 (mean 1.97e-4), so 6 decimal places round 72 of 2,623 rows to zero
-- outright. DOUBLE PRECISION also matches the existing total_cost_usd column,
-- which the CHECK below compares against.
ALTER TABLE aiqg.event_metrics
  ADD COLUMN IF NOT EXISTS reduction_mode                     TEXT,
  ADD COLUMN IF NOT EXISTS projected_direct_payload_waste_usd DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS projected_reduction_relevance_usd  DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS projected_reduction_slm_usd        DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS projected_reduction_combined_usd   DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS induced_output_waste_estimated_usd DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS genuine_post_model_waste_usd       DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS context_efficiency_ratio           DOUBLE PRECISION;

-- Invariant #6 (tas-llm-router/pkg/clear/cost_decomposer.go:16):
--   direct + induced + genuine <= actual.
-- DecomposeCost clamps to it at the source and TestDecompose_BoundInvariant is
-- the Go canary, but a clamp in one emitter is not an invariant -- a backfill,
-- a second producer, or a change to the heuristic can all publish a
-- decomposition that claims more waste than the request cost. This makes it
-- non-bypassable.
--
-- Verified against the live corpus before adding: 0 violations in 2,623
-- decomposed rows, worst margin -8.6e-7. The epsilon absorbs float8
-- representation noise -- the JSON actual_cost_usd and the total_cost_usd
-- column agree exactly in float8, and differ by at most 7e-18 once cast
-- through NUMERIC.
--
-- COALESCE on every term because the columns are nullable: an undecomposed row
-- contributes 0 and passes trivially, which is also what makes this safe to
-- validate immediately against existing rows (they are all NULL).
ALTER TABLE aiqg.event_metrics
  ADD CONSTRAINT event_metrics_decomposition_bound CHECK (
      COALESCE(projected_direct_payload_waste_usd, 0)
    + COALESCE(induced_output_waste_estimated_usd, 0)
    + COALESCE(genuine_post_model_waste_usd, 0)
    <= COALESCE(total_cost_usd, 0) + 1e-9
  );

-- Partial index on the decomposed subset. Every projected-savings query filters
-- reduction_mode IS NOT NULL, and that is 2,624 of 6,186 rows today -- a
-- fraction that only shrinks as unpriced and pre-Phase-1 traffic accumulates.
CREATE INDEX IF NOT EXISTS event_metrics_decomposed_idx
    ON aiqg.event_metrics (tenant_id, "time" DESC)
    WHERE reduction_mode IS NOT NULL;
