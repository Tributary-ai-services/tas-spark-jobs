-- AIQG-47 Part 2 — cache-aware cost columns on the Spark-owned
-- aiqg.event_metrics hypertable (TimescaleDB, tas_events db).
--
-- Why. AIQG-41 (tas-llm-router #251) promoted cache accounting onto the event,
-- but only into Loki. The dashboard reads this table, which has no such
-- columns, so every cost the UI renders is still the legacy cache-blind
-- total_cost_usd. Measured 2026-10-07 on 117 cached rows (all Anthropic):
-- legacy $10.78 vs cache-aware $11.94 in aggregate, and per-row gaps up to
-- ~70x (claude-sonnet-5: $0.0005 vs $0.0347).
--
-- Additive + nullable ONLY (mirrors 001 and 002). The aggregator writes these
-- via INSERT ... ON CONFLICT DO NOTHING; existing columns are untouched, so
-- this is safe to apply before OR after the aggregator image ships -- but apply
-- it FIRST, because the new INSERT fails on missing columns.
--
-- NULLABLE, not NOT NULL DEFAULT 0, on purpose. The gateway tags every cache
-- field `omitempty`, so a request with no cache activity omits them entirely
-- (tas-llm-router/pkg/aiqg/events/emitter.go:222). NULL therefore means "no
-- cache tokens reported" and the dashboard reads
-- COALESCE(cache_aware_total_cost_usd, total_cost_usd). Defaulting to 0 would
-- make an uncached row report a cache-aware cost of $0.
--
-- total_cost_usd KEEPS its legacy meaning (uncached input + output). AIQG-41
-- kept it deliberately: dashboards and saved queries read it, and silently
-- redefining a published number is worse than adding a second one.
--
-- model_pricing_version is not cache-specific but is carried by 22,378 of
-- 25,947 existing rows in raw and promoted nowhere; it answers "what rate
-- table priced this row", which matters now that two tables existed on
-- 2026-10-06 alone.
--
-- DOUBLE PRECISION for USD, matching total_cost_usd and 002 (NUMERIC(12,6)
-- rounds sub-cent per-row values to zero). INTEGER for tokens, matching
-- prompt_tokens / the Spark IntegerType in schema.py.
--
-- Apply out-of-band (the hypertable is not created by this repo):
--   psql "$JDBC_URL_equivalent" -f migrations/003_cache_aware_cost_columns.sql
--
-- Contract: tas-llm-router/pkg/aiqg/events/event.go (TokenAccounting json
-- tags, lines 376-391), emitted by pkg/aiqg/events/emitter.go:208-228.
ALTER TABLE aiqg.event_metrics
  ADD COLUMN IF NOT EXISTS cache_creation_tokens       INTEGER,
  ADD COLUMN IF NOT EXISTS cache_read_tokens           INTEGER,
  ADD COLUMN IF NOT EXISTS cache_creation_cost_usd     DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS cache_read_cost_usd         DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS cache_aware_total_cost_usd  DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS model_pricing_version       TEXT;
