-- Migration 002: Create materialized views for performance optimization
-- This migration creates materialized views to pre-compute expensive average calculations
-- Updated to include data validation guards to prevent invalid batch_ids

-- 1. Materialized view for batch prove times
-- Pre-computes the time difference between batch proposal and proof using L1 block timestamps
CREATE MATERIALIZED VIEW IF NOT EXISTS ${DB}.batch_prove_times_mv
(
    batch_id UInt64,
    prove_time_ms UInt64,
    proved_at DateTime64(3),
    proved_hour DateTime64(3) MATERIALIZED toStartOfHour(proved_at),
    proved_day Date MATERIALIZED toDate(proved_at)
) ENGINE = MergeTree()
ORDER BY (proved_day, proved_hour, batch_id)
AS SELECT
    p.batch_id AS batch_id,
    (l1_proved.block_ts - l1_proposed.block_ts) * 1000 AS prove_time_ms,
    fromUnixTimestamp(l1_proved.block_ts) AS proved_at
FROM ${DB}.proved_batches p
INNER JOIN ${DB}.batches b ON p.batch_id = b.batch_id
INNER JOIN ${DB}.l1_head_events l1_proposed ON b.l1_block_number = l1_proposed.l1_block_number
INNER JOIN ${DB}.l1_head_events l1_proved ON p.l1_block_number = l1_proved.l1_block_number
WHERE p.batch_id != 0  -- Guard against invalid batch_ids
  AND b.batch_id != 0  -- Double-check on batches table
  AND l1_proved.block_ts > l1_proposed.block_ts  -- Sanity check: proved must be after proposed
  AND (l1_proved.block_ts - l1_proposed.block_ts) BETWEEN 1 AND 604800;  -- 1 second to 7 days

-- 2. Materialized view for batch verify times
-- Pre-computes the time difference between proof and verification using L1 block timestamps
CREATE MATERIALIZED VIEW IF NOT EXISTS ${DB}.batch_verify_times_mv
(
    batch_id UInt64,
    verify_time_ms UInt64,
    verified_at DateTime64(3),
    verified_hour DateTime64(3) MATERIALIZED toStartOfHour(verified_at),
    verified_day Date MATERIALIZED toDate(verified_at)
) ENGINE = MergeTree()
ORDER BY (verified_day, verified_hour, batch_id)
AS SELECT
    v.batch_id AS batch_id,
    (l1_verified.block_ts - l1_proved.block_ts) * 1000 AS verify_time_ms,
    fromUnixTimestamp(l1_verified.block_ts) AS verified_at
FROM ${DB}.verified_batches v
INNER JOIN ${DB}.proved_batches p ON v.batch_id = p.batch_id AND v.block_hash = p.block_hash
INNER JOIN ${DB}.l1_head_events l1_proved ON p.l1_block_number = l1_proved.l1_block_number
INNER JOIN ${DB}.l1_head_events l1_verified ON v.l1_block_number = l1_verified.l1_block_number
WHERE v.batch_id != 0  -- Guard against invalid batch_ids
  AND p.batch_id != 0  -- Double-check on proved_batches
  AND l1_verified.block_ts > l1_proved.block_ts  -- Sanity check: verified must be after proved
  AND (l1_verified.block_ts - l1_proved.block_ts) BETWEEN 1 AND 604800;  -- 1 second to 7 days

-- 3. Aggregated hourly averages for prove times
-- Pre-computes hourly averages to speed up range queries
CREATE MATERIALIZED VIEW IF NOT EXISTS ${DB}.hourly_avg_prove_times_mv
(
    hour DateTime64(3),
    avg_prove_time_ms Float64,
    sample_count UInt64
) ENGINE = MergeTree()
ORDER BY hour
AS SELECT
    proved_hour AS hour,
    avg(prove_time_ms) AS avg_prove_time_ms,
    count() AS sample_count
FROM ${DB}.batch_prove_times_mv
WHERE batch_id != 0  -- Extra safety check
GROUP BY proved_hour;

-- 4. Aggregated hourly averages for verify times
-- Pre-computes hourly averages to speed up range queries
CREATE MATERIALIZED VIEW IF NOT EXISTS ${DB}.hourly_avg_verify_times_mv
(
    hour DateTime64(3),
    avg_verify_time_ms Float64,
    sample_count UInt64
) ENGINE = MergeTree()
ORDER BY hour
AS SELECT
    verified_hour AS hour,
    avg(verify_time_ms) AS avg_verify_time_ms,
    count() AS sample_count
FROM ${DB}.batch_verify_times_mv
WHERE batch_id != 0  -- Extra safety check
GROUP BY verified_hour;

-- 5. Daily averages for prove times (for longer range queries)
CREATE MATERIALIZED VIEW IF NOT EXISTS ${DB}.daily_avg_prove_times_mv
(
    day Date,
    avg_prove_time_ms Float64,
    sample_count UInt64
) ENGINE = MergeTree()
ORDER BY day
AS SELECT
    proved_day AS day,
    avg(prove_time_ms) AS avg_prove_time_ms,
    count() AS sample_count
FROM ${DB}.batch_prove_times_mv
WHERE batch_id != 0  -- Extra safety check
GROUP BY proved_day;

-- 6. Daily averages for verify times (for longer range queries)
CREATE MATERIALIZED VIEW IF NOT EXISTS ${DB}.daily_avg_verify_times_mv
(
    day Date,
    avg_verify_time_ms Float64,
    sample_count UInt64
) ENGINE = MergeTree()
ORDER BY day
AS SELECT
    verified_day AS day,
    avg(verify_time_ms) AS avg_verify_time_ms,
    count() AS sample_count
FROM ${DB}.batch_verify_times_mv
WHERE batch_id != 0  -- Extra safety check
GROUP BY verified_day;