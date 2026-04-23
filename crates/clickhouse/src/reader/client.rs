//! `ClickHouse` reader functionality for API
//! Handles read-only operations and analytics queries

use super::TimeRange;
use chrono::{DateTime, LocalResult, TimeZone, Utc};
use clickhouse::{Client, Row, sql::Identifier};
use derive_more::Debug;
use eyre::{Context, Result};
use hex::encode;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tokio::try_join;
use tracing::{debug, error};
use url::Url;

use crate::{
    BatchRow,
    models::{
        BatchBlobCountRow, BatchFeeComponentRow, BatchPostingTimeRow, BatchProveTimeRow,
        BatchVerifyTimeRow, BlockFeeComponentRow, BlockTransactionRow, FailedProposalRow,
        ForcedInclusionProcessedRow, L1BlockTimeRow, L1DataCostRow, L2BlockTimeRow, L2GasUsedRow,
        L2ReorgRow, L2TpsRow, PreconfData, ProveCostRow, SequencerBlockRow, SequencerBlocksGrouped,
        SequencerDistributionRow, SequencerFeeRow, SlashingEventRow,
    },
    types::{AddressBytes, HashBytes},
};

#[derive(Row, Deserialize, Serialize)]
struct MaxTs {
    block_ts: u64,
}

/// `ClickHouse` reader client for API (read-only operations)
#[derive(Clone, Debug)]
pub struct ClickhouseReader {
    /// Base client
    #[debug(skip)]
    base: Client,
    /// Database name
    db_name: String,
}

impl ClickhouseReader {
    /// Create a new `ClickHouse` reader client
    pub fn new(url: Url, db_name: String, username: String, password: String) -> Result<Self> {
        let client = Client::default().with_url(url).with_user(username).with_password(password);

        Ok(Self { base: client, db_name })
    }

    async fn execute<R>(&self, query: &str) -> Result<Vec<R>>
    where
        R: Row + for<'b> Deserialize<'b>,
    {
        let client = self.base.clone();
        let start = Instant::now();

        let result = client.query(query).fetch_all::<R>().await;

        let duration_ms = start.elapsed().as_millis();
        match &result {
            Ok(rows) => {
                debug!(query = %query, duration_ms, rows = rows.len(), "ClickHouse query executed")
            }
            Err(e) => error!(query = %query, duration_ms, error = %e, "ClickHouse query failed"),
        }
        result.map_err(Into::into)
    }

    /// Anti-subquery that hides blocks later rolled back by a reorg.
    /// Use with `NOT IN (SELECT block_hash FROM ...)`
    fn reorg_filter(&self, table_alias: &str) -> String {
        // Disabled to avoid ClickHouse "Not-ready Set" errors from NOT IN.
        // Keep a harmless predicate that still references the alias to avoid JOIN ambiguity.
        // TODO: reintroduce reorg filtering via LEFT JOIN / NULL check to stay safe against reorgs.
        format!("assumeNotNull({table_alias}.block_hash) = assumeNotNull({table_alias}.block_hash)")
    }

    /// Get last L2 head time
    pub async fn get_last_l2_head_time(&self) -> Result<Option<DateTime<Utc>>> {
        let client = self.base.clone();
        let sql = "SELECT max(block_ts) AS block_ts FROM ?.l2_head_events";

        let start = Instant::now();
        let result = client.query(sql).bind(Identifier(&self.db_name)).fetch_all::<MaxTs>().await;

        let duration_ms = start.elapsed().as_millis();
        match &result {
            Ok(rows) => {
                debug!(query = sql, duration_ms, rows = rows.len(), "ClickHouse query executed")
            }
            Err(e) => error!(query = sql, duration_ms, error = %e, "ClickHouse query failed"),
        }

        let rows = result.context("fetching max(block_ts) failed")?;
        let row = match rows.into_iter().next() {
            Some(r) => r,
            None => return Ok(None),
        };
        if row.block_ts == 0 {
            return Ok(None);
        }
        let ts_opt = match Utc.timestamp_opt(row.block_ts as i64, 0) {
            LocalResult::Single(dt) => Some(dt),
            _ => None,
        };
        Ok(ts_opt)
    }

    /// Get timestamp of the latest L1 head event in UTC
    pub async fn get_last_l1_head_time(&self) -> Result<Option<DateTime<Utc>>> {
        let client = self.base.clone();
        let sql = "SELECT max(block_ts) AS block_ts FROM ?.l1_head_events";

        let start = Instant::now();
        let result = client.query(sql).bind(Identifier(&self.db_name)).fetch_all::<MaxTs>().await;

        let duration_ms = start.elapsed().as_millis();
        match &result {
            Ok(rows) => {
                debug!(query = sql, duration_ms, rows = rows.len(), "ClickHouse query executed")
            }
            Err(e) => error!(query = sql, duration_ms, error = %e, "ClickHouse query failed"),
        }

        let rows = result.context("fetching max(block_ts) failed")?;

        let row = match rows.into_iter().next() {
            Some(r) => r,
            None => return Ok(None),
        };

        if row.block_ts == 0 {
            return Ok(None);
        }

        let ts_opt = match Utc.timestamp_opt(row.block_ts as i64, 0) {
            LocalResult::Single(dt) => Some(dt),
            _ => None,
        };

        Ok(ts_opt)
    }

    /// Get the latest L2 block number.
    /// Uses an optimized query that should be faster on large tables.
    pub async fn get_last_l2_block_number(&self) -> Result<Option<u64>> {
        #[derive(Row, Deserialize)]
        struct BlockNumber {
            l2_block_number: u64,
        }

        let client = self.base.clone();
        let sql =
            "SELECT l2_block_number FROM ?.l2_head_events ORDER BY l2_block_number DESC LIMIT 1";

        let start = Instant::now();
        let result =
            client.query(sql).bind(Identifier(&self.db_name)).fetch_all::<BlockNumber>().await;

        let duration_ms = start.elapsed().as_millis();
        match &result {
            Ok(rows) => {
                debug!(query = sql, duration_ms, rows = rows.len(), "ClickHouse query executed")
            }
            Err(e) => error!(query = sql, duration_ms, error = %e, "ClickHouse query failed"),
        }

        let rows = result?;
        let row = match rows.into_iter().next() {
            Some(r) => r,
            None => return Ok(None),
        };
        if row.l2_block_number == 0 {
            return Ok(None);
        }
        Ok(Some(row.l2_block_number))
    }

    /// Get the latest L1 block number.
    /// Uses an optimized query that should be faster on large tables.
    pub async fn get_last_l1_block_number(&self) -> Result<Option<u64>> {
        #[derive(Row, Deserialize)]
        struct BlockNumber {
            l1_block_number: u64,
        }

        let client = self.base.clone();
        let sql =
            "SELECT l1_block_number FROM ?.l1_head_events ORDER BY l1_block_number DESC LIMIT 1";

        let start = Instant::now();
        let result =
            client.query(sql).bind(Identifier(&self.db_name)).fetch_all::<BlockNumber>().await;

        let duration_ms = start.elapsed().as_millis();
        match &result {
            Ok(rows) => {
                debug!(query = sql, duration_ms, rows = rows.len(), "ClickHouse query executed")
            }
            Err(e) => error!(query = sql, duration_ms, error = %e, "ClickHouse query failed"),
        }

        let rows = result?;
        let row = match rows.into_iter().next() {
            Some(r) => r,
            None => return Ok(None),
        };
        if row.l1_block_number == 0 {
            return Ok(None);
        }
        Ok(Some(row.l1_block_number))
    }

    /// Get timestamp of the latest `BatchProposed` event based on L1 block timestamp in UTC
    pub async fn get_last_batch_time(&self) -> Result<Option<DateTime<Utc>>> {
        let client = self.base.clone();
        let sql = "SELECT max(l1_events.block_ts) AS block_ts \
             FROM ?.batches b \
             INNER JOIN ?.l1_head_events l1_events \
               ON b.l1_block_number = l1_events.l1_block_number";

        let start = Instant::now();
        let result = client
            .query(sql)
            .bind(Identifier(&self.db_name))
            .bind(Identifier(&self.db_name))
            .fetch_all::<MaxTs>()
            .await;

        let duration_ms = start.elapsed().as_millis();
        match &result {
            Ok(rows) => {
                debug!(query = sql, duration_ms, rows = rows.len(), "ClickHouse query executed")
            }
            Err(e) => error!(query = sql, duration_ms, error = %e, "ClickHouse query failed"),
        }

        let rows = result.context("fetching max batch L1 block timestamp failed")?;

        let row = match rows.into_iter().next() {
            Some(r) => r,
            None => return Ok(None),
        };

        if row.block_ts == 0 {
            return Ok(None);
        }

        let ts_opt = match Utc.timestamp_opt(row.block_ts as i64, 0) {
            LocalResult::Single(dt) => Some(dt),
            _ => None,
        };

        Ok(ts_opt)
    }

    /// Get the most recent preconfiguration data
    pub async fn get_last_preconf_data(&self) -> Result<Option<PreconfData>> {
        let client = self.base.clone();
        let sql = "SELECT slot, current_operator, next_operator FROM ?.preconf_data ORDER BY inserted_at DESC LIMIT 1";

        let start = Instant::now();
        let result =
            client.query(sql).bind(Identifier(&self.db_name)).fetch_all::<PreconfData>().await;

        let duration_ms = start.elapsed().as_millis();
        match &result {
            Ok(rows) => {
                debug!(query = sql, duration_ms, rows = rows.len(), "ClickHouse query executed")
            }
            Err(e) => error!(query = sql, duration_ms, error = %e, "ClickHouse query failed"),
        }

        let rows = result?;
        Ok(rows.into_iter().next())
    }

    /// Get all batches that have not been proven and are older than the given cutoff time
    pub async fn get_unproved_batches_older_than(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<(u64, u64, DateTime<Utc>)>> {
        let client = self.base.clone();
        let sql = "SELECT b.l1_block_number, b.batch_id, toUnixTimestamp64Milli(b.inserted_at) as inserted_at \
             FROM (SELECT l1_block_number, batch_id, inserted_at \
                   FROM ?.batches \
                   WHERE inserted_at < toDateTime64(?, 3)) AS b \
             LEFT JOIN ?.proved_batches p \
               ON b.l1_block_number = p.l1_block_number AND b.batch_id = p.batch_id \
             WHERE p.batch_id IS NULL \
             ORDER BY b.inserted_at ASC";

        let start = Instant::now();
        let result = client
            .query(sql)
            .bind(Identifier(&self.db_name))
            .bind(cutoff.timestamp_millis() as f64 / 1000.0)
            .bind(Identifier(&self.db_name))
            .fetch_all::<(u64, u64, u64)>()
            .await;

        let duration_ms = start.elapsed().as_millis();
        match &result {
            Ok(rows) => {
                debug!(query = sql, duration_ms, rows = rows.len(), "ClickHouse query executed")
            }
            Err(e) => error!(query = sql, duration_ms, error = %e, "ClickHouse query failed"),
        }
        let rows = result.context("fetching unproved batches failed")?;
        Ok(rows
            .into_iter()
            .filter_map(|(l1_block_number, batch_id, inserted_at)| {
                match chrono::Utc.timestamp_millis_opt(inserted_at as i64) {
                    chrono::LocalResult::Single(dt) => Some((l1_block_number, batch_id, dt)),
                    _ => None,
                }
            })
            .collect())
    }

    /// Get batch rows ordered by recency for repair and consistency checks.
    pub async fn get_recent_batch_rows(&self, limit: u64) -> Result<Vec<BatchRow>> {
        let client = self.base.clone();
        let sql = "SELECT l1_block_number, l1_tx_hash, batch_id, batch_size, last_l2_block_number, proposer_addr, blob_count, blob_total_bytes \
             FROM ?.batches \
             ORDER BY inserted_at DESC \
             LIMIT ?";

        let start = Instant::now();
        let result: clickhouse::error::Result<Vec<BatchRow>> = client
            .query(sql)
            .bind(Identifier(&self.db_name))
            .bind(limit)
            .fetch_all::<BatchRow>()
            .await;

        let duration_ms = start.elapsed().as_millis();
        match &result {
            Ok(rows) => {
                debug!(query = sql, duration_ms, rows = rows.len(), "ClickHouse query executed")
            }
            Err(e) => error!(query = sql, duration_ms, error = %e, "ClickHouse query failed"),
        }

        result.context("fetching recent batch rows failed")
    }

    /// Get all proved batch IDs from the `proved_batches` table
    pub async fn get_proved_batch_ids(&self) -> Result<Vec<u64>> {
        #[derive(Row, Deserialize)]
        struct ProvedBatchIdRow {
            batch_id: u64,
        }
        let client = self.base.clone();
        let sql = "SELECT batch_id FROM ?.proved_batches";

        let start = Instant::now();
        let result =
            client.query(sql).bind(Identifier(&self.db_name)).fetch_all::<ProvedBatchIdRow>().await;

        let duration_ms = start.elapsed().as_millis();
        match &result {
            Ok(rows) => {
                debug!(query = sql, duration_ms, rows = rows.len(), "ClickHouse query executed")
            }
            Err(e) => error!(query = sql, duration_ms, error = %e, "ClickHouse query failed"),
        }

        let rows = result?;
        Ok(rows.into_iter().map(|r| r.batch_id).collect())
    }

    /// Get all slashing events that occurred after the given cutoff time
    pub async fn get_slashing_events_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<SlashingEventRow>> {
        let client = self.base.clone();
        let sql = "SELECT l1_block_number, validator_addr FROM ?.slashing_events \
             WHERE inserted_at > toDateTime64(?, 3) \
             ORDER BY inserted_at ASC";

        let start = Instant::now();
        let result = client
            .query(sql)
            .bind(Identifier(&self.db_name))
            .bind(since.timestamp_millis() as f64 / 1000.0)
            .fetch_all::<SlashingEventRow>()
            .await;

        let duration_ms = start.elapsed().as_millis();
        match &result {
            Ok(rows) => {
                debug!(query = sql, duration_ms, rows = rows.len(), "ClickHouse query executed")
            }
            Err(e) => error!(query = sql, duration_ms, error = %e, "ClickHouse query failed"),
        }
        let rows = result.context("fetching slashing events failed")?;
        Ok(rows)
    }

    /// Get slashing events that occurred within the given time range
    pub async fn get_slashing_events_range(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<SlashingEventRow>> {
        let client = self.base.clone();
        let sql = "SELECT l1_block_number, validator_addr FROM ?.slashing_events \
             WHERE inserted_at > toDateTime64(?, 3) \
               AND inserted_at <= toDateTime64(?, 3) \
             ORDER BY inserted_at ASC";

        let start = Instant::now();
        let result = client
            .query(sql)
            .bind(Identifier(&self.db_name))
            .bind(since.timestamp_millis() as f64 / 1000.0)
            .bind(until.timestamp_millis() as f64 / 1000.0)
            .fetch_all::<SlashingEventRow>()
            .await;

        let duration_ms = start.elapsed().as_millis();
        match &result {
            Ok(rows) => {
                debug!(query = sql, duration_ms, rows = rows.len(), "ClickHouse query executed")
            }
            Err(e) => error!(query = sql, duration_ms, error = %e, "ClickHouse query failed"),
        }
        let rows = result.context("fetching slashing events failed")?;
        Ok(rows)
    }

    /// Get all forced inclusion events that occurred after the given cutoff time
    pub async fn get_forced_inclusions_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<ForcedInclusionProcessedRow>> {
        let client = self.base.clone();
        let sql = "SELECT blob_hash FROM ?.forced_inclusion_processed \
             WHERE inserted_at > toDateTime64(?, 3) \
             ORDER BY inserted_at ASC";

        let start = Instant::now();
        let result = client
            .query(sql)
            .bind(Identifier(&self.db_name))
            .bind(since.timestamp_millis() as f64 / 1000.0)
            .fetch_all::<ForcedInclusionProcessedRow>()
            .await;

        let duration_ms = start.elapsed().as_millis();
        match &result {
            Ok(rows) => {
                debug!(query = sql, duration_ms, rows = rows.len(), "ClickHouse query executed")
            }
            Err(e) => error!(query = sql, duration_ms, error = %e, "ClickHouse query failed"),
        }
        let rows = result.context("fetching forced inclusion events failed")?;
        Ok(rows)
    }

    /// Get forced inclusion events that occurred within the given time range
    pub async fn get_forced_inclusions_range(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<ForcedInclusionProcessedRow>> {
        let client = self.base.clone();
        let sql = "SELECT blob_hash FROM ?.forced_inclusion_processed \
             WHERE inserted_at > toDateTime64(?, 3) \
               AND inserted_at <= toDateTime64(?, 3) \
             ORDER BY inserted_at ASC";

        let start = Instant::now();
        let result = client
            .query(sql)
            .bind(Identifier(&self.db_name))
            .bind(since.timestamp_millis() as f64 / 1000.0)
            .bind(until.timestamp_millis() as f64 / 1000.0)
            .fetch_all::<ForcedInclusionProcessedRow>()
            .await;

        let duration_ms = start.elapsed().as_millis();
        match &result {
            Ok(rows) => {
                debug!(query = sql, duration_ms, rows = rows.len(), "ClickHouse query executed")
            }
            Err(e) => error!(query = sql, duration_ms, error = %e, "ClickHouse query failed"),
        }
        let rows = result.context("fetching forced inclusion events failed")?;
        Ok(rows)
    }

    /// Get failed proposal events that occurred after the given cutoff time
    pub async fn get_failed_proposals_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<FailedProposalRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            batch_id: u64,
            original_sequencer: AddressBytes,
            proposer: AddressBytes,
            l1_block_number: u64,
            ts: u64,
        }

        let rf = self.reorg_filter("h");
        let (addr_arr, name_arr) = crate::mapping::transform_arrays_sql();
        let query = format!(
            "SELECT b.batch_id, h.sequencer AS original_sequencer, \
                    b.proposer_addr AS proposer, b.l1_block_number, \
                    toUInt64(toUnixTimestamp64Milli(b.inserted_at)) AS ts \
             FROM {db}.batches b \
             INNER JOIN {db}.l2_head_events h \
               ON h.l2_block_number = b.last_l2_block_number \
             WHERE b.inserted_at > toDateTime64({since}, 3) \
               AND {rf} \
               AND transform(lower(concat('0x', hex(h.sequencer))), {addr_arr}, {name_arr}, lower(concat('0x', hex(h.sequencer)))) \
                   != transform(lower(concat('0x', hex(b.proposer_addr))), {addr_arr}, {name_arr}, lower(concat('0x', hex(b.proposer_addr)))) \
             ORDER BY b.inserted_at ASC",
            db = self.db_name,
            since = since.timestamp_millis() as f64 / 1000.0,
            rf = rf,
            addr_arr = addr_arr,
            name_arr = name_arr,
        );
        let rows =
            self.execute::<RawRow>(&query).await.context("fetching failed proposals failed")?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let ts = Utc.timestamp_millis_opt(r.ts as i64).single()?;
                Some(FailedProposalRow {
                    batch_id: r.batch_id,
                    original_sequencer: r.original_sequencer,
                    proposer: r.proposer,
                    l1_block_number: r.l1_block_number,
                    inserted_at: ts,
                })
            })
            .collect())
    }

    /// Get failed proposal events within the given time range
    pub async fn get_failed_proposals_range(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<FailedProposalRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            batch_id: u64,
            original_sequencer: AddressBytes,
            proposer: AddressBytes,
            l1_block_number: u64,
            ts: u64,
        }

        let rf = self.reorg_filter("h");
        let (addr_arr, name_arr) = crate::mapping::transform_arrays_sql();
        let query = format!(
            "SELECT b.batch_id, h.sequencer AS original_sequencer, \
                    b.proposer_addr AS proposer, b.l1_block_number, \
                    toUInt64(toUnixTimestamp64Milli(b.inserted_at)) AS ts \
             FROM {db}.batches b \
             INNER JOIN {db}.l2_head_events h \
               ON h.l2_block_number = b.last_l2_block_number \
             WHERE b.inserted_at > toDateTime64({since}, 3) \
               AND b.inserted_at <= toDateTime64({until}, 3) \
               AND {rf} \
               AND transform(lower(concat('0x', hex(h.sequencer))), {addr_arr}, {name_arr}, lower(concat('0x', hex(h.sequencer)))) \
                   != transform(lower(concat('0x', hex(b.proposer_addr))), {addr_arr}, {name_arr}, lower(concat('0x', hex(b.proposer_addr)))) \
             ORDER BY b.inserted_at ASC",
            db = self.db_name,
            since = since.timestamp_millis() as f64 / 1000.0,
            until = until.timestamp_millis() as f64 / 1000.0,
            rf = rf,
            addr_arr = addr_arr,
            name_arr = name_arr,
        );
        let rows =
            self.execute::<RawRow>(&query).await.context("fetching failed proposals failed")?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let ts = Utc.timestamp_millis_opt(r.ts as i64).single()?;
                Some(FailedProposalRow {
                    batch_id: r.batch_id,
                    original_sequencer: r.original_sequencer,
                    proposer: r.proposer,
                    l1_block_number: r.l1_block_number,
                    inserted_at: ts,
                })
            })
            .collect())
    }

    /// Get failed proposal events since the given time with cursor-based pagination.
    /// Results are returned in descending order by time recorded.
    pub async fn get_failed_proposals_paginated(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: u64,
        starting_after: Option<u64>,
        ending_before: Option<u64>,
    ) -> Result<Vec<FailedProposalRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            batch_id: u64,
            original_sequencer: AddressBytes,
            proposer: AddressBytes,
            l1_block_number: u64,
            ts: u64,
        }

        let rf = self.reorg_filter("h");
        let (addr_arr, name_arr) = crate::mapping::transform_arrays_sql();
        let mut query = format!(
            "SELECT b.batch_id, h.sequencer AS original_sequencer, \
                    b.proposer_addr AS proposer, b.l1_block_number, \
                    toUInt64(toUnixTimestamp64Milli(b.inserted_at)) AS ts \
             FROM {db}.batches b \
             INNER JOIN {db}.l2_head_events h \
               ON h.l2_block_number = b.last_l2_block_number \
             WHERE {rf} \
               AND transform(lower(concat('0x', hex(h.sequencer))), {addr_arr}, {name_arr}, lower(concat('0x', hex(h.sequencer)))) \
                   != transform(lower(concat('0x', hex(b.proposer_addr))), {addr_arr}, {name_arr}, lower(concat('0x', hex(b.proposer_addr))))",
            db = self.db_name,
            rf = rf,
            addr_arr = addr_arr,
            name_arr = name_arr,
        );

        let since_sec = since.timestamp_millis() as f64 / 1000.0;
        let until_sec = until.timestamp_millis() as f64 / 1000.0;

        match (starting_after, ending_before) {
            (Some(start), None) => {
                // Next page: b.inserted_at > since AND (b.inserted_at < until OR (b.inserted_at =
                // until AND l2 < start))
                query.push_str(&format!(
                    " AND b.inserted_at > toDateTime64({since}, 3) \
                       AND (b.inserted_at < toDateTime64({until}, 3) \
                            OR (b.inserted_at = toDateTime64({until}, 3) AND b.batch_id < {start}))",
                    since = since_sec,
                    until = until_sec,
                    start = start,
                ));
            }
            (None, Some(end)) => {
                // Prev page: let pivot = since + 1ms
                let pivot = since_sec + 0.001;
                query.push_str(&format!(
                    " AND b.inserted_at <= toDateTime64({until}, 3) \
                       AND (b.inserted_at > toDateTime64({pivot}, 3) \
                            OR (b.inserted_at = toDateTime64({pivot}, 3) AND b.batch_id > {end}))",
                    until = until_sec,
                    pivot = pivot,
                    end = end,
                ));
            }
            _ => {
                query.push_str(&format!(
                    " AND b.inserted_at > toDateTime64({since}, 3) AND b.inserted_at <= toDateTime64({until}, 3)",
                    since = since_sec,
                    until = until_sec,
                ));
            }
        }

        query.push_str(" ORDER BY b.inserted_at DESC, b.batch_id DESC");
        query.push_str(&format!(" LIMIT {}", limit));

        let rows =
            self.execute::<RawRow>(&query).await.context("fetching failed proposals failed")?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let ts = Utc.timestamp_millis_opt(r.ts as i64).single()?;
                Some(FailedProposalRow {
                    batch_id: r.batch_id,
                    original_sequencer: r.original_sequencer,
                    proposer: r.proposer,
                    l1_block_number: r.l1_block_number,
                    inserted_at: ts,
                })
            })
            .collect())
    }

    /// Get all L2 reorg events that occurred after the given cutoff time
    pub async fn get_l2_reorgs_since(&self, since: DateTime<Utc>) -> Result<Vec<L2ReorgRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            l2_block_number: u64,
            depth: u16,
            old_sequencer: AddressBytes,
            new_sequencer: AddressBytes,
            ts: u64,
        }

        let query = format!(
            "SELECT l2_block_number, depth, old_sequencer, new_sequencer, \
                    toUInt64(toUnixTimestamp64Milli(inserted_at)) AS ts \
             FROM {}.l2_reorgs \
             WHERE inserted_at > toDateTime64({}, 3) \
             ORDER BY inserted_at ASC",
            self.db_name,
            since.timestamp_millis() as f64 / 1000.0,
        );
        let rows = self.execute::<RawRow>(&query).await.context("fetching reorg events failed")?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let ts = Utc.timestamp_millis_opt(r.ts as i64).single()?;
                Some(L2ReorgRow {
                    l2_block_number: r.l2_block_number,
                    depth: r.depth,
                    old_sequencer: r.old_sequencer,
                    new_sequencer: r.new_sequencer,
                    inserted_at: ts,
                })
            })
            .collect())
    }

    /// Get L2 reorg events since the given cutoff with cursor-based pagination.
    /// Results are returned in descending order by time recorded.
    pub async fn get_l2_reorgs_paginated(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: u64,
        starting_after: Option<u64>,
        ending_before: Option<u64>,
    ) -> Result<Vec<L2ReorgRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            l2_block_number: u64,
            depth: u16,
            old_sequencer: AddressBytes,
            new_sequencer: AddressBytes,
            ts: u64,
        }

        let mut query = format!(
            "SELECT l2_block_number, depth, old_sequencer, new_sequencer, \
                    toUInt64(toUnixTimestamp64Milli(inserted_at)) AS ts \
             FROM {db}.l2_reorgs \
             WHERE inserted_at > toDateTime64({since}, 3) \
               AND inserted_at <= toDateTime64({until}, 3)",
            db = self.db_name,
            since = since.timestamp_millis() as f64 / 1000.0,
            until = until.timestamp_millis() as f64 / 1000.0,
        );

        if let Some(start) = starting_after {
            query.push_str(&format!(" AND l2_block_number < {}", start));
        }
        if let Some(end) = ending_before {
            query.push_str(&format!(" AND l2_block_number > {}", end));
        }

        query.push_str(" ORDER BY inserted_at DESC");
        query.push_str(&format!(" LIMIT {}", limit));

        let rows = self.execute::<RawRow>(&query).await.context("fetching reorg events failed")?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let ts = Utc.timestamp_millis_opt(r.ts as i64).single()?;
                Some(L2ReorgRow {
                    l2_block_number: r.l2_block_number,
                    depth: r.depth,
                    old_sequencer: r.old_sequencer,
                    new_sequencer: r.new_sequencer,
                    inserted_at: ts,
                })
            })
            .collect())
    }

    /// Get all active gateway addresses observed since the given cutoff time
    /// Get the number of blocks produced by each sequencer since the given cutoff time
    pub async fn get_sequencer_distribution_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<SequencerDistributionRow>> {
        let query = format!(
            "SELECT sequencer,\n\
                   count(DISTINCT h.l2_block_number) AS blocks,\n\
                   toUInt64(min(h.block_ts)) AS min_ts,\n\
                   toUInt64(max(h.block_ts)) AS max_ts,\n\
                   sum(sum_tx) AS tx_sum\n\
             FROM {db}.l2_head_events h\n\
             WHERE h.block_ts > {since}\n\
               AND {filter}\n\
             GROUP BY sequencer\n\
             ORDER BY blocks DESC",
            since = since.timestamp(),
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );

        let rows = self.execute::<SequencerDistributionRow>(&query).await?;
        Ok(rows)
    }

    /// Get the list of block numbers proposed by each sequencer since the given cutoff time
    pub async fn get_sequencer_blocks_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<SequencerBlockRow>> {
        let query = format!(
            "SELECT sequencer, h.l2_block_number \
             FROM {db}.l2_head_events h \
             WHERE h.block_ts > {} \
               AND {filter} \
             ORDER BY sequencer, h.l2_block_number ASC",
            since.timestamp(),
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );

        let rows = self.execute::<SequencerBlockRow>(&query).await?;
        Ok(rows)
    }

    /// Get sequencer blocks grouped by sequencer address since the given cutoff time.
    /// This uses database aggregation instead of in-memory grouping for better performance.
    pub async fn get_sequencer_blocks_grouped_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<SequencerBlocksGrouped>> {
        let query = format!(
            "SELECT sequencer, groupArray(h.l2_block_number) as blocks \
             FROM {db}.l2_head_events h \
             WHERE h.block_ts > {} \
               AND {filter} \
             GROUP BY sequencer \
             ORDER BY sequencer ASC",
            since.timestamp(),
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );

        let rows = self.execute::<SequencerBlocksGrouped>(&query).await?;
        Ok(rows)
    }

    /// Get transactions per block since the given cutoff time with cursor-based
    /// pagination. Results are returned in descending order by block number.
    pub async fn get_block_transactions_paginated(
        &self,
        since: DateTime<Utc>,
        limit: u64,
        starting_after: Option<u64>,
        ending_before: Option<u64>,
        sequencer: Option<AddressBytes>,
        bucket: Option<u64>,
    ) -> Result<Vec<BlockTransactionRow>> {
        let bucket = bucket.unwrap_or(1);

        if bucket <= 1 {
            // Non-bucketed implementation
            #[derive(Row, Deserialize)]
            struct RawRow {
                sequencer: AddressBytes,
                l2_block_number: u64,
                block_time: u64,
                sum_tx: u32,
            }

            let mut query = format!(
                "SELECT sequencer, h.l2_block_number, h.block_ts AS block_time, sum_tx \
                 FROM {db}.l2_head_events h \
                 WHERE h.block_ts >= {} \
                   AND {filter}",
                since.timestamp(),
                filter = self.reorg_filter("h"),
                db = self.db_name,
            );
            if let Some(addr) = sequencer {
                query.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
            }

            if let Some(start) = starting_after {
                query.push_str(&format!(" AND l2_block_number < {}", start));
            }

            if let Some(end) = ending_before {
                query.push_str(&format!(" AND l2_block_number > {}", end));
            }

            query.push_str(" ORDER BY l2_block_number DESC");
            query.push_str(&format!(" LIMIT {}", limit));

            let rows = self.execute::<RawRow>(&query).await?;
            return Ok(rows
                .into_iter()
                .map(|r| BlockTransactionRow {
                    sequencer: r.sequencer,
                    l2_block_number: r.l2_block_number,
                    block_time: Utc.timestamp_opt(r.block_time as i64, 0).unwrap(),
                    sum_tx: r.sum_tx,
                })
                .collect());
        }

        // Bucketed implementation using SQL aggregation
        #[derive(Row, Deserialize)]
        struct AggRow {
            sequencer: AddressBytes,
            l2_block_number: u64,
            block_time: u64,
            sum_tx: u32,
        }

        let mut inner = format!(
            "SELECT sequencer, h.l2_block_number, h.block_ts AS block_time, sum_tx \
             FROM {db}.l2_head_events h \
             WHERE h.block_ts >= {} \
               AND {filter}",
            since.timestamp(),
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );
        if let Some(addr) = sequencer {
            inner.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
        }

        if let Some(start) = starting_after {
            inner.push_str(&format!(" AND l2_block_number < {}", start));
        }

        if let Some(end) = ending_before {
            inner.push_str(&format!(" AND l2_block_number > {}", end));
        }

        let query = format!(
            "SELECT l2_bucket AS l2_block_number, \
                    argMax(sequencer, l2_block_number) AS sequencer, \
                    max(block_time) AS block_time, \
                    toUInt32(avg(sum_tx)) AS sum_tx \
             FROM ( \
                SELECT intDiv(l2_block_number, {bucket}) * {bucket} AS l2_bucket, \
                       sequencer, \
                       l2_block_number, \
                       block_time, \
                       sum_tx \
                FROM ({inner}) AS base \
             ) AS sub \
             GROUP BY l2_bucket \
             ORDER BY l2_bucket DESC \
             LIMIT {limit}",
            bucket = bucket,
            inner = inner,
            limit = limit,
        );

        let rows = self.execute::<AggRow>(&query).await?;
        Ok(rows
            .into_iter()
            .map(|r| BlockTransactionRow {
                sequencer: r.sequencer,
                l2_block_number: r.l2_block_number,
                block_time: Utc.timestamp_opt(r.block_time as i64, 0).unwrap(),
                sum_tx: r.sum_tx,
            })
            .collect())
    }

    /// Get transactions per block for the specified block range.
    pub async fn get_block_transactions_block_range(
        &self,
        start_block: Option<u64>,
        end_block: Option<u64>,
        sequencer: Option<AddressBytes>,
        limit: u64,
        starting_after: Option<u64>,
        ending_before: Option<u64>,
    ) -> Result<Vec<BlockTransactionRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            sequencer: AddressBytes,
            l2_block_number: u64,
            block_time: u64,
            sum_tx: u32,
        }

        let mut query = format!(
            "SELECT sequencer, h.l2_block_number, h.block_ts AS block_time, sum_tx \
             FROM {db}.l2_head_events h \
             WHERE {filter}",
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );

        if let Some(start) = start_block {
            query.push_str(&format!(" AND h.l2_block_number >= {}", start));
        }

        if let Some(end) = end_block {
            query.push_str(&format!(" AND h.l2_block_number <= {}", end));
        }

        if let Some(addr) = sequencer {
            query.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
        }

        if let Some(start) = starting_after {
            query.push_str(&format!(" AND l2_block_number < {}", start));
        }

        if let Some(end) = ending_before {
            query.push_str(&format!(" AND l2_block_number > {}", end));
        }

        query.push_str(" ORDER BY l2_block_number DESC");
        query.push_str(&format!(" LIMIT {}", limit));

        let rows = self.execute::<RawRow>(&query).await?;
        Ok(rows
            .into_iter()
            .map(|r| BlockTransactionRow {
                sequencer: r.sequencer,
                l2_block_number: r.l2_block_number,
                block_time: Utc.timestamp_opt(r.block_time as i64, 0).unwrap(),
                sum_tx: r.sum_tx,
            })
            .collect())
    }

    /// Get L2 block times since the given cutoff with cursor-based pagination.
    /// Results are returned in descending order by block number.
    pub async fn get_l2_block_times_paginated(
        &self,
        since: DateTime<Utc>,
        limit: u64,
        starting_after: Option<u64>,
        ending_before: Option<u64>,
        sequencer: Option<AddressBytes>,
    ) -> Result<Vec<L2BlockTimeRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            l2_block_number: u64,
            block_time: u64,
            s_since_prev_block: Option<u64>,
        }

        let mut query = format!(
            "WITH time_diffs AS ( \
                SELECT h.l2_block_number, \
                       h.block_ts AS block_time, \
                       h.sequencer, \
                       toUInt64OrNull(toString(if(isNull(lagInFrame(h.block_ts) OVER (ORDER BY h.l2_block_number)), NULL, h.block_ts - lagInFrame(h.block_ts) OVER (ORDER BY h.l2_block_number)))) \
                           AS s_since_prev_block \
                FROM {db}.l2_head_events h \
                WHERE {filter} \
            ) \
            SELECT l2_block_number, block_time, s_since_prev_block \
            FROM time_diffs \
            WHERE block_time >= {since}",
            since = since.timestamp(),
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );
        if let Some(addr) = sequencer {
            query.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
        }
        if let Some(start) = starting_after {
            query.push_str(&format!(" AND l2_block_number < {}", start));
        }
        if let Some(end) = ending_before {
            query.push_str(&format!(" AND l2_block_number > {}", end));
        }
        query.push_str(" ORDER BY l2_block_number DESC");
        query.push_str(&format!(" LIMIT {}", limit));

        let rows = self.execute::<RawRow>(&query).await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let dt = Utc.timestamp_opt(r.block_time as i64, 0).single()?;
                r.s_since_prev_block.map(|s| L2BlockTimeRow {
                    l2_block_number: r.l2_block_number,
                    block_time: dt,
                    s_since_prev_block: s,
                })
            })
            .collect())
    }

    /// Get L2 gas usage since the given cutoff with cursor-based pagination.
    /// Results are returned in descending order by block number.
    pub async fn get_l2_gas_used_paginated(
        &self,
        since: DateTime<Utc>,
        limit: u64,
        starting_after: Option<u64>,
        ending_before: Option<u64>,
        sequencer: Option<AddressBytes>,
    ) -> Result<Vec<L2GasUsedRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            l2_block_number: u64,
            block_time: u64,
            gas_used: u64,
        }

        let mut query = format!(
            "SELECT h.l2_block_number, h.block_ts AS block_time, toUInt64(sum_gas_used) AS gas_used \
             FROM {db}.l2_head_events h \
             WHERE h.block_ts >= {since} \
               AND {filter}",
            since = since.timestamp(),
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );
        if let Some(addr) = sequencer {
            query.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
        }
        if let Some(start) = starting_after {
            query.push_str(&format!(" AND l2_block_number < {}", start));
        }
        if let Some(end) = ending_before {
            query.push_str(&format!(" AND l2_block_number > {}", end));
        }
        query.push_str(" ORDER BY l2_block_number DESC");
        query.push_str(&format!(" LIMIT {}", limit));

        let rows = self.execute::<RawRow>(&query).await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let dt = Utc.timestamp_opt(r.block_time as i64, 0).unwrap();
                L2GasUsedRow {
                    l2_block_number: r.l2_block_number,
                    block_time: dt,
                    gas_used: r.gas_used,
                }
            })
            .collect())
    }

    /// Get L2 TPS since the given cutoff with cursor-based pagination.
    /// Results are returned in descending order by block number.
    pub async fn get_l2_tps_paginated(
        &self,
        since: DateTime<Utc>,
        limit: u64,
        starting_after: Option<u64>,
        ending_before: Option<u64>,
        sequencer: Option<AddressBytes>,
    ) -> Result<Vec<L2TpsRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            l2_block_number: u64,
            sum_tx: u32,
            s_since_prev_block: Option<u64>,
        }

        let mut query = format!(
            "SELECT h.l2_block_number, sum_tx, \
                    toUInt64OrNull(toString(if(isNull(lagInFrame(h.block_ts) OVER (ORDER BY h.l2_block_number)), NULL, h.block_ts - lagInFrame(h.block_ts) OVER (ORDER BY h.l2_block_number)))) AS s_since_prev_block \
             FROM {db}.l2_head_events h \
             WHERE h.block_ts >= {since} \
               AND {filter}",
            since = since.timestamp(),
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );
        if let Some(addr) = sequencer {
            query.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
        }
        if let Some(start) = starting_after {
            query.push_str(&format!(" AND l2_block_number < {}", start));
        }
        if let Some(end) = ending_before {
            query.push_str(&format!(" AND l2_block_number > {}", end));
        }
        query.push_str(" ORDER BY l2_block_number DESC");
        query.push_str(&format!(" LIMIT {}", limit));

        let rows = self.execute::<RawRow>(&query).await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let s = r.s_since_prev_block?;
                if s == 0 {
                    None
                } else {
                    Some(L2TpsRow {
                        l2_block_number: r.l2_block_number,
                        tps: r.sum_tx as f64 / s as f64,
                    })
                }
            })
            .collect())
    }

    /// Get the average time in milliseconds it takes for a batch to be proven
    /// for proofs submitted within the given time range
    pub async fn get_avg_prove_time(&self, range: TimeRange) -> Result<Option<u64>> {
        #[derive(Row, Deserialize)]
        struct AvgRow {
            avg_ms: f64,
        }

        // First try the materialized view
        let mv_query = format!(
            "SELECT avg(prove_time_ms) AS avg_ms \
             FROM {db}.batch_prove_times_mv \
             WHERE proved_at >= now64() - INTERVAL {interval} \
               AND batch_id != 0",
            interval = range.interval(),
            db = self.db_name,
        );

        let rows = self.execute::<AvgRow>(&mv_query).await?;
        if let Some(row) = rows.into_iter().next() &&
            !row.avg_ms.is_nan()
        {
            return Ok(Some(row.avg_ms.round() as u64));
        }

        // Fallback to raw data if materialized view is empty
        let fallback_query = format!(
            "SELECT avg((l1_proved.block_ts - l1_proposed.block_ts) * 1000) AS avg_ms \
             FROM {db}.batches b \
             JOIN {db}.proved_batches pb ON b.batch_id = pb.batch_id \
             JOIN {db}.l1_head_events l1_proposed \
               ON b.l1_block_number = l1_proposed.l1_block_number \
             JOIN {db}.l1_head_events l1_proved \
               ON pb.l1_block_number = l1_proved.l1_block_number \
             WHERE l1_proved.block_ts >= (toUInt64(now()) - {secs}) \
               AND b.batch_id != 0",
            secs = range.seconds(),
            db = self.db_name,
        );

        let rows = self.execute::<AvgRow>(&fallback_query).await?;
        let row = match rows.into_iter().next() {
            Some(r) => r,
            None => return Ok(None),
        };

        if row.avg_ms.is_nan() { Ok(None) } else { Ok(Some(row.avg_ms.round() as u64)) }
    }

    /// Get the average time in milliseconds it takes for a batch to be verified
    /// for verifications submitted within the given time range
    pub async fn get_avg_verify_time(&self, range: TimeRange) -> Result<Option<u64>> {
        #[derive(Row, Deserialize)]
        struct AvgRow {
            avg_ms: f64,
        }

        let mv_query = format!(
            "SELECT avg(verify_time_ms) AS avg_ms \
             FROM {db}.batch_verify_times_mv \
             WHERE verified_at >= now64() - INTERVAL {interval} \
               AND batch_id != 0",
            interval = range.interval(),
            db = self.db_name,
        );

        let rows = self.execute::<AvgRow>(&mv_query).await?;
        if let Some(row) = rows.into_iter().next() &&
            !row.avg_ms.is_nan()
        {
            return Ok(Some(row.avg_ms.round() as u64));
        }

        let fallback_query = format!(
            "SELECT avg((l1_verified.block_ts - l1_proved.block_ts) * 1000) AS avg_ms \
             FROM {db}.proved_batches pb \
             INNER JOIN {db}.verified_batches vb \
                ON pb.batch_id = vb.batch_id AND pb.block_hash = vb.block_hash \
             INNER JOIN {db}.l1_head_events l1_proved \
                ON pb.l1_block_number = l1_proved.l1_block_number \
             INNER JOIN {db}.l1_head_events l1_verified \
                ON vb.l1_block_number = l1_verified.l1_block_number \
             WHERE l1_verified.block_ts >= (toUInt64(now()) - {secs}) \
               AND l1_verified.block_ts > l1_proved.block_ts \
               AND (l1_verified.block_ts - l1_proved.block_ts) > 60 \
               AND pb.batch_id != 0",
            secs = range.seconds(),
            db = self.db_name,
        );

        let rows = self.execute::<AvgRow>(&fallback_query).await?;
        let row = match rows.into_iter().next() {
            Some(r) => r,
            None => return Ok(None),
        };

        if row.avg_ms.is_nan() { Ok(None) } else { Ok(Some(row.avg_ms.round() as u64)) }
    }

    /// Get the average interval in milliseconds between consecutive L2 blocks
    /// observed within the given range.
    pub async fn get_l2_block_cadence(
        &self,
        sequencer: Option<AddressBytes>,
        range: TimeRange,
    ) -> Result<Option<u64>> {
        #[derive(Row, Deserialize)]
        struct CadenceRow {
            min_ts: u64,
            max_ts: u64,
            cnt: u64,
        }

        let mut query = format!(
            "SELECT toUInt64(min(h.block_ts) * 1000) AS min_ts, \
                    toUInt64(max(h.block_ts) * 1000) AS max_ts, \
                    count() as cnt \
             FROM {db}.l2_head_events h \
             WHERE h.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval}) \
               AND {filter}",
            interval = range.interval(),
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );
        if let Some(addr) = sequencer {
            query.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
        }

        let rows = self.execute::<CadenceRow>(&query).await?;
        let row = match rows.into_iter().next() {
            Some(r) => r,
            None => return Ok(None),
        };

        if row.cnt > 1 && row.max_ts > row.min_ts {
            Ok(Some((row.max_ts - row.min_ts) / (row.cnt - 1)))
        } else {
            Ok(None)
        }
    }

    /// Get the average interval in milliseconds between consecutive batch
    /// proposals observed within the given range.
    pub async fn get_batch_posting_cadence(&self, range: TimeRange) -> Result<Option<u64>> {
        #[derive(Row, Deserialize)]
        struct CadenceRow {
            min_ts: u64,
            max_ts: u64,
            cnt: u64,
        }

        let query = format!(
            "SELECT toUInt64(min(l1_events.block_ts) * 1000) AS min_ts, \
                    toUInt64(max(l1_events.block_ts) * 1000) AS max_ts, \
                    count() as cnt \
             FROM {db}.batches b \
             INNER JOIN {db}.l1_head_events l1_events \
               ON b.l1_block_number = l1_events.l1_block_number \
             WHERE l1_events.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval})",
            interval = range.interval(),
            db = self.db_name,
        );

        let rows = self.execute::<CadenceRow>(&query).await?;
        let row = match rows.into_iter().next() {
            Some(r) => r,
            None => return Ok(None),
        };

        if row.cnt > 1 && row.max_ts > row.min_ts {
            Ok(Some((row.max_ts - row.min_ts) / (row.cnt - 1)))
        } else {
            Ok(None)
        }
    }

    /// Get the interval in milliseconds between consecutive batch proposals
    /// observed within the given range.
    pub async fn get_batch_posting_times(
        &self,
        range: TimeRange,
    ) -> Result<Vec<BatchPostingTimeRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            batch_id: u64,
            ts: u64,
            ms_since_prev_batch: Option<u64>,
        }

        let query = format!(
            "SELECT batch_id, ts, \
                    if(ts > prev_ts, CAST(ts - prev_ts AS UInt64), NULL) AS ms_since_prev_batch \
             FROM ( \
                 SELECT b.batch_id AS batch_id, \
                        toUInt64(l1_events.block_ts * 1000) AS ts, \
                        lagInFrame(toNullable(toUInt64(l1_events.block_ts * 1000))) \
                            OVER (ORDER BY l1_events.block_ts, b.batch_id) AS prev_ts \
                   FROM {db}.batches b \
                   INNER JOIN {db}.l1_head_events l1_events \
                     ON b.l1_block_number = l1_events.l1_block_number \
                  WHERE l1_events.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval}) \
                  ORDER BY l1_events.block_ts, b.batch_id \
             ) \
             WHERE prev_ts IS NOT NULL \
             ORDER BY ts",
            interval = range.interval(),
            db = self.db_name,
        );

        let rows = self.execute::<RawRow>(&query).await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let dt = Utc.timestamp_millis_opt(r.ts as i64).single()?;
                r.ms_since_prev_batch.map(|ms| BatchPostingTimeRow {
                    batch_id: r.batch_id,
                    inserted_at: dt,
                    ms_since_prev_batch: ms,
                })
            })
            .collect())
    }

    /// Get the interval between consecutive batch proposals since the given cutoff
    /// time with cursor-based pagination. Results are returned in descending order
    /// by batch id.
    pub async fn get_batch_posting_times_paginated(
        &self,
        since: DateTime<Utc>,
        limit: u64,
        starting_after: Option<u64>,
        ending_before: Option<u64>,
    ) -> Result<Vec<BatchPostingTimeRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            batch_id: u64,
            ts: u64,
            ms_since_prev_batch: Option<u64>,
        }

        let mut query = format!(
            "SELECT batch_id, ts, \
                    if(ts > prev_ts, CAST(ts - prev_ts AS UInt64), NULL) AS ms_since_prev_batch \
             FROM ( \
                 SELECT b.batch_id AS batch_id, \
                        toUInt64(l1_events.block_ts * 1000) AS ts, \
                        lagInFrame(toNullable(toUInt64(l1_events.block_ts * 1000))) \
                            OVER (ORDER BY l1_events.block_ts, b.batch_id) AS prev_ts \
                   FROM {db}.batches b \
                   INNER JOIN {db}.l1_head_events l1_events \
                     ON b.l1_block_number = l1_events.l1_block_number \
                  WHERE l1_events.block_ts >= {since} \
                  ORDER BY l1_events.block_ts, b.batch_id \
             ) \
             WHERE prev_ts IS NOT NULL",
            since = since.timestamp(),
            db = self.db_name,
        );
        if let Some(start) = starting_after {
            // For descending order we fetch records with id less than the
            // cursor provided in `starting_after`.
            query.push_str(&format!(" AND batch_id < {}", start));
        }
        if let Some(end) = ending_before {
            // When `ending_before` is set we only return records with id
            // greater than the provided cursor.
            query.push_str(&format!(" AND batch_id > {}", end));
        }
        query.push_str(" ORDER BY batch_id DESC");
        query.push_str(&format!(" LIMIT {}", limit));

        let rows = self.execute::<RawRow>(&query).await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let dt = Utc.timestamp_millis_opt(r.ts as i64).single()?;
                r.ms_since_prev_batch.map(|ms| BatchPostingTimeRow {
                    batch_id: r.batch_id,
                    inserted_at: dt,
                    ms_since_prev_batch: ms,
                })
            })
            .collect())
    }

    /// Get prove times in seconds for batches proved within the given range
    pub async fn get_prove_times(
        &self,
        range: TimeRange,
        bucket: Option<u64>,
    ) -> Result<Vec<BatchProveTimeRow>> {
        let bucket = bucket.unwrap_or(1);

        if bucket <= 1 {
            // Non-bucketed implementation
            let mv_query = format!(
                "SELECT batch_id, toUInt64(prove_time_ms / 1000) AS seconds_to_prove \
                 FROM {db}.batch_prove_times_mv \
                 WHERE proved_at >= now64() - INTERVAL {interval} \
                   AND batch_id != 0 \
                 ORDER BY batch_id ASC",
                interval = range.interval(),
                db = self.db_name,
            );

            let rows = self.execute::<BatchProveTimeRow>(&mv_query).await?;
            if !rows.is_empty() {
                return Ok(rows);
            }

            let fallback_query = format!(
                "SELECT toUInt64(b.batch_id) AS batch_id, \
                        (l1_proved.block_ts - l1_proposed.block_ts) AS seconds_to_prove \
                 FROM {db}.batches b \
                 JOIN {db}.proved_batches pb ON b.batch_id = pb.batch_id \
                 JOIN {db}.l1_head_events l1_proposed \
                   ON b.l1_block_number = l1_proposed.l1_block_number \
                 JOIN {db}.l1_head_events l1_proved \
                   ON pb.l1_block_number = l1_proved.l1_block_number \
                 WHERE l1_proved.block_ts >= (toUInt64(now()) - {secs}) \
                   AND b.batch_id != 0 \
                 ORDER BY b.batch_id ASC",
                secs = range.seconds(),
                db = self.db_name,
            );

            let rows = self.execute::<BatchProveTimeRow>(&fallback_query).await?;
            return Ok(rows);
        }

        // Bucketed implementation using SQL aggregation
        let mv_query = format!(
            "SELECT batch_bucket AS batch_id, \
                    toUInt64(avg(seconds_to_prove)) AS seconds_to_prove \
             FROM ( \
                SELECT intDiv(batch_id, {bucket}) * {bucket} AS batch_bucket, \
                       toUInt64(prove_time_ms / 1000) AS seconds_to_prove \
                FROM {db}.batch_prove_times_mv \
                WHERE proved_at >= now64() - INTERVAL {interval} \
                  AND batch_id != 0 \
             ) AS sub \
             GROUP BY batch_bucket \
             ORDER BY batch_bucket ASC",
            bucket = bucket,
            interval = range.interval(),
            db = self.db_name,
        );

        let rows = self.execute::<BatchProveTimeRow>(&mv_query).await?;
        if !rows.is_empty() {
            return Ok(rows);
        }

        // Fallback with bucketing
        let fallback_query = format!(
            "SELECT batch_bucket AS batch_id, \
                    toUInt64(avg(seconds_to_prove)) AS seconds_to_prove \
             FROM ( \
                SELECT intDiv(b.batch_id, {bucket}) * {bucket} AS batch_bucket, \
                       (l1_proved.block_ts - l1_proposed.block_ts) AS seconds_to_prove \
                FROM {db}.batches b \
                JOIN {db}.proved_batches pb ON b.batch_id = pb.batch_id \
                JOIN {db}.l1_head_events l1_proposed \
                  ON b.l1_block_number = l1_proposed.l1_block_number \
                JOIN {db}.l1_head_events l1_proved \
                  ON pb.l1_block_number = l1_proved.l1_block_number \
                WHERE l1_proved.block_ts >= (toUInt64(now()) - {secs}) \
                  AND b.batch_id != 0 \
             ) AS sub \
             GROUP BY batch_bucket \
             ORDER BY batch_bucket ASC",
            bucket = bucket,
            secs = range.seconds(),
            db = self.db_name,
        );

        let rows = self.execute::<BatchProveTimeRow>(&fallback_query).await?;
        Ok(rows)
    }

    /// Get prove times with cursor-based pagination
    /// Results are returned in descending order by batch id
    pub async fn get_prove_times_paginated(
        &self,
        since: DateTime<Utc>,
        limit: u64,
        starting_after: Option<u64>,
        ending_before: Option<u64>,
    ) -> Result<Vec<BatchProveTimeRow>> {
        // First try the materialized view
        let mut mv_query = format!(
            "SELECT batch_id, toUInt64(prove_time_ms / 1000) AS seconds_to_prove \
             FROM {db}.batch_prove_times_mv \
             WHERE proved_at >= toDateTime64({since}, 3) \
               AND batch_id != 0",
            since = since.timestamp(),
            db = self.db_name,
        );
        if let Some(start) = starting_after {
            mv_query.push_str(&format!(" AND batch_id < {}", start));
        }
        if let Some(end) = ending_before {
            mv_query.push_str(&format!(" AND batch_id > {}", end));
        }
        mv_query.push_str(" ORDER BY batch_id DESC");
        mv_query.push_str(&format!(" LIMIT {}", limit));

        let rows = self.execute::<BatchProveTimeRow>(&mv_query).await?;
        if !rows.is_empty() {
            return Ok(rows);
        }

        // Fallback to raw data if materialized view is empty
        let mut fallback_query = format!(
            "SELECT toUInt64(b.batch_id) AS batch_id, \
                    (l1_proved.block_ts - l1_proposed.block_ts) AS seconds_to_prove \
             FROM {db}.batches b \
             JOIN {db}.proved_batches pb ON b.batch_id = pb.batch_id \
             JOIN {db}.l1_head_events l1_proposed \
               ON b.l1_block_number = l1_proposed.l1_block_number \
             JOIN {db}.l1_head_events l1_proved \
               ON pb.l1_block_number = l1_proved.l1_block_number \
             WHERE l1_proved.block_ts >= {since} \
               AND b.batch_id != 0",
            since = since.timestamp(),
            db = self.db_name,
        );
        if let Some(start) = starting_after {
            fallback_query.push_str(&format!(" AND b.batch_id < {}", start));
        }
        if let Some(end) = ending_before {
            fallback_query.push_str(&format!(" AND b.batch_id > {}", end));
        }
        fallback_query.push_str(" ORDER BY b.batch_id DESC");
        fallback_query.push_str(&format!(" LIMIT {}", limit));

        let rows = self.execute::<BatchProveTimeRow>(&fallback_query).await?;
        Ok(rows)
    }

    /// Get verify times in seconds for batches verified within the given range
    pub async fn get_verify_times(
        &self,
        range: TimeRange,
        bucket: Option<u64>,
    ) -> Result<Vec<BatchVerifyTimeRow>> {
        let bucket = bucket.unwrap_or(1);

        if bucket <= 1 {
            let mv_query = format!(
                "SELECT batch_id, toUInt64(verify_time_ms / 1000) AS seconds_to_verify \
                 FROM {db}.batch_verify_times_mv \
                 WHERE verified_at >= now64() - INTERVAL {interval} \
                   AND verify_time_ms > 60000 \
                   AND batch_id != 0 \
                 ORDER BY batch_id ASC",
                interval = range.interval(),
                db = self.db_name,
            );

            let rows = self.execute::<BatchVerifyTimeRow>(&mv_query).await?;
            if !rows.is_empty() {
                return Ok(rows);
            }

            let fallback_query = format!(
                "SELECT toUInt64(pb.batch_id) AS batch_id, \
                        (l1_verified.block_ts - l1_proved.block_ts) AS seconds_to_verify \
                 FROM {db}.proved_batches pb \
                 INNER JOIN {db}.verified_batches vb \
                    ON pb.batch_id = vb.batch_id AND pb.block_hash = vb.block_hash \
                 INNER JOIN {db}.l1_head_events l1_proved \
                    ON pb.l1_block_number = l1_proved.l1_block_number \
                 INNER JOIN {db}.l1_head_events l1_verified \
                    ON vb.l1_block_number = l1_verified.l1_block_number \
                 WHERE l1_verified.block_ts >= (toUInt64(now()) - {secs}) \
                   AND l1_verified.block_ts > l1_proved.block_ts \
                   AND (l1_verified.block_ts - l1_proved.block_ts) > 60 \
                   AND pb.batch_id != 0",
                secs = range.seconds(),
                db = self.db_name,
            );

            return self.execute::<BatchVerifyTimeRow>(&fallback_query).await;
        }

        let mv_query = format!(
            "SELECT batch_bucket AS batch_id, \
                    toUInt64(avg(seconds_to_verify)) AS seconds_to_verify \
             FROM ( \
                SELECT intDiv(batch_id, {bucket}) * {bucket} AS batch_bucket, \
                       toUInt64(verify_time_ms / 1000) AS seconds_to_verify \
                FROM {db}.batch_verify_times_mv \
                WHERE verified_at >= now64() - INTERVAL {interval} \
                  AND verify_time_ms > 60000 \
                  AND batch_id != 0 \
             ) AS sub \
             GROUP BY batch_bucket \
             ORDER BY batch_bucket ASC",
            bucket = bucket,
            interval = range.interval(),
            db = self.db_name,
        );

        let rows = self.execute::<BatchVerifyTimeRow>(&mv_query).await?;
        if !rows.is_empty() {
            return Ok(rows);
        }

        let fallback_query = format!(
            "SELECT batch_bucket AS batch_id, \
                    toUInt64(avg(seconds_to_verify)) AS seconds_to_verify \
             FROM ( \
                SELECT intDiv(pb.batch_id, {bucket}) * {bucket} AS batch_bucket, \
                       (l1_verified.block_ts - l1_proved.block_ts) AS seconds_to_verify \
                FROM {db}.proved_batches pb \
                INNER JOIN {db}.verified_batches vb \
                   ON pb.batch_id = vb.batch_id AND pb.block_hash = vb.block_hash \
                INNER JOIN {db}.l1_head_events l1_proved \
                   ON pb.l1_block_number = l1_proved.l1_block_number \
                INNER JOIN {db}.l1_head_events l1_verified \
                   ON vb.l1_block_number = l1_verified.l1_block_number \
                WHERE l1_verified.block_ts >= (toUInt64(now()) - {secs}) \
                  AND l1_verified.block_ts > l1_proved.block_ts \
                  AND (l1_verified.block_ts - l1_proved.block_ts) > 60 \
                  AND pb.batch_id != 0 \
             ) AS sub \
             GROUP BY batch_bucket \
             ORDER BY batch_bucket ASC",
            bucket = bucket,
            secs = range.seconds(),
            db = self.db_name,
        );

        self.execute::<BatchVerifyTimeRow>(&fallback_query).await
    }

    /// Get verify times with cursor-based pagination
    /// Results are returned in descending order by batch id
    pub async fn get_verify_times_paginated(
        &self,
        since: DateTime<Utc>,
        limit: u64,
        starting_after: Option<u64>,
        ending_before: Option<u64>,
    ) -> Result<Vec<BatchVerifyTimeRow>> {
        let mut mv_query = format!(
            "SELECT batch_id, toUInt64(verify_time_ms / 1000) AS seconds_to_verify \
             FROM {db}.batch_verify_times_mv \
             WHERE verified_at >= toDateTime64({since}, 3) \
               AND verify_time_ms > 60000 \
               AND batch_id != 0",
            since = since.timestamp(),
            db = self.db_name,
        );
        if let Some(start) = starting_after {
            mv_query.push_str(&format!(" AND batch_id < {}", start));
        }
        if let Some(end) = ending_before {
            mv_query.push_str(&format!(" AND batch_id > {}", end));
        }
        mv_query.push_str(" ORDER BY batch_id DESC");
        mv_query.push_str(&format!(" LIMIT {}", limit));

        let rows = self.execute::<BatchVerifyTimeRow>(&mv_query).await?;
        if !rows.is_empty() {
            return Ok(rows);
        }

        let mut fallback_query = format!(
            "SELECT toUInt64(pb.batch_id) AS batch_id, \
                    (l1_verified.block_ts - l1_proved.block_ts) AS seconds_to_verify \
             FROM {db}.proved_batches pb \
             INNER JOIN {db}.verified_batches vb \
                ON pb.batch_id = vb.batch_id AND pb.block_hash = vb.block_hash \
             INNER JOIN {db}.l1_head_events l1_proved \
                ON pb.l1_block_number = l1_proved.l1_block_number \
             INNER JOIN {db}.l1_head_events l1_verified \
                ON vb.l1_block_number = l1_verified.l1_block_number \
             WHERE l1_verified.block_ts >= {since} \
               AND l1_verified.block_ts > l1_proved.block_ts \
               AND (l1_verified.block_ts - l1_proved.block_ts) > 60 \
               AND pb.batch_id != 0",
            since = since.timestamp(),
            db = self.db_name,
        );
        if let Some(start) = starting_after {
            fallback_query.push_str(&format!(" AND pb.batch_id < {}", start));
        }
        if let Some(end) = ending_before {
            fallback_query.push_str(&format!(" AND pb.batch_id > {}", end));
        }
        fallback_query.push_str(" ORDER BY pb.batch_id DESC");
        fallback_query.push_str(&format!(" LIMIT {}", limit));

        self.execute::<BatchVerifyTimeRow>(&fallback_query).await
    }

    /// Get L1 block numbers grouped by minute for the given range
    pub async fn get_l1_block_times(&self, range: TimeRange) -> Result<Vec<L1BlockTimeRow>> {
        let query = format!(
            "SELECT toUInt64(toStartOfMinute(fromUnixTimestamp64Milli(block_ts * 1000))) AS minute, \
                    max(l1_block_number) AS l1_block_number \
             FROM {db}.l1_head_events \
              WHERE block_ts >= toUnixTimestamp(now64() - INTERVAL {interval}) \
              GROUP BY minute \
              ORDER BY minute",
            interval = range.interval(),
            db = self.db_name,
        );

        let rows = self.execute::<L1BlockTimeRow>(&query).await?;
        Ok(rows)
    }

    /// Get the time between consecutive L2 blocks for the given range
    pub async fn get_l2_block_times(
        &self,
        sequencer: Option<AddressBytes>,
        range: TimeRange,
        bucket: Option<u64>,
    ) -> Result<Vec<L2BlockTimeRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            l2_block_number: u64,
            block_time: u64,
            s_since_prev_block: Option<u64>,
        }
        #[derive(Row, Deserialize)]
        struct AggRow {
            l2_block_number: u64,
            block_time: u64,
            s_since_prev_block: u64,
        }

        let bucket = bucket.unwrap_or(1);
        if bucket <= 1 {
            let mut query = format!(
                "WITH time_diffs AS ( \
                    SELECT h.l2_block_number, \
                           h.block_ts AS block_time, \
                           h.sequencer, \
                           toUInt64OrNull(toString( \
                               if(isNull(lagInFrame(h.block_ts) OVER (ORDER BY \
                                h.l2_block_number)), NULL, h.block_ts - lagInFrame(h.block_ts) OVER (ORDER BY \
                                h.l2_block_number)) \
                           )) AS s_since_prev_block \
                    FROM {db}.l2_head_events h \
                    WHERE {filter} \
                ) \
                SELECT l2_block_number, block_time, s_since_prev_block \
                FROM time_diffs \
                WHERE block_time >= toUnixTimestamp(now64() - INTERVAL {interval})",
                interval = range.interval(),
                filter = self.reorg_filter("h"),
                db = self.db_name,
            );
            if let Some(addr) = sequencer {
                query.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
            }
            query.push_str(" ORDER BY l2_block_number ASC");

            let rows = self.execute::<RawRow>(&query).await?;
            return Ok(rows
                .into_iter()
                .filter_map(|r| {
                    let dt = Utc.timestamp_opt(r.block_time as i64, 0).single()?;
                    r.s_since_prev_block.map(|s| L2BlockTimeRow {
                        l2_block_number: r.l2_block_number,
                        block_time: dt,
                        s_since_prev_block: s,
                    })
                })
                .collect());
        }

        let mut inner = format!(
            "WITH time_diffs AS ( \
                SELECT h.l2_block_number, \
                       h.block_ts AS block_time, \
                       h.sequencer, \
                       toUInt64OrNull(toString( \
                           if(isNull(lagInFrame(h.block_ts) OVER (ORDER BY \
                            h.l2_block_number)), NULL, h.block_ts - lagInFrame(h.block_ts) OVER (ORDER BY \
                            h.l2_block_number)) \
                       )) AS s_since_prev_block \
                FROM {db}.l2_head_events h \
                WHERE {filter} \
            ) \
            SELECT l2_block_number, block_time, s_since_prev_block \
            FROM time_diffs \
            WHERE block_time >= toUnixTimestamp(now64() - INTERVAL {interval})",
            interval = range.interval(),
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );
        if let Some(addr) = sequencer {
            inner.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
        }
        let query = format!(
            "SELECT intDiv(l2_block_number, {bucket}) * {bucket} AS l2_block_number, \
                    max(block_time) AS block_time, \
                    toUInt64(ifNull(avg(s_since_prev_block), 0)) AS s_since_prev_block \
             FROM ({inner}) as sub \
             GROUP BY l2_block_number \
             ORDER BY l2_block_number ASC",
            bucket = bucket,
            inner = inner,
        );

        let rows = self.execute::<AggRow>(&query).await?;
        Ok(rows
            .into_iter()
            .map(|r| L2BlockTimeRow {
                l2_block_number: r.l2_block_number,
                block_time: Utc.timestamp_opt(r.block_time as i64, 0).unwrap(),
                s_since_prev_block: r.s_since_prev_block,
            })
            .collect())
    }

    /// Get the time between consecutive L2 blocks for the specified block range
    pub async fn get_l2_block_times_block_range(
        &self,
        sequencer: Option<AddressBytes>,
        start_block: Option<u64>,
        end_block: Option<u64>,
        limit: u64,
        starting_after: Option<u64>,
        ending_before: Option<u64>,
    ) -> Result<Vec<L2BlockTimeRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            l2_block_number: u64,
            block_time: u64,
            s_since_prev_block: Option<u64>,
        }

        let mut query = format!(
            "WITH time_diffs AS ( \
                SELECT h.l2_block_number, \
                       h.block_ts AS block_time, \
                       h.sequencer, \
                       toUInt64OrNull(toString(\
                           if(isNull(lagInFrame(h.block_ts) OVER (ORDER BY \
                            h.l2_block_number)), NULL, h.block_ts - lagInFrame(h.block_ts) OVER (ORDER BY \
                            h.l2_block_number))\
                       )) AS s_since_prev_block \
                FROM {db}.l2_head_events h \
                WHERE {filter} \
            ) \
            SELECT l2_block_number, block_time, s_since_prev_block \
            FROM time_diffs \
            WHERE 1=1",
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );

        if let Some(start) = start_block {
            query.push_str(&format!(" AND l2_block_number >= {}", start));
        }

        if let Some(end) = end_block {
            query.push_str(&format!(" AND l2_block_number <= {}", end));
        }

        if let Some(addr) = sequencer {
            query.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
        }

        if let Some(start) = starting_after {
            query.push_str(&format!(" AND l2_block_number < {}", start));
        }

        if let Some(end) = ending_before {
            query.push_str(&format!(" AND l2_block_number > {}", end));
        }

        query.push_str(" ORDER BY l2_block_number DESC");
        query.push_str(&format!(" LIMIT {}", limit));

        let rows = self.execute::<RawRow>(&query).await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let dt = Utc.timestamp_opt(r.block_time as i64, 0).single()?;
                r.s_since_prev_block.map(|s| L2BlockTimeRow {
                    l2_block_number: r.l2_block_number,
                    block_time: dt,
                    s_since_prev_block: s,
                })
            })
            .collect())
    }

    /// Get the average number of L2 transactions per second for the given range
    pub async fn get_avg_l2_tps(
        &self,
        sequencer: Option<AddressBytes>,
        range: TimeRange,
    ) -> Result<Option<f64>> {
        #[derive(Row, Deserialize)]
        struct TpsRow {
            min_ts: u64,
            max_ts: u64,
            tx_sum: u64,
        }

        let mut query = format!(
            "SELECT toUInt64(min(h.block_ts)) AS min_ts, \
                    toUInt64(max(h.block_ts)) AS max_ts, \
                    sum(sum_tx) AS tx_sum \
             FROM {db}.l2_head_events h \
             WHERE h.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval}) \
               AND {filter}",
            interval = range.interval(),
            filter = self.reorg_filter("h"),
            db = self.db_name
        );
        if let Some(addr) = sequencer {
            query.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
        }

        let rows = self.execute::<TpsRow>(&query).await?;
        let row = match rows.into_iter().next() {
            Some(r) => r,
            None => return Ok(None),
        };

        if row.max_ts > row.min_ts && row.tx_sum > 0 {
            let duration = (row.max_ts - row.min_ts) as f64;
            Ok(Some(row.tx_sum as f64 / duration))
        } else {
            Ok(None)
        }
    }

    /// Get the gas used for each L2 block within the given range
    pub async fn get_l2_gas_used(
        &self,
        sequencer: Option<AddressBytes>,
        range: TimeRange,
        bucket: Option<u64>,
    ) -> Result<Vec<L2GasUsedRow>> {
        let bucket = bucket.unwrap_or(1);

        if bucket <= 1 {
            // Non-bucketed implementation
            #[derive(Row, Deserialize)]
            struct RawRow {
                l2_block_number: u64,
                block_time: u64,
                gas_used: u64,
            }

            let mut query = format!(
                "SELECT h.l2_block_number, h.block_ts AS block_time, toUInt64(sum_gas_used) AS gas_used \
                 FROM {db}.l2_head_events h \
                 WHERE h.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval}) \
                   AND {filter}",
                interval = range.interval(),
                filter = self.reorg_filter("h"),
                db = self.db_name,
            );
            if let Some(addr) = sequencer {
                query.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
            }
            query.push_str(" ORDER BY l2_block_number ASC");

            let rows = self.execute::<RawRow>(&query).await?;
            return Ok(rows
                .into_iter()
                .map(|r| {
                    let dt = Utc.timestamp_opt(r.block_time as i64, 0).unwrap();
                    L2GasUsedRow {
                        l2_block_number: r.l2_block_number,
                        block_time: dt,
                        gas_used: r.gas_used,
                    }
                })
                .collect());
        }

        // Bucketed implementation using SQL aggregation
        #[derive(Row, Deserialize)]
        struct AggRow {
            l2_block_number: u64,
            block_time: u64,
            gas_used: u64,
        }

        let mut inner = format!(
            "SELECT h.l2_block_number, h.block_ts AS block_time, toUInt64(sum_gas_used) AS gas_used \
             FROM {db}.l2_head_events h \
             WHERE h.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval}) \
               AND {filter}",
            interval = range.interval(),
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );
        if let Some(addr) = sequencer {
            inner.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
        }

        let query = format!(
            "SELECT l2_bucket AS l2_block_number, \
                    max(block_time) AS block_time, \
                    toUInt64(avg(gas_used)) AS gas_used \
             FROM ( \
                SELECT intDiv(l2_block_number, {bucket}) * {bucket} AS l2_bucket, \
                       block_time, \
                       gas_used \
                FROM ({inner}) AS base \
             ) AS sub \
             GROUP BY l2_bucket \
             ORDER BY l2_bucket ASC",
            bucket = bucket,
            inner = inner,
        );

        let rows = self.execute::<AggRow>(&query).await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let dt = Utc.timestamp_opt(r.block_time as i64, 0).unwrap();
                L2GasUsedRow {
                    l2_block_number: r.l2_block_number,
                    block_time: dt,
                    gas_used: r.gas_used,
                }
            })
            .collect())
    }

    /// Get the gas used for each L2 block within the specified block range
    pub async fn get_l2_gas_used_block_range(
        &self,
        sequencer: Option<AddressBytes>,
        start_block: Option<u64>,
        end_block: Option<u64>,
        limit: u64,
        starting_after: Option<u64>,
        ending_before: Option<u64>,
    ) -> Result<Vec<L2GasUsedRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            l2_block_number: u64,
            block_time: u64,
            gas_used: u64,
        }

        let mut query = format!(
            "SELECT h.l2_block_number, h.block_ts AS block_time, toUInt64(sum_gas_used) AS gas_used \
             FROM {db}.l2_head_events h \
             WHERE {filter}",
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );

        if let Some(start) = start_block {
            query.push_str(&format!(" AND h.l2_block_number >= {}", start));
        }

        if let Some(end) = end_block {
            query.push_str(&format!(" AND h.l2_block_number <= {}", end));
        }

        if let Some(addr) = sequencer {
            query.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
        }

        if let Some(start) = starting_after {
            query.push_str(&format!(" AND l2_block_number < {}", start));
        }

        if let Some(end) = ending_before {
            query.push_str(&format!(" AND l2_block_number > {}", end));
        }

        query.push_str(" ORDER BY l2_block_number DESC");
        query.push_str(&format!(" LIMIT {}", limit));

        let rows = self.execute::<RawRow>(&query).await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let dt = Utc.timestamp_opt(r.block_time as i64, 0).unwrap();
                L2GasUsedRow {
                    l2_block_number: r.l2_block_number,
                    block_time: dt,
                    gas_used: r.gas_used,
                }
            })
            .collect())
    }
    /// Get the L1 data posting cost for each block within the given range
    pub async fn get_l1_data_costs(&self, range: TimeRange) -> Result<Vec<L1DataCostRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            l1_block_number: u64,
            cost: u128,
        }

        // Group by l1_block_number and sum costs since L1DataCostRow only has these 2 fields
        let query = format!(
            "SELECT c.l1_block_number, sum(c.cost) as cost \
         FROM {db}.l1_data_costs c \
         INNER JOIN {db}.l1_head_events h \
           ON c.l1_block_number = h.l1_block_number \
         WHERE h.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval}) \
         GROUP BY c.l1_block_number \
         ORDER BY c.l1_block_number ASC",
            interval = range.interval(),
            db = self.db_name,
        );

        let rows = self.execute::<RawRow>(&query).await?;
        Ok(rows
            .into_iter()
            .map(|r| L1DataCostRow { l1_block_number: r.l1_block_number, cost: r.cost })
            .collect())
    }

    /// Get the L1 data posting cost since the given cutoff time with cursor-based pagination.
    /// Results are returned in descending order by block number.
    pub async fn get_l1_data_costs_paginated(
        &self,
        since: DateTime<Utc>,
        limit: u64,
        starting_after: Option<u64>,
        ending_before: Option<u64>,
    ) -> Result<Vec<L1DataCostRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            l1_block_number: u64,
            cost: u128,
        }

        // First join with l1_head_events to filter by time, then group by l1_block_number
        let mut query = format!(
            "SELECT c.l1_block_number, sum(c.cost) as cost \
         FROM {db}.l1_data_costs c \
         INNER JOIN {db}.l1_head_events h \
           ON c.l1_block_number = h.l1_block_number \
         WHERE h.block_ts >= {since}",
            since = since.timestamp(),
            db = self.db_name,
        );
        if let Some(start) = starting_after {
            query.push_str(&format!(" AND c.l1_block_number < {}", start));
        }
        if let Some(end) = ending_before {
            query.push_str(&format!(" AND c.l1_block_number > {}", end));
        }
        query.push_str(" GROUP BY c.l1_block_number");
        query.push_str(" ORDER BY c.l1_block_number DESC");
        query.push_str(&format!(" LIMIT {}", limit));

        let rows = self.execute::<RawRow>(&query).await?;
        Ok(rows
            .into_iter()
            .map(|r| L1DataCostRow { l1_block_number: r.l1_block_number, cost: r.cost })
            .collect())
    }

    /// Get the total L1 data posting cost for the given range
    pub async fn get_l1_total_data_cost(
        &self,
        sequencer: Option<AddressBytes>,
        range: TimeRange,
    ) -> Result<Option<u128>> {
        #[derive(Row, Deserialize)]
        struct SumRow {
            total: u128,
        }

        let mut query = format!(
            "SELECT sum(c.cost) AS total \
             FROM {db}.l1_data_costs c \
             INNER JOIN {db}.batches b \
               ON c.batch_id = b.batch_id AND c.l1_block_number = b.l1_block_number \
             INNER JOIN {db}.l1_head_events l1 \
               ON b.l1_block_number = l1.l1_block_number \
             WHERE l1.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval})",
            interval = range.interval(),
            db = self.db_name,
        );
        if let Some(addr) = sequencer {
            query.push_str(&format!(" AND b.proposer_addr = unhex('{}')", encode(addr)));
        }

        let rows = self.execute::<SumRow>(&query).await?;
        let row = match rows.into_iter().next() {
            Some(r) => r,
            None => return Ok(None),
        };
        Ok(Some(row.total))
    }

    /// Get priority fee, base fee and L1 data cost for each L2 block
    pub async fn get_l2_fee_components(
        &self,
        sequencer: Option<AddressBytes>,
        range: TimeRange,
        bucket: Option<u64>,
    ) -> Result<Vec<BlockFeeComponentRow>> {
        let bucket = bucket.unwrap_or(1);

        if bucket <= 1 {
            // Non-bucketed implementation
            #[derive(Row, Deserialize)]
            struct RawRow {
                l2_block_number: u64,
                priority_fee: u128,
                base_fee: u128,
                l1_data_cost: Option<u128>,
            }

            let mut query = format!(
                "SELECT h.l2_block_number, \
                        sum_priority_fee AS priority_fee, \
                        sum_base_fee AS base_fee, \
                        toNullable(if(b.batch_size > 0, intDiv(dc.cost, b.batch_size), NULL)) AS l1_data_cost \
                 FROM {db}.l2_head_events h \
                 LEFT JOIN (SELECT DISTINCT batch_id, l2_block_number FROM {db}.batch_blocks) bb \
                   ON h.l2_block_number = bb.l2_block_number \
                 LEFT JOIN {db}.batches b \
                   ON bb.batch_id = b.batch_id \
                 LEFT JOIN {db}.l1_data_costs dc \
                   ON b.batch_id = dc.batch_id AND b.l1_block_number = dc.l1_block_number \
                 WHERE h.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval}) \
                   AND {filter}",
                interval = range.interval(),
                filter = self.reorg_filter("h"),
                db = self.db_name,
            );
            if let Some(addr) = sequencer {
                query.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
            }
            query.push_str(" ORDER BY l2_block_number ASC");

            let rows = self.execute::<RawRow>(&query).await?;
            return Ok(rows
                .into_iter()
                .map(|r| BlockFeeComponentRow {
                    l2_block_number: r.l2_block_number,
                    priority_fee: r.priority_fee,
                    base_fee: r.base_fee,
                    l1_data_cost: r.l1_data_cost,
                })
                .collect());
        }

        // Bucketed implementation using SQL aggregation
        #[derive(Row, Deserialize)]
        struct AggRow {
            l2_block_number: u64,
            priority_fee: u128,
            base_fee: u128,
            l1_data_cost: Option<u128>,
        }

        let mut inner = format!(
            "SELECT h.l2_block_number, \
                    sum_priority_fee AS priority_fee, \
                    sum_base_fee AS base_fee, \
                    toNullable(if(b.batch_size > 0, intDiv(dc.cost, b.batch_size), NULL)) AS l1_data_cost \
             FROM {db}.l2_head_events h \
             LEFT JOIN (SELECT DISTINCT batch_id, l2_block_number FROM {db}.batch_blocks) bb \
               ON h.l2_block_number = bb.l2_block_number \
             LEFT JOIN {db}.batches b \
               ON bb.batch_id = b.batch_id \
             LEFT JOIN {db}.l1_data_costs dc \
               ON b.batch_id = dc.batch_id AND b.l1_block_number = dc.l1_block_number \
             WHERE h.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval}) \
               AND {filter}",
            interval = range.interval(),
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );
        if let Some(addr) = sequencer {
            inner.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
        }

        let query = format!(
            "SELECT l2_bucket AS l2_block_number, \
                    sum(priority_fee) AS priority_fee, \
                    sum(base_fee) AS base_fee, \
                    if(sum(if(l1_data_cost IS NOT NULL, 1, 0)) > 0, sum(l1_data_cost), NULL) AS l1_data_cost \
             FROM ( \
                SELECT intDiv(l2_block_number, {bucket}) * {bucket} AS l2_bucket, \
                       priority_fee, \
                       base_fee, \
                       l1_data_cost \
                FROM ({inner}) AS base \
             ) AS sub \
             GROUP BY l2_bucket \
             ORDER BY l2_bucket ASC",
            bucket = bucket,
            inner = inner,
        );

        let rows = self.execute::<AggRow>(&query).await?;
        Ok(rows
            .into_iter()
            .map(|r| BlockFeeComponentRow {
                l2_block_number: r.l2_block_number,
                priority_fee: r.priority_fee,
                base_fee: r.base_fee,
                l1_data_cost: r.l1_data_cost,
            })
            .collect())
    }

    /// Get priority fee, base fee and L1 data cost for each batch
    pub async fn get_batch_fee_components(
        &self,
        proposer: Option<AddressBytes>,
        range: TimeRange,
    ) -> Result<Vec<BatchFeeComponentRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            batch_id: u64,
            l1_block_number: u64,
            l1_tx_hash: HashBytes,
            proposer: AddressBytes,
            priority_fee: u128,
            base_fee: u128,
            l1_data_cost: Option<u128>,
            prove_cost: Option<u128>,
        }

        let client = self.base.clone();
        let proposer_clause = proposer
            .map(|addr| format!("AND b.proposer_addr = unhex('{}')", encode(addr)))
            .unwrap_or_default();
        let query = format!(
            r#"
WITH recent_batches AS (
    SELECT
        b.batch_id,
        b.l1_block_number,
        b.l1_tx_hash,
        b.proposer_addr
    FROM ?.batches b
    INNER JOIN ?.l1_head_events l1 ON b.l1_block_number = l1.l1_block_number
    WHERE l1.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval})
    {proposer_clause}
),
recent_batch_blocks AS (
    SELECT DISTINCT bb.batch_id, bb.l2_block_number
    FROM ?.batch_blocks bb
    INNER JOIN recent_batches rb USING (batch_id)
)
SELECT
    rb.batch_id,
    rb.l1_block_number,
    rb.l1_tx_hash,
    rb.proposer_addr AS proposer,
    coalesce(sum(h.sum_priority_fee), toUInt128(0)) AS priority_fee,
    coalesce(sum(h.sum_base_fee),   toUInt128(0)) AS base_fee,
    toNullable(max(dc.cost)) AS l1_data_cost,
    toNullable(max(pc.cost)) AS prove_cost
FROM recent_batches rb
LEFT JOIN recent_batch_blocks bb USING (batch_id)
LEFT JOIN ?.l2_head_events h
       ON bb.l2_block_number = h.l2_block_number
      AND {filter}
LEFT JOIN ?.l1_data_costs dc
       ON rb.batch_id = dc.batch_id AND rb.l1_block_number = dc.l1_block_number
LEFT JOIN ?.prove_costs pc
       ON rb.batch_id = pc.batch_id
GROUP BY rb.batch_id, rb.l1_block_number, rb.l1_tx_hash, rb.proposer_addr
ORDER BY rb.batch_id ASC
"#,
            interval = range.interval(),
            proposer_clause = proposer_clause,
            filter = self.reorg_filter("h"),
        );

        let start = Instant::now();
        let result = client
            .query(&query)
            .bind(Identifier(&self.db_name))
            .bind(Identifier(&self.db_name))
            .bind(Identifier(&self.db_name))
            .bind(Identifier(&self.db_name))
            .bind(Identifier(&self.db_name))
            .bind(Identifier(&self.db_name))
            .fetch_all::<RawRow>()
            .await;

        let duration_ms = start.elapsed().as_millis();
        match &result {
            Ok(rows) => {
                debug!(query = %query, duration_ms, rows = rows.len(), "ClickHouse query executed")
            }
            Err(e) => error!(query = %query, duration_ms, error = %e, "ClickHouse query failed"),
        }

        let rows = result?;
        Ok(rows
            .into_iter()
            .map(|r| BatchFeeComponentRow {
                batch_id: r.batch_id,
                l1_block_number: r.l1_block_number,
                l1_tx_hash: r.l1_tx_hash,
                sequencer: r.proposer,
                priority_fee: r.priority_fee,
                base_fee: r.base_fee,
                l1_data_cost: r.l1_data_cost,
                prove_cost: r.prove_cost,
            })
            .collect())
    }

    /// Get the total priority fee for the given range aggregated by batch
    pub async fn get_batch_priority_fee(
        &self,
        proposer: Option<AddressBytes>,
        range: TimeRange,
    ) -> Result<Option<u128>> {
        let rows = self.get_batch_fee_components(proposer, range).await?;
        let total: u128 = rows.iter().map(|r| r.priority_fee).sum();
        Ok((total > 0).then_some(total))
    }

    /// Get the total base fee for the given range aggregated by batch
    pub async fn get_batch_base_fee(
        &self,
        proposer: Option<AddressBytes>,
        range: TimeRange,
    ) -> Result<Option<u128>> {
        let rows = self.get_batch_fee_components(proposer, range).await?;
        let total: u128 = rows.iter().map(|r| r.base_fee).sum();
        Ok((total > 0).then_some(total))
    }

    /// Get the total L1 data cost for the given range aggregated by batch
    pub async fn get_batch_total_data_cost(
        &self,
        proposer: Option<AddressBytes>,
        range: TimeRange,
    ) -> Result<Option<u128>> {
        let rows = self.get_batch_fee_components(proposer, range).await?;
        let total: u128 = rows.iter().map(|r| r.l1_data_cost.unwrap_or(0)).sum();
        Ok((total > 0).then_some(total))
    }

    /// Get aggregated prove costs grouped by proposer for the given range
    pub async fn get_prove_costs_by_proposer(
        &self,
        range: TimeRange,
    ) -> Result<Vec<(AddressBytes, u128)>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            proposer: AddressBytes,
            total_cost: u128,
        }

        let query = format!(
            "SELECT b.proposer_addr AS proposer, \
                    sum(pc.cost) AS total_cost \
             FROM {db}.prove_costs pc \
             INNER JOIN {db}.batches b ON pc.batch_id = b.batch_id \
             INNER JOIN {db}.l1_head_events l1 ON pc.l1_block_number = l1.l1_block_number \
             WHERE l1.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval}) \
             GROUP BY b.proposer_addr \
             ORDER BY total_cost DESC",
            interval = range.interval(),
            db = self.db_name,
        );

        let rows = self.execute::<RawRow>(&query).await?;
        Ok(rows.into_iter().map(|r| (r.proposer, r.total_cost)).collect())
    }

    /// Get aggregated batch fees grouped by proposer for the given range
    pub async fn get_batch_fees_by_proposer(
        &self,
        range: TimeRange,
    ) -> Result<Vec<SequencerFeeRow>> {
        let query = format!(
            "SELECT b.proposer_addr AS proposer, \
                    sum(h.sum_priority_fee) AS priority_fee, \
                    sum(h.sum_base_fee) AS base_fee, \
                    coalesce(sum(if(b.batch_size > 0, intDiv(dc.cost, b.batch_size), NULL)), toUInt128(0)) AS l1_data_cost, \
                    coalesce(sum(if(b.batch_size > 0, intDiv(pc.cost, b.batch_size), NULL)), toUInt128(0)) AS prove_cost \
             FROM (SELECT DISTINCT batch_id, l2_block_number FROM {db}.batch_blocks) bb \
             INNER JOIN {db}.batches b \
               ON bb.batch_id = b.batch_id \
             INNER JOIN {db}.l1_head_events l1 \
               ON b.l1_block_number = l1.l1_block_number \
             LEFT JOIN {db}.l2_head_events h \
               ON bb.l2_block_number = h.l2_block_number \
             LEFT JOIN {db}.l1_data_costs dc \
               ON b.batch_id = dc.batch_id \
             LEFT JOIN {db}.prove_costs pc \
               ON b.batch_id = pc.batch_id \
             WHERE l1.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval}) \
               AND {filter} \
             GROUP BY b.proposer_addr \
             ORDER BY priority_fee DESC",
            interval = range.interval(),
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );

        let rows = self.execute::<SequencerFeeRow>(&query).await?;
        Ok(rows)
    }

    /// Get prover cost since the given cutoff time with cursor-based pagination
    /// Results are returned in descending order by batch id
    pub async fn get_prove_costs_paginated(
        &self,
        since: DateTime<Utc>,
        limit: u64,
        starting_after: Option<u64>,
        ending_before: Option<u64>,
    ) -> Result<Vec<ProveCostRow>> {
        let mut query = format!(
            "SELECT pc.l1_block_number, pc.batch_id, pc.cost \
         FROM {db}.prove_costs pc \
         INNER JOIN {db}.l1_head_events h \
           ON pc.l1_block_number = h.l1_block_number \
         WHERE h.block_ts >= toUnixTimestamp(fromUnixTimestamp({since})) \
           AND pc.cost > 0", // Only return non-zero costs
            since = since.timestamp(),
            db = self.db_name,
        );
        if let Some(start) = starting_after {
            query.push_str(&format!(" AND pc.batch_id < {}", start));
        }
        if let Some(end) = ending_before {
            query.push_str(&format!(" AND pc.batch_id > {}", end));
        }
        query.push_str(" ORDER BY pc.batch_id DESC");
        query.push_str(&format!(" LIMIT {}", limit));

        let rows = self.execute::<ProveCostRow>(&query).await?;
        Ok(rows)
    }

    /// Get the total prover cost for the given range
    pub async fn get_total_prove_cost(
        &self,
        sequencer: Option<AddressBytes>,
        range: TimeRange,
    ) -> Result<Option<u128>> {
        #[derive(Row, Deserialize)]
        struct SumRow {
            total: u128,
        }

        let mut query = format!(
            "SELECT sum(pc.cost) AS total \
             FROM {db}.prove_costs pc \
             INNER JOIN {db}.batches b ON pc.batch_id = b.batch_id \
             INNER JOIN {db}.l1_head_events l1 ON pc.l1_block_number = l1.l1_block_number \
             WHERE l1.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval})",
            interval = range.interval(),
            db = self.db_name,
        );
        if let Some(addr) = sequencer {
            query.push_str(&format!(" AND b.proposer_addr = unhex('{}')", encode(addr)));
        }

        let rows = self.execute::<SumRow>(&query).await?;
        let row = match rows.into_iter().next() {
            Some(r) => r,
            None => return Ok(None),
        };
        Ok(Some(row.total))
    }

    /// Find missing L1 block numbers within a range (for gap detection)
    pub async fn find_missing_l1_blocks(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> Result<Vec<u64>> {
        #[derive(Row, Deserialize)]
        struct BlockNumber {
            number: u64,
        }

        let query = format!(
            "SELECT number
             FROM numbers({}, {})
             WHERE number NOT IN (
                 SELECT l1_block_number
                 FROM {db}.l1_head_events
                 WHERE l1_block_number >= {} AND l1_block_number <= {}
             )
             ORDER BY number",
            start_block,
            end_block - start_block + 1,
            start_block,
            end_block,
            db = self.db_name,
        );

        let rows =
            self.base.query(&query).fetch_all::<BlockNumber>().await.map_err(eyre::Error::from)?;

        Ok(rows.into_iter().map(|row| row.number).collect())
    }

    /// Find missing L2 block numbers within a range (for gap detection)
    pub async fn find_missing_l2_blocks(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> Result<Vec<u64>> {
        #[derive(Row, Deserialize)]
        struct BlockNumber {
            number: u64,
        }

        let query = format!(
            "SELECT number
             FROM numbers({}, {})
             WHERE number NOT IN (
                 SELECT l2_block_number
                 FROM {db}.l2_head_events
                 WHERE l2_block_number >= {} AND l2_block_number <= {}
             )
             ORDER BY number",
            start_block,
            end_block - start_block + 1,
            start_block,
            end_block,
            db = self.db_name,
        );

        let rows =
            self.base.query(&query).fetch_all::<BlockNumber>().await.map_err(eyre::Error::from)?;

        Ok(rows.into_iter().map(|row| row.number).collect())
    }

    /// Get the latest L1 block number in the database
    pub async fn get_latest_l1_block(&self) -> Result<Option<u64>> {
        #[derive(Row, Deserialize)]
        struct MaxBlock {
            max_block: Option<u64>,
        }

        let query = format!(
            "SELECT if(count() = 0, NULL, max(l1_block_number)) as max_block FROM {db}.l1_head_events",
            db = self.db_name
        );

        // Use the execute method which handles errors more gracefully
        match self.execute::<MaxBlock>(&query).await {
            Ok(rows) => {
                if let Some(row) = rows.into_iter().next() {
                    Ok(row.max_block)
                } else {
                    Ok(None)
                }
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("doesn't exist") || error_msg.contains("Unknown table") {
                    // Table doesn't exist, return None to indicate no data
                    Ok(None)
                } else if error_msg.contains("tag for enum is not valid") ||
                    error_msg.contains("deserialization")
                {
                    // Deserialization issue, likely due to schema mismatch - return None for now
                    tracing::warn!(
                        query = %query,
                        error = %error_msg,
                        "ClickHouse deserialization error in get_latest_l1_block - likely schema mismatch, returning None"
                    );
                    Ok(None)
                } else {
                    Err(e.wrap_err("Failed to query latest L1 block"))
                }
            }
        }
    }

    /// Get the latest L2 block number in the database
    pub async fn get_latest_l2_block(&self) -> Result<Option<u64>> {
        #[derive(Row, Deserialize)]
        struct MaxBlock {
            max_block: Option<u64>,
        }

        let query = format!(
            "SELECT if(count() = 0, NULL, max(l2_block_number)) as max_block FROM {db}.l2_head_events",
            db = self.db_name
        );

        // Use the execute method which handles errors more gracefully
        match self.execute::<MaxBlock>(&query).await {
            Ok(rows) => {
                if let Some(row) = rows.into_iter().next() {
                    Ok(row.max_block)
                } else {
                    Ok(None)
                }
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("doesn't exist") || error_msg.contains("Unknown table") {
                    // Table doesn't exist, return None to indicate no data
                    Ok(None)
                } else if error_msg.contains("tag for enum is not valid") ||
                    error_msg.contains("deserialization")
                {
                    // Deserialization issue, likely due to schema mismatch - return None for now
                    tracing::warn!(
                        query = %query,
                        error = %error_msg,
                        "ClickHouse deserialization error in get_latest_l2_block - likely schema mismatch, returning None"
                    );
                    Ok(None)
                } else {
                    Err(e.wrap_err("Failed to query latest L2 block"))
                }
            }
        }
    }

    /// Get the earliest L1 block number in the database
    pub async fn get_earliest_l1_block(&self) -> Result<Option<u64>> {
        #[derive(Row, Deserialize)]
        struct MinBlock {
            #[serde(rename = "min(l1_block_number)")]
            min_block: Option<u64>,
        }

        let query = format!("SELECT min(l1_block_number) FROM {}.l1_head_events", self.db_name);

        let row =
            self.base.query(&query).fetch_one::<MinBlock>().await.map_err(eyre::Error::from)?;

        Ok(row.min_block)
    }

    /// Get the earliest L2 block number in the database
    pub async fn get_earliest_l2_block(&self) -> Result<Option<u64>> {
        #[derive(Row, Deserialize)]
        struct MinBlock {
            #[serde(rename = "min(l2_block_number)")]
            min_block: Option<u64>,
        }

        let query = format!("SELECT min(l2_block_number) FROM {}.l2_head_events", self.db_name);

        let row =
            self.base.query(&query).fetch_one::<MinBlock>().await.map_err(eyre::Error::from)?;

        Ok(row.min_block)
    }

    /// Get the transactions per second for each L2 block within the given range
    pub async fn get_l2_tps(
        &self,
        sequencer: Option<AddressBytes>,
        range: TimeRange,
        bucket: Option<u64>,
    ) -> Result<Vec<L2TpsRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            l2_block_number: u64,
            sum_tx: u32,
            s_since_prev_block: Option<u64>,
        }
        #[derive(Row, Deserialize)]
        struct AggRow {
            l2_block_number: u64,
            tps: f64,
        }

        let bucket = bucket.unwrap_or(1);
        if bucket <= 1 {
            let mut query = format!(
                "SELECT h.l2_block_number, sum_tx, \
                        toUInt64OrNull(toString((h.block_ts - \
                            lagInFrame(h.block_ts) OVER (ORDER BY h.l2_block_number)))) \
                            AS s_since_prev_block \
                 FROM {db}.l2_head_events h \
                 WHERE h.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval}) \
                   AND {filter}",
                interval = range.interval(),
                filter = self.reorg_filter("h"),
                db = self.db_name,
            );
            if let Some(addr) = sequencer {
                query.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
            }
            query.push_str(" ORDER BY l2_block_number ASC");

            let rows = self.execute::<RawRow>(&query).await?;
            return Ok(rows
                .into_iter()
                .filter_map(|r| {
                    let s = r.s_since_prev_block?;
                    if s == 0 {
                        return None;
                    }
                    Some(L2TpsRow {
                        l2_block_number: r.l2_block_number,
                        tps: r.sum_tx as f64 / s as f64,
                    })
                })
                .collect());
        }

        let mut inner = format!(
            "SELECT h.l2_block_number, \
                    sum_tx, \
                    toUInt64OrNull(toString(if(isNull(lagInFrame(h.block_ts) OVER (ORDER BY h.l2_block_number)), NULL, h.block_ts - lagInFrame(h.block_ts) OVER (ORDER BY h.l2_block_number)))) AS s_since_prev_block \
             FROM {db}.l2_head_events h \
             WHERE h.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval}) \
               AND {filter}",
            interval = range.interval(),
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );
        if let Some(addr) = sequencer {
            inner.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
        }
        let query = format!(
            "SELECT l2_bucket AS l2_block_number, \
                    ifNull(avg(tps), 0.0) AS tps \
             FROM ( \
                SELECT intDiv(l2_block_number, {bucket}) * {bucket} AS l2_bucket, \
                       toFloat64(sum_tx) / s_since_prev_block AS tps \
                  FROM ({inner}) AS base \
                 WHERE s_since_prev_block > 0 \
             ) AS sub \
             GROUP BY l2_bucket \
             ORDER BY l2_bucket ASC",
            bucket = bucket,
            inner = inner,
        );

        let rows = self.execute::<AggRow>(&query).await?;
        Ok(rows
            .into_iter()
            .map(|r| L2TpsRow { l2_block_number: r.l2_block_number, tps: r.tps })
            .collect())
    }

    /// Get the transactions per second for each L2 block within the specified block range
    pub async fn get_l2_tps_block_range(
        &self,
        sequencer: Option<AddressBytes>,
        start_block: Option<u64>,
        end_block: Option<u64>,
        limit: u64,
        starting_after: Option<u64>,
        ending_before: Option<u64>,
    ) -> Result<Vec<L2TpsRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            l2_block_number: u64,
            sum_tx: u32,
            s_since_prev_block: Option<u64>,
        }

        let mut query = format!(
            "SELECT h.l2_block_number, sum_tx, \
                    toUInt64OrNull(toString(if(isNull(lagInFrame(h.block_ts) OVER (ORDER BY h.l2_block_number)), NULL, h.block_ts - lagInFrame(h.block_ts) OVER (ORDER BY h.l2_block_number)))) \
                        AS s_since_prev_block \
             FROM {db}.l2_head_events h \
             WHERE {filter}",
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );

        if let Some(start) = start_block {
            query.push_str(&format!(" AND h.l2_block_number >= {}", start));
        }

        if let Some(end) = end_block {
            query.push_str(&format!(" AND h.l2_block_number <= {}", end));
        }

        if let Some(addr) = sequencer {
            query.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
        }

        if let Some(start) = starting_after {
            query.push_str(&format!(" AND l2_block_number < {}", start));
        }

        if let Some(end) = ending_before {
            query.push_str(&format!(" AND l2_block_number > {}", end));
        }

        query.push_str(" ORDER BY l2_block_number DESC");
        query.push_str(&format!(" LIMIT {}", limit));

        let rows = self.execute::<RawRow>(&query).await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let s = r.s_since_prev_block?;
                if s == 0 {
                    None
                } else {
                    Some(L2TpsRow {
                        l2_block_number: r.l2_block_number,
                        tps: r.sum_tx as f64 / s as f64,
                    })
                }
            })
            .collect())
    }

    /// Get aggregated L2 fees grouped by sequencer for the given range
    pub async fn get_l2_fees_by_sequencer(&self, range: TimeRange) -> Result<Vec<SequencerFeeRow>> {
        let client = self.base.clone();
        let query = format!(
            r#"
WITH valid_batches AS (
    SELECT
        b.batch_id,
        b.proposer_addr AS seq_addr,
        b.l1_block_number
    FROM ?.batches b
    INNER JOIN ?.l1_head_events l1 ON b.l1_block_number = l1.l1_block_number
    WHERE l1.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval})
),
valid_batch_blocks AS (
    SELECT DISTINCT
        vb.batch_id,
        vb.seq_addr,
        bb.l2_block_number
    FROM valid_batches vb
    LEFT JOIN ?.batch_blocks bb USING (batch_id)
),
revenues AS (
    SELECT
        vb.seq_addr AS seq_addr,
        sum(h.sum_priority_fee) AS priority_fee,
        sum(h.sum_base_fee) AS base_fee
    FROM valid_batches vb
    LEFT JOIN valid_batch_blocks vbb
           ON vbb.batch_id = vb.batch_id
          AND vbb.seq_addr = vb.seq_addr
    LEFT JOIN ?.l2_head_events h
           ON vbb.l2_block_number = h.l2_block_number
          AND {filter}
    GROUP BY vb.seq_addr
),
costs AS (
    SELECT
        vb.seq_addr AS seq_addr,
        sum(dc.cost) AS l1_data_cost,
        sum(pc.cost) AS prove_cost
    FROM valid_batches vb
    LEFT JOIN ?.l1_data_costs dc ON vb.batch_id = dc.batch_id AND vb.l1_block_number = dc.l1_block_number
    LEFT JOIN ?.prove_costs  pc ON vb.batch_id = pc.batch_id
    GROUP BY vb.seq_addr
)
SELECT
    CAST(
      coalesce(
        nullIf(r.seq_addr, unhex('0000000000000000000000000000000000000000')),
        nullIf(c.seq_addr, unhex('0000000000000000000000000000000000000000'))
      ) AS FixedString(20)
    ) AS sequencer,
    coalesce(r.priority_fee, toUInt128(0)) AS priority_fee,
    coalesce(r.base_fee,     toUInt128(0)) AS base_fee,
    coalesce(c.l1_data_cost, toUInt128(0)) AS l1_data_cost,
    coalesce(c.prove_cost,   toUInt128(0)) AS prove_cost
FROM revenues r
FULL OUTER JOIN costs c ON r.seq_addr = c.seq_addr
ORDER BY priority_fee DESC
"#,
            interval = range.interval(),
            filter = self.reorg_filter("h"),
        );

        let start = Instant::now();
        let result = client
            .query(&query)
            .bind(Identifier(&self.db_name))
            .bind(Identifier(&self.db_name))
            .bind(Identifier(&self.db_name))
            .bind(Identifier(&self.db_name))
            .bind(Identifier(&self.db_name))
            .bind(Identifier(&self.db_name))
            .fetch_all::<SequencerFeeRow>()
            .await;

        let duration_ms = start.elapsed().as_millis();
        match &result {
            Ok(rows) => {
                debug!(query = %query, duration_ms, rows = rows.len(), "ClickHouse query executed")
            }
            Err(e) => error!(query = %query, duration_ms, error = %e, "ClickHouse query failed"),
        }

        result.map_err(Into::into)
    }

    /// Get the blob count for each batch within the given range
    pub async fn get_blobs_per_batch(&self, range: TimeRange) -> Result<Vec<BatchBlobCountRow>> {
        let query = format!(
            "SELECT b.l1_block_number, b.batch_id, b.blob_count \
             FROM {db}.batches b \
             INNER JOIN {db}.l1_head_events l1_events \
               ON b.l1_block_number = l1_events.l1_block_number \
             WHERE l1_events.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval}) \
             ORDER BY b.l1_block_number ASC",
            interval = range.interval(),
            db = self.db_name,
        );

        let rows = self.execute::<BatchBlobCountRow>(&query).await?;
        Ok(rows)
    }

    /// Get the blob count per batch since the given cutoff time with cursor-based pagination.
    /// Results are returned in descending order by batch id.
    pub async fn get_blobs_per_batch_paginated(
        &self,
        since: DateTime<Utc>,
        limit: u64,
        starting_after: Option<u64>,
        ending_before: Option<u64>,
    ) -> Result<Vec<BatchBlobCountRow>> {
        let mut query = format!(
            "SELECT b.l1_block_number, b.batch_id, b.blob_count \
             FROM {db}.batches b \
             INNER JOIN {db}.l1_head_events l1_events \
               ON b.l1_block_number = l1_events.l1_block_number \
             WHERE l1_events.block_ts >= {since}",
            since = since.timestamp(),
            db = self.db_name,
        );
        if let Some(start) = starting_after {
            query.push_str(&format!(" AND b.batch_id < {}", start));
        }
        if let Some(end) = ending_before {
            query.push_str(&format!(" AND b.batch_id > {}", end));
        }
        query.push_str(" ORDER BY b.batch_id DESC");
        query.push_str(&format!(" LIMIT {}", limit));

        let rows = self.execute::<BatchBlobCountRow>(&query).await?;
        Ok(rows)
    }

    /// Get the distribution of blocks and TPS across different sequencers within a specified time
    /// window.
    pub async fn get_sequencer_distribution_range(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<SequencerDistributionRow>> {
        let query = format!(
            r#"
WITH recent_batches AS (
    SELECT
        b.batch_id,
        b.proposer_addr,
        b.l1_block_number
    FROM {db}.batches b
    INNER JOIN {db}.l1_head_events l1 ON b.l1_block_number = l1.l1_block_number
    WHERE l1.block_ts > {since}
      AND l1.block_ts <= {until}
),
recent_batch_blocks AS (
    SELECT DISTINCT bb.batch_id, bb.l2_block_number
    FROM {db}.batch_blocks bb
    INNER JOIN recent_batches rb USING (batch_id)
)
SELECT
  rb.proposer_addr AS sequencer,
  countDistinct(h.l2_block_number) AS blocks,
  countDistinct(rb.batch_id)       AS batches,
  toUInt64(min(h.block_ts))        AS min_ts,
  toUInt64(max(h.block_ts))        AS max_ts,
  sum(h.sum_tx)                    AS tx_sum
FROM recent_batches rb
INNER JOIN recent_batch_blocks rbb USING (batch_id)
INNER JOIN {db}.l2_head_events h ON rbb.l2_block_number = h.l2_block_number
WHERE {filter}
GROUP BY rb.proposer_addr
ORDER BY blocks DESC
"#,
            db = self.db_name,
            since = since.timestamp(),
            until = until.timestamp(),
            filter = self.reorg_filter("h"),
        );

        let rows = self
            .execute::<SequencerDistributionRow>(&query)
            .await
            .context("fetching sequencer distribution failed")?;
        Ok(rows)
    }

    /// Get aggregated block transactions with automatic bucketing based on time range
    pub async fn get_block_transactions(
        &self,
        sequencer: Option<AddressBytes>,
        range: TimeRange,
        bucket: Option<u64>,
    ) -> Result<Vec<BlockTransactionRow>> {
        #[derive(Row, Deserialize)]
        struct RawRow {
            sequencer: AddressBytes,
            l2_block_number: u64,
            block_time: u64,
            sum_tx: u32,
        }
        #[derive(Row, Deserialize)]
        struct AggRow {
            l2_block_number: u64,
            block_time: u64,
            sum_tx: u32,
        }

        let bucket = bucket.unwrap_or(1);
        if bucket <= 1 {
            let mut query = format!(
                "SELECT sequencer, h.l2_block_number, h.block_ts AS block_time, sum_tx \
             FROM {db}.l2_head_events h \
             WHERE h.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval}) \
               AND {filter}",
                interval = range.interval(),
                filter = self.reorg_filter("h"),
                db = self.db_name,
            );
            if let Some(addr) = sequencer {
                query.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
            }
            query.push_str(" ORDER BY l2_block_number ASC");

            let rows = self.execute::<RawRow>(&query).await?;
            return Ok(rows
                .into_iter()
                .map(|r| BlockTransactionRow {
                    sequencer: r.sequencer,
                    l2_block_number: r.l2_block_number,
                    block_time: Utc.timestamp_opt(r.block_time as i64, 0).unwrap(),
                    sum_tx: r.sum_tx,
                })
                .collect());
        }

        let mut inner = format!(
            "SELECT sequencer, h.l2_block_number, h.block_ts AS block_time, sum_tx \
         FROM {db}.l2_head_events h \
         WHERE h.block_ts >= toUnixTimestamp(now64() - INTERVAL {interval}) \
           AND {filter}",
            interval = range.interval(),
            filter = self.reorg_filter("h"),
            db = self.db_name,
        );
        if let Some(addr) = sequencer {
            inner.push_str(&format!(" AND sequencer = unhex('{}')", encode(addr)));
        }

        // Use bucketed aggregation and cap outliers at p99 per bucket.
        let query = format!(
            "WITH base AS ( \
                SELECT intDiv(l2_block_number, {bucket}) * {bucket} AS bucket_num, \
                       block_time, \
                       sum_tx \
                FROM ({inner}) AS base \
             ), \
             stats AS ( \
                SELECT bucket_num, \
                       quantileExact(0.99)(sum_tx) AS q99 \
                FROM base \
                GROUP BY bucket_num \
             ) \
             SELECT l2_block_number, \
                    block_time, \
                    toUInt32(if(isNaN(capped_avg), raw_avg, capped_avg)) AS sum_tx \
             FROM ( \
                SELECT b.bucket_num AS l2_block_number, \
                       max(b.block_time) AS block_time, \
                       avgIf(b.sum_tx, b.sum_tx < s.q99) AS capped_avg, \
                       avg(b.sum_tx) AS raw_avg \
                FROM base b \
                INNER JOIN stats s USING (bucket_num) \
                GROUP BY b.bucket_num \
             ) AS agg \
             ORDER BY l2_block_number ASC",
            bucket = bucket,
            inner = inner,
        );

        let rows = self.execute::<AggRow>(&query).await?;
        Ok(rows
            .into_iter()
            .map(|r| BlockTransactionRow {
                sequencer: AddressBytes::default(), // Use default/empty sequencer for bucketed data
                l2_block_number: r.l2_block_number,
                block_time: Utc.timestamp_opt(r.block_time as i64, 0).unwrap(),
                sum_tx: r.sum_tx,
            })
            .collect())
    }

    /// Get the most recent block hashes for the specified block numbers
    /// This is used to identify orphaned blocks during reorgs
    pub async fn get_latest_hashes_for_blocks(
        &self,
        block_numbers: &[u64],
    ) -> Result<Vec<(HashBytes, u64)>> {
        if block_numbers.is_empty() {
            return Ok(vec![]);
        }

        #[derive(Row, Deserialize)]
        struct HashRow {
            block_hash: HashBytes,
            l2_block_number: u64,
        }

        let block_list = block_numbers.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(",");
        let query = format!(
            "SELECT block_hash, l2_block_number \
             FROM (\
                 SELECT block_hash, l2_block_number, \
                        ROW_NUMBER() OVER (PARTITION BY l2_block_number ORDER BY inserted_at DESC) as rn \
                 FROM {db}.l2_head_events \
                 WHERE l2_block_number IN ({block_list})\
             ) ranked \
             WHERE rn = 1 \
             ORDER BY l2_block_number",
            db = self.db_name,
        );

        let rows = self.execute::<HashRow>(&query).await?;
        Ok(rows.into_iter().map(|r| (r.block_hash, r.l2_block_number)).collect())
    }

    /// Get combined L2 fees and batch components data for a unified endpoint
    pub async fn get_l2_fees_and_components(
        &self,
        proposer: Option<AddressBytes>,
        range: TimeRange,
    ) -> Result<(Vec<SequencerFeeRow>, Vec<BatchFeeComponentRow>)> {
        // Fetch both concurrently
        let (sequencer_fees, batch_components) = try_join!(
            self.get_l2_fees_by_sequencer(range),
            self.get_batch_fee_components(proposer, range)
        )?;
        Ok((sequencer_fees, batch_components))
    }
}
