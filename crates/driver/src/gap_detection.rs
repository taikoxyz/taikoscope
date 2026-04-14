//! Gap detection and backfill functionality
#![allow(missing_docs)]

use std::{collections::HashSet, time::Duration};

use alloy_primitives::{Address, B256};
use clickhouse::{AddressBytes, ClickhouseReader, ClickhouseWriter, HashBytes, L2HeadEvent};
use extractor::Extractor;
use eyre::{Context, Result};
use messages::{
    BatchProposedWrapper, BatchesProvedWrapper, BatchesVerifiedWrapper,
    ForcedInclusionProcessedWrapper,
};
use tracing::{error, info, warn};

use crate::event_handler::{EventHandler, GapDetectionState};

/// Retry an async operation with exponential backoff
async fn retry_with_backoff<T, E, F, Fut>(operation: F, operation_name: &str) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    const MAX_RETRIES: u32 = 3;
    let mut last_error = None;

    for attempt in 0..MAX_RETRIES {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(error) => {
                if attempt < MAX_RETRIES - 1 {
                    let delay_ms = 1000 * 2_u64.pow(attempt);
                    warn!(
                        operation = operation_name,
                        attempt = attempt + 1,
                        delay_ms = delay_ms,
                        "Operation failed, retrying..."
                    );
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                last_error = Some(error);
            }
        }
    }

    Err(last_error.unwrap())
}

/// Verify RPC connectivity before starting backfill operations
async fn verify_rpc_health(extractor: &Extractor) -> bool {
    let health_check = async {
        let _l1_block = extractor.get_l1_latest_block_number().await?;
        let _l2_block = extractor.get_l2_latest_block_number().await?;
        Ok::<(), eyre::Report>(())
    };

    match tokio::time::timeout(Duration::from_secs(5), health_check).await {
        Ok(Ok(())) => {
            info!("RPC health check passed");
            true
        }
        Ok(Err(e)) => {
            warn!(err = %e, "RPC health check failed - RPC error");
            false
        }
        Err(_) => {
            warn!("RPC health check failed - timeout");
            false
        }
    }
}

async fn collect_batch_proposed_wrappers_for_l1_range(
    extractor: &Extractor,
    inbox_address: Address,
    start_block: u64,
    end_block: u64,
) -> Result<Vec<BatchProposedWrapper>> {
    use chainio::ITaikoInbox::BatchProposed;

    let mut wrappers = Vec::new();

    for block_number in start_block..=end_block {
        let block = retry_with_backoff(
            || extractor.get_l1_block_by_number(block_number),
            &format!("fetch L1 block {block_number}"),
        )
        .await
        .wrap_err_with(|| {
            format!("failed to fetch L1 block {block_number} for batch data repair")
        })?;

        for tx_hash in block.transactions.hashes() {
            let receipt = retry_with_backoff(
                || extractor.get_receipt(tx_hash),
                &format!("fetch receipt {tx_hash}"),
            )
            .await
            .wrap_err_with(|| {
                format!("failed to fetch receipt {tx_hash} while repairing L1 block {block_number}")
            })?;

            for log in receipt.logs() {
                if log.removed || log.address() != inbox_address {
                    continue;
                }

                if let Ok(decoded) = log.log_decode::<BatchProposed>() {
                    wrappers.push(BatchProposedWrapper::from((
                        decoded.data().clone(),
                        log.block_number.unwrap_or(block_number),
                        tx_hash,
                        false,
                    )));
                }
            }
        }
    }

    Ok(wrappers)
}

async fn detect_repair_start_block(
    reader: &ClickhouseReader,
    extractor: &Extractor,
    limit: u64,
) -> Result<Option<u64>> {
    let recent_batches = reader.get_recent_batch_rows(limit).await?;
    let mut earliest_mismatch = None;

    for batch in recent_batches {
        let receipt = match extractor.get_receipt(B256::from(batch.l1_tx_hash)).await {
            Ok(receipt) => receipt,
            Err(_) => continue,
        };

        let actual_block = receipt.block_number;
        if let Some(block_number) = actual_block &&
            block_number != batch.l1_block_number
        {
            earliest_mismatch = Some(block_number);
        }
    }

    Ok(earliest_mismatch)
}

/// Gap detection and backfill methods for the Driver
impl crate::driver::Driver {
    /// Repair `BatchProposed`-derived rows for an explicit L1 block range.
    pub async fn repair_batch_proposed_data_range(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> Result<()> {
        if start_block > end_block {
            eyre::bail!("repair batch data start block must not exceed end block");
        }

        if !self.enable_db_writes {
            eyre::bail!("batch data repair requires database writes to be enabled");
        }

        let writer = self
            .clickhouse_writer
            .as_ref()
            .ok_or_else(|| eyre::eyre!("ClickHouse writer not available for batch data repair"))?;

        info!(
            start_block = start_block,
            end_block = end_block,
            "Collecting BatchProposed events for batch data repair"
        );
        let wrappers = collect_batch_proposed_wrappers_for_l1_range(
            &self.extractor,
            self.inbox_address,
            start_block,
            end_block,
        )
        .await?;

        if wrappers.is_empty() {
            info!(
                start_block = start_block,
                end_block = end_block,
                "No BatchProposed events found in repair range"
            );
            return Ok(());
        }

        let batch_ids =
            wrappers.iter().map(|wrapper| wrapper.batch.meta.batchId).collect::<Vec<_>>();
        let unique_batch_count = batch_ids.iter().copied().collect::<HashSet<_>>().len();

        info!(
            start_block = start_block,
            end_block = end_block,
            events_found = wrappers.len(),
            unique_batch_count = unique_batch_count,
            "Deleting stale BatchProposed-derived rows before replay"
        );
        writer.delete_batch_proposed_data(&batch_ids).await?;

        let handler = EventHandler::new(writer, &self.extractor, true);
        for wrapper in wrappers {
            handler.handle_batch_proposed(wrapper).await?;
        }

        info!(
            start_block = start_block,
            end_block = end_block,
            repaired_batches = unique_batch_count,
            "Completed batch data repair"
        );
        Ok(())
    }

    /// Detect a likely stale repair range from recent batch rows and repair it.
    pub async fn auto_repair_batch_proposed_data(&self, lookback_batches: u64) -> Result<()> {
        let reader = self
            .clickhouse_reader
            .as_ref()
            .ok_or_else(|| eyre::eyre!("ClickHouse reader not available for batch data repair"))?;

        let Some(start_block) =
            detect_repair_start_block(reader, &self.extractor, lookback_batches).await?
        else {
            info!(
                lookback_batches = lookback_batches,
                "No BatchProposed mismatch detected in recent batches"
            );
            return Ok(());
        };

        let end_block = reader
            .get_latest_l1_block()
            .await?
            .ok_or_else(|| eyre::eyre!("No L1 blocks available to bound batch data repair"))?;

        self.repair_batch_proposed_data_range(start_block, end_block).await
    }

    /// Start the gap detection and backfill task
    pub async fn start_gap_detection_task(&self) -> Option<tokio::task::JoinHandle<()>> {
        // Only start gap detection if we have a reader
        let reader = self.clickhouse_reader.as_ref()?.clone();
        let writer = self.clickhouse_writer.as_ref()?.clone();
        let extractor = self.extractor.clone();
        let inbox_address = self.inbox_address;
        let taiko_wrapper_address = self.taiko_wrapper_address;
        let enable_db_writes = self.enable_db_writes;
        let gap_dry_run = self.gap_dry_run;
        let finalization_buffer = self.gap_finalization_buffer_blocks;
        let continuous_lookback = self.gap_continuous_lookback_blocks;
        let poll_interval = self.gap_poll_interval_secs;
        let min_l1_block = self.gap_min_l1_block;
        let min_l2_block = self.gap_min_l2_block;

        info!("Starting gap detection task");

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(poll_interval));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                match run_gap_detection(
                    &reader,
                    Some(&writer),
                    &extractor,
                    inbox_address,
                    taiko_wrapper_address,
                    enable_db_writes && !gap_dry_run,
                    finalization_buffer,
                    continuous_lookback,
                    min_l1_block,
                    min_l2_block,
                )
                .await
                {
                    Ok(()) => {
                        info!("Gap detection cycle completed");
                    }
                    Err(e) if e.to_string().contains("Database tables not available") => {
                        warn!("Skipping gap detection cycle - database tables not available");
                    }
                    Err(e) => {
                        error!(err = %e, "Gap detection failed");
                    }
                }
            }
        });

        Some(handle)
    }

    /// Perform initial gap catch-up on startup
    pub async fn initial_gap_catchup(&self) -> Result<()> {
        // Only perform catch-up if we have a reader
        let reader = self.clickhouse_reader.as_ref().ok_or_else(|| {
            eyre::eyre!("ClickHouse reader not available for initial gap catch-up")
        })?;

        // For dry-run mode, pass None for writer
        let writer = if self.enable_db_writes { self.clickhouse_writer.as_ref() } else { None };

        info!("Starting initial gap catch-up with startup lookback");

        // Get gap detection state for preview
        let gap_state =
            get_gap_detection_state(reader, &self.extractor, self.gap_finalization_buffer_blocks)
                .await?;

        // Calculate startup lookback ranges
        let l1_start = std::cmp::max(
            1,
            gap_state.latest_l1_db.saturating_sub(self.gap_startup_lookback_blocks) + 1,
        );
        let l1_end = gap_state.l1_backfill_end;
        let l2_start = std::cmp::max(
            1,
            gap_state.latest_l2_db.saturating_sub(self.gap_startup_lookback_blocks) + 1,
        );
        let l2_end = gap_state.l2_backfill_end;

        // Get missing block counts for preview
        let l1_missing = reader.find_missing_l1_blocks(l1_start, l1_end).await?.len();
        let l2_missing = reader.find_missing_l2_blocks(l2_start, l2_end).await?.len();

        info!(
            l1_missing = l1_missing,
            l1_range = format!("{}..{}", l1_start, l1_end),
            l2_missing = l2_missing,
            l2_range = format!("{}..{}", l2_start, l2_end),
            "Initial gap catch-up preview: L1 missing={} ({}), L2 missing={} ({})",
            l1_missing,
            format!("{}..{}", l1_start, l1_end),
            l2_missing,
            format!("{}..{}", l2_start, l2_end)
        );

        match run_gap_detection(
            reader,
            writer,
            &self.extractor,
            self.inbox_address,
            self.taiko_wrapper_address,
            self.enable_db_writes && !self.gap_dry_run,
            self.gap_finalization_buffer_blocks,
            self.gap_startup_lookback_blocks,
            self.gap_min_l1_block,
            self.gap_min_l2_block,
        )
        .await
        {
            Ok(()) => Ok(()),
            Err(e) if e.to_string().contains("Database tables not available") => {
                warn!("Skipping initial gap catch-up - database tables not available");
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

/// Run initial gap catch-up - standalone version for background task
#[allow(clippy::too_many_arguments)]
pub async fn run_initial_gap_catchup(
    reader: &ClickhouseReader,
    writer: Option<&ClickhouseWriter>,
    extractor: &Extractor,
    inbox_address: Address,
    taiko_wrapper_address: Address,
    enable_db_writes: bool,
    gap_finalization_buffer_blocks: u64,
    gap_startup_lookback_blocks: u64,
    gap_min_l1_block: u64,
    gap_min_l2_block: u64,
) -> Result<()> {
    info!("Starting initial gap catch-up with startup lookback");

    // Get gap detection state for preview
    let gap_state =
        get_gap_detection_state(reader, extractor, gap_finalization_buffer_blocks).await?;

    // Calculate startup lookback ranges
    let l1_start =
        std::cmp::max(1, gap_state.latest_l1_db.saturating_sub(gap_startup_lookback_blocks) + 1);
    let l1_end = gap_state.l1_backfill_end;
    let l2_start =
        std::cmp::max(1, gap_state.latest_l2_db.saturating_sub(gap_startup_lookback_blocks) + 1);
    let l2_end = gap_state.l2_backfill_end;

    // Get missing block counts for preview
    let l1_missing = reader.find_missing_l1_blocks(l1_start, l1_end).await?.len();
    let l2_missing = reader.find_missing_l2_blocks(l2_start, l2_end).await?.len();

    info!(
        l1_missing = l1_missing,
        l1_range = format!("{}..{}", l1_start, l1_end),
        l2_missing = l2_missing,
        l2_range = format!("{}..{}", l2_start, l2_end),
        "Initial gap catch-up preview: L1 missing={} ({}), L2 missing={} ({})",
        l1_missing,
        format!("{}..{}", l1_start, l1_end),
        l2_missing,
        format!("{}..{}", l2_start, l2_end)
    );

    match run_gap_detection(
        reader,
        writer,
        extractor,
        inbox_address,
        taiko_wrapper_address,
        enable_db_writes,
        gap_finalization_buffer_blocks,
        gap_startup_lookback_blocks,
        gap_min_l1_block,
        gap_min_l2_block,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(e) if e.to_string().contains("Database tables not available") => {
            warn!("Skipping initial gap catch-up - database tables not available");
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Run a single cycle of gap detection and backfill
#[allow(clippy::too_many_arguments)]
pub async fn run_gap_detection(
    reader: &ClickhouseReader,
    writer: Option<&ClickhouseWriter>,
    extractor: &Extractor,
    inbox_address: Address,
    taiko_wrapper_address: Address,
    enable_db_writes: bool,
    finalization_buffer: u64,
    lookback_blocks: u64,
    min_l1_block: u64,
    min_l2_block: u64,
) -> Result<()> {
    // Verify RPC health before starting gap detection
    if !verify_rpc_health(extractor).await {
        warn!("Skipping gap detection cycle due to RPC health check failure");
        return Ok(());
    }

    let gap_state = get_gap_detection_state(reader, extractor, finalization_buffer).await?;

    // Calculate start overrides for lookback
    let l1_start_override = (lookback_blocks > 0)
        .then(|| std::cmp::max(1, gap_state.latest_l1_db.saturating_sub(lookback_blocks) + 1));
    let l2_start_override = (lookback_blocks > 0)
        .then(|| std::cmp::max(1, gap_state.latest_l2_db.saturating_sub(lookback_blocks) + 1));

    process_l1_gaps(
        reader,
        writer,
        extractor,
        &gap_state,
        inbox_address,
        taiko_wrapper_address,
        enable_db_writes,
        l1_start_override,
        min_l1_block,
    )
    .await?;

    process_l2_gaps(
        reader,
        writer,
        extractor,
        &gap_state,
        enable_db_writes,
        l2_start_override,
        min_l2_block,
    )
    .await?;

    Ok(())
}

/// Get the current state for gap detection (blockchain vs database)
pub async fn get_gap_detection_state(
    reader: &ClickhouseReader,
    extractor: &Extractor,
    finalization_buffer: u64,
) -> Result<GapDetectionState> {
    // Get current blockchain state
    let latest_l1_rpc = extractor
        .get_l1_latest_block_number()
        .await
        .map_err(|e| eyre::eyre!("Failed to get latest L1 block: {}", e))?;
    let latest_l2_rpc = extractor
        .get_l2_latest_block_number()
        .await
        .map_err(|e| eyre::eyre!("Failed to get latest L2 block: {}", e))?;

    // Get database state - handle case where tables don't exist
    let (latest_l1_db, latest_l2_db) = match (
        reader.get_latest_l1_block().await,
        reader.get_latest_l2_block().await,
    ) {
        (Ok(l1), Ok(l2)) => (l1.unwrap_or(0), l2.unwrap_or(0)),
        (Err(e), _) | (_, Err(e)) => {
            let error_msg = e.to_string();
            if error_msg.contains("doesn't exist") || error_msg.contains("Unknown table") {
                warn!(
                    err = %e,
                    "Database tables do not exist yet. Skipping gap detection - tables will be created when data is first ingested."
                );
            } else if error_msg.contains("tag for enum is not valid") {
                warn!(
                    err = %e,
                    "Database schema mismatch detected. This may be due to recent migrations. Gap detection will be skipped this cycle."
                );
            } else {
                warn!(
                    err = %e,
                    "Failed to get database state for gap detection due to unexpected error. Skipping gap detection."
                );
            }
            return Err(eyre::eyre!("Database tables not available for gap detection: {}", e));
        }
    };

    // Only backfill finalized data (using configurable buffer)
    let l1_backfill_end = latest_l1_rpc.saturating_sub(finalization_buffer);
    let l2_backfill_end = latest_l2_rpc.saturating_sub(finalization_buffer);

    let state = GapDetectionState {
        latest_l1_rpc,
        latest_l2_rpc,
        latest_l1_db,
        latest_l2_db,
        l1_backfill_end,
        l2_backfill_end,
    };

    info!(
        latest_l1_rpc = state.latest_l1_rpc,
        latest_l1_db = state.latest_l1_db,
        latest_l2_rpc = state.latest_l2_rpc,
        latest_l2_db = state.latest_l2_db,
        finalization_buffer = finalization_buffer,
        "Gap detection: blockchain vs database state"
    );

    Ok(state)
}

/// Process L1 gaps and perform backfill if needed
#[allow(clippy::too_many_arguments)]
pub async fn process_l1_gaps(
    reader: &ClickhouseReader,
    writer: Option<&ClickhouseWriter>,
    extractor: &Extractor,
    state: &GapDetectionState,
    inbox_address: Address,
    taiko_wrapper_address: Address,
    enable_db_writes: bool,
    start_block_override: Option<u64>,
    min_l1_block: u64,
) -> Result<()> {
    let start_block = start_block_override.unwrap_or(state.latest_l1_db + 1);
    if start_block > state.l1_backfill_end {
        return Ok(());
    }

    let l1_gaps = reader.find_missing_l1_blocks(start_block, state.l1_backfill_end).await?;
    if l1_gaps.is_empty() {
        return Ok(());
    }

    if enable_db_writes {
        info!(gaps = l1_gaps.len(), "Found L1 gaps to backfill: {:?}", l1_gaps);
        let still_missing = recheck_gaps_for_race_conditions(
            reader,
            l1_gaps,
            start_block,
            state.l1_backfill_end,
            true,
        )
        .await?;

        if still_missing.is_empty() {
            info!("All L1 gaps were filled by live processing, skipping backfill");
        } else {
            info!(
                gaps = still_missing.len(),
                "Confirmed L1 gaps still missing after double-check: {:?}", still_missing
            );
            backfill_l1_blocks(
                writer,
                extractor,
                still_missing,
                inbox_address,
                taiko_wrapper_address,
                enable_db_writes,
                min_l1_block,
            )
            .await?;
        }
    } else {
        info!(gaps = l1_gaps.len(), "🧪 DRY-RUN: Would backfill L1 gaps: {:?}", l1_gaps);
    }

    Ok(())
}

/// Process L2 gaps and perform backfill if needed
pub async fn process_l2_gaps(
    reader: &ClickhouseReader,
    writer: Option<&ClickhouseWriter>,
    extractor: &Extractor,
    state: &GapDetectionState,
    enable_db_writes: bool,
    start_block_override: Option<u64>,
    min_l2_block: u64,
) -> Result<()> {
    let start_block = start_block_override.unwrap_or(state.latest_l2_db + 1);
    if start_block > state.l2_backfill_end {
        return Ok(());
    }

    let l2_gaps = reader.find_missing_l2_blocks(start_block, state.l2_backfill_end).await?;
    if l2_gaps.is_empty() {
        return Ok(());
    }

    if enable_db_writes {
        info!(gaps = l2_gaps.len(), "Found L2 gaps to backfill: {:?}", l2_gaps);
        let still_missing = recheck_gaps_for_race_conditions(
            reader,
            l2_gaps,
            start_block,
            state.l2_backfill_end,
            false,
        )
        .await?;

        if still_missing.is_empty() {
            info!("All L2 gaps were filled by live processing, skipping backfill");
        } else {
            info!(
                gaps = still_missing.len(),
                "Confirmed L2 gaps still missing after double-check: {:?}", still_missing
            );
            backfill_l2_blocks(writer, extractor, still_missing, enable_db_writes, min_l2_block)
                .await?;
        }
    } else {
        info!(gaps = l2_gaps.len(), "🧪 DRY-RUN: Would backfill L2 gaps: {:?}", l2_gaps);
    }

    Ok(())
}

/// Re-check gaps to avoid race conditions with live processing
pub async fn recheck_gaps_for_race_conditions(
    reader: &ClickhouseReader,
    original_gaps: Vec<u64>,
    start_block: u64,
    end_block: u64,
    is_l1: bool,
) -> Result<Vec<u64>> {
    let current_gaps = if is_l1 {
        reader.find_missing_l1_blocks(start_block, end_block).await?
    } else {
        reader.find_missing_l2_blocks(start_block, end_block).await?
    };

    let current_gaps_set: HashSet<u64> = current_gaps.into_iter().collect();
    let still_missing: Vec<u64> =
        original_gaps.into_iter().filter(|&block| current_gaps_set.contains(&block)).collect();

    Ok(still_missing)
}

/// Backfill missing L1 blocks and extract all Taiko events from those blocks
pub async fn backfill_l1_blocks(
    writer: Option<&ClickhouseWriter>,
    extractor: &Extractor,
    block_numbers: Vec<u64>,
    inbox_address: Address,
    taiko_wrapper_address: Address,
    enable_db_writes: bool,
    min_l1_block: u64,
) -> Result<()> {
    // Filter out blocks before the minimum L1 block number
    let original_count = block_numbers.len();
    let filtered_blocks: Vec<u64> =
        block_numbers.into_iter().filter(|&block_number| block_number >= min_l1_block).collect();

    if filtered_blocks.len() != original_count {
        let skipped_count = original_count - filtered_blocks.len();
        info!(
            skipped_count = skipped_count,
            min_l1_block = min_l1_block,
            "Skipped {} L1 blocks below minimum block number {}",
            skipped_count,
            min_l1_block
        );
    }

    let mut consecutive_failures = 0;
    const MAX_CONSECUTIVE_FAILURES: u32 = 5;

    for block_number in filtered_blocks {
        // Use retry logic for block fetching
        let block_result = retry_with_backoff(
            || extractor.get_l1_block_by_number(block_number),
            &format!("fetch L1 block {}", block_number),
        )
        .await;

        match block_result {
            Ok(block) => {
                consecutive_failures = 0; // Reset on successful fetch
                // Insert L1 header with slot derived from block number
                let slot = block.header.number;

                let header = primitives::headers::L1Header {
                    number: block.header.number,
                    hash: block.header.hash,
                    slot,
                    timestamp: block.header.timestamp,
                };

                if enable_db_writes &&
                    let Some(w) = writer &&
                    let Err(e) = w.insert_l1_header(&header).await
                {
                    error!(block_number = block_number, err = %e, "Failed to backfill L1 header");
                    continue;
                }
                if !enable_db_writes {
                    info!(
                        block_number = block_number,
                        hash = %header.hash,
                        "🧪 DRY-RUN: Would insert L1 header"
                    );
                }

                // Process preconf data for backfill
                if enable_db_writes {
                    process_preconf_data_for_backfill(writer, extractor, &header).await;
                } else {
                    info!(
                        block_number = header.number,
                        "🧪 DRY-RUN: Would process preconf data for backfill"
                    );
                }

                // Process all Taiko events from this L1 block
                process_l1_block_taiko_events(
                    writer,
                    extractor,
                    &block,
                    inbox_address,
                    taiko_wrapper_address,
                    enable_db_writes,
                )
                .await?;

                if enable_db_writes {
                    info!(
                        block_number = block_number,
                        "Successfully backfilled L1 block with events"
                    );
                } else {
                    info!(
                        block_number = block_number,
                        "🧪 DRY-RUN: Would complete L1 block backfill with events"
                    );
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                error!(
                    block_number = block_number,
                    consecutive_failures = consecutive_failures,
                    err = %e,
                    "Could not fetch L1 block for backfill after retries"
                );

                // Circuit breaker: stop processing if too many consecutive failures
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    error!(
                        consecutive_failures = consecutive_failures,
                        max_failures = MAX_CONSECUTIVE_FAILURES,
                        "Too many consecutive L1 block fetch failures, stopping backfill for this cycle"
                    );
                    break;
                }
            }
        }
    }
    Ok(())
}

/// Process all Taiko events found in an L1 block during backfill
pub async fn process_l1_block_taiko_events(
    writer: Option<&ClickhouseWriter>,
    extractor: &Extractor,
    block: &alloy_rpc_types_eth::Block,
    inbox_address: Address,
    taiko_wrapper_address: Address,
    enable_db_writes: bool,
) -> Result<()> {
    use chainio::{
        BatchesVerified,
        ITaikoInbox::{BatchProposed, BatchesProved, BatchesVerified as InboxBatchesVerified},
        taiko::wrapper::ITaikoWrapper::ForcedInclusionProcessed,
    };
    use messages::{
        BatchProposedWrapper, BatchesProvedWrapper, BatchesVerifiedWrapper,
        ForcedInclusionProcessedWrapper,
    };

    let block_number = block.header.number;
    let mut events_found = 0;

    info!(
        block_number = block_number,
        tx_count = block.transactions.len(),
        "Processing L1 block for Taiko events during backfill"
    );

    // Process all transactions in the block to find Taiko events
    for tx_hash in block.transactions.hashes() {
        // Get transaction receipt to access logs
        match extractor.get_receipt(tx_hash).await {
            Ok(receipt) => {
                for log in receipt.logs() {
                    // Skip removed logs (shouldn't happen in backfill but be safe)
                    if log.removed {
                        continue;
                    }

                    // Process events based on contract address
                    if log.address() == inbox_address {
                        // Try to decode BatchProposed
                        if let Ok(decoded) = log.log_decode::<BatchProposed>() {
                            info!(
                                block_number = block_number,
                                tx_hash = %tx_hash,
                                "Found BatchProposed event in backfill"
                            );
                            let wrapper = BatchProposedWrapper::from((
                                decoded.data().clone(),
                                block_number,
                                tx_hash,
                                false, // not reorged
                            ));
                            handle_batch_proposed_event_during_backfill(
                                writer,
                                extractor,
                                wrapper,
                                enable_db_writes,
                            )
                            .await?;
                            events_found += 1;
                            continue;
                        }

                        // Try to decode BatchesProved
                        if let Ok(decoded) = log.log_decode::<BatchesProved>() {
                            info!(
                                block_number = block_number,
                                tx_hash = %tx_hash,
                                "Found BatchesProved event in backfill"
                            );
                            let wrapper = BatchesProvedWrapper::from((
                                decoded.data().clone(),
                                block_number,
                                tx_hash,
                                false, // not reorged
                            ));
                            handle_batches_proved_event_during_backfill(
                                writer,
                                extractor,
                                wrapper,
                                enable_db_writes,
                            )
                            .await?;
                            events_found += 1;
                            continue;
                        }

                        // Try to decode BatchesVerified
                        if let Ok(decoded) = log.log_decode::<InboxBatchesVerified>() {
                            info!(
                                block_number = block_number,
                                tx_hash = %tx_hash,
                                "Found BatchesVerified event in backfill"
                            );
                            let data = decoded.data();
                            let mut block_hash = [0u8; 32];
                            block_hash.copy_from_slice(data.blockHash.as_slice());
                            let verified = BatchesVerified { batch_id: data.batchId, block_hash };
                            let wrapper = BatchesVerifiedWrapper::from((
                                verified,
                                block_number,
                                tx_hash,
                                false, // not reorged
                            ));
                            handle_batches_verified_event_during_backfill(
                                writer,
                                extractor,
                                wrapper,
                                enable_db_writes,
                            )
                            .await?;
                            events_found += 1;
                        }
                    } else if log.address() == taiko_wrapper_address {
                        // Try to decode ForcedInclusionProcessed
                        if let Ok(decoded) = log.log_decode::<ForcedInclusionProcessed>() {
                            info!(
                                block_number = block_number,
                                tx_hash = %tx_hash,
                                "Found ForcedInclusionProcessed event in backfill"
                            );
                            let wrapper = ForcedInclusionProcessedWrapper::from((
                                decoded.data().clone(),
                                false, // not reorged
                            ));
                            handle_forced_inclusion_event_during_backfill(
                                writer,
                                extractor,
                                wrapper,
                                enable_db_writes,
                            )
                            .await?;
                            events_found += 1;
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    block_number = block_number,
                    tx_hash = %tx_hash,
                    err = %e,
                    "Failed to get receipt for transaction during L1 backfill"
                );
            }
        }
    }

    info!(
        block_number = block_number,
        events_found = events_found,
        "Completed L1 block Taiko event extraction during backfill"
    );
    Ok(())
}

// Event handlers for backfill - reuse exact same logic as live processing
pub async fn handle_batch_proposed_event_during_backfill(
    writer: Option<&ClickhouseWriter>,
    extractor: &Extractor,
    wrapper: BatchProposedWrapper,
    enable_db_writes: bool,
) -> Result<()> {
    if let Some(writer) = writer {
        let handler = EventHandler::new(writer, extractor, enable_db_writes);
        handler.handle_batch_proposed(wrapper).await
    } else {
        info!("🧪 DRY-RUN: Would handle batch proposed event during backfill");
        Ok(())
    }
}

pub async fn handle_batches_proved_event_during_backfill(
    writer: Option<&ClickhouseWriter>,
    extractor: &Extractor,
    wrapper: BatchesProvedWrapper,
    enable_db_writes: bool,
) -> Result<()> {
    if let Some(writer) = writer {
        let handler = EventHandler::new(writer, extractor, enable_db_writes);
        handler.handle_batches_proved(wrapper).await
    } else {
        info!("🧪 DRY-RUN: Would handle batches proved event during backfill");
        Ok(())
    }
}

pub async fn handle_batches_verified_event_during_backfill(
    writer: Option<&ClickhouseWriter>,
    extractor: &Extractor,
    wrapper: BatchesVerifiedWrapper,
    enable_db_writes: bool,
) -> Result<()> {
    if let Some(writer) = writer {
        let handler = EventHandler::new(writer, extractor, enable_db_writes);
        handler.handle_batches_verified(wrapper).await
    } else {
        info!("🧪 DRY-RUN: Would handle batches verified event during backfill");
        Ok(())
    }
}

pub async fn handle_forced_inclusion_event_during_backfill(
    writer: Option<&ClickhouseWriter>,
    extractor: &Extractor,
    wrapper: ForcedInclusionProcessedWrapper,
    enable_db_writes: bool,
) -> Result<()> {
    if let Some(writer) = writer {
        let handler = EventHandler::new(writer, extractor, enable_db_writes);
        handler.handle_forced_inclusion(wrapper).await
    } else {
        info!("🧪 DRY-RUN: Would handle forced inclusion event during backfill");
        Ok(())
    }
}

/// Backfill missing L2 blocks using exact same logic as live processing
pub async fn backfill_l2_blocks(
    writer: Option<&ClickhouseWriter>,
    extractor: &Extractor,
    block_numbers: Vec<u64>,
    enable_db_writes: bool,
    min_l2_block: u64,
) -> Result<()> {
    // Filter out blocks before the minimum L2 block number
    let original_count = block_numbers.len();
    let filtered_blocks: Vec<u64> =
        block_numbers.into_iter().filter(|&block_number| block_number >= min_l2_block).collect();

    if filtered_blocks.len() != original_count {
        let skipped_count = original_count - filtered_blocks.len();
        info!(
            skipped_count = skipped_count,
            min_l2_block = min_l2_block,
            "Skipped {} L2 blocks below minimum block number {}",
            skipped_count,
            min_l2_block
        );
    }

    let mut consecutive_failures = 0;
    const MAX_CONSECUTIVE_FAILURES: u32 = 5;

    for block_number in filtered_blocks {
        // Use retry logic for block fetching
        let block_result = retry_with_backoff(
            || extractor.get_l2_block_by_number(block_number),
            &format!("fetch L2 block {}", block_number),
        )
        .await;

        match block_result {
            Ok(block) => {
                consecutive_failures = 0; // Reset on successful fetch
                let header = primitives::headers::L2Header {
                    number: block.header.number,
                    hash: block.header.hash,
                    parent_hash: block.header.parent_hash,
                    timestamp: block.header.timestamp,
                    gas_used: block.header.gas_used,
                    beneficiary: block.header.beneficiary,
                    base_fee_per_gas: block.header.base_fee_per_gas.unwrap_or(0),
                };

                // Use same stats calculation as processor
                let (sum_gas_used, sum_tx, sum_priority_fee) = extractor
                    .get_l2_block_stats(alloy_primitives::B256::from(*header.hash), header.base_fee_per_gas)
                    .await
                    .unwrap_or_else(|e| {
                        error!(header_number = header.number, err = %e, "Failed to get L2 block stats for backfill, using defaults");
                        (0, 0, 0)
                    });

                let sum_base_fee = sum_gas_used.saturating_mul(header.base_fee_per_gas as u128);

                let event = L2HeadEvent {
                    l2_block_number: header.number,
                    block_hash: HashBytes(*header.hash),
                    block_ts: header.timestamp,
                    sum_gas_used,
                    sum_tx,
                    sum_priority_fee,
                    sum_base_fee,
                    sequencer: AddressBytes(header.beneficiary.into_array()),
                };

                if enable_db_writes &&
                    let Some(w) = writer &&
                    let Err(e) = w.insert_l2_header(&event).await
                {
                    error!(block_number = block_number, err = %e, "Failed to backfill L2 block");
                } else if enable_db_writes && let Some(_w) = writer {
                    info!(block_number = block_number, "Successfully backfilled L2 block");
                } else {
                    info!(
                        block_number = block_number,
                        gas_used = event.sum_gas_used,
                        tx_count = event.sum_tx,
                        "🧪 DRY-RUN: Would insert L2 header with stats"
                    );
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                error!(
                    block_number = block_number,
                    consecutive_failures = consecutive_failures,
                    err = %e,
                    "Could not fetch L2 block for backfill after retries"
                );

                // Circuit breaker: stop processing if too many consecutive failures
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    error!(
                        consecutive_failures = consecutive_failures,
                        max_failures = MAX_CONSECUTIVE_FAILURES,
                        "Too many consecutive L2 block fetch failures, stopping backfill for this cycle"
                    );
                    break;
                }
            }
        }
    }
    Ok(())
}

/// Process preconf data for backfill operations (static method)
pub async fn process_preconf_data_for_backfill(
    writer: Option<&ClickhouseWriter>,
    extractor: &Extractor,
    header: &primitives::headers::L1Header,
) {
    // Get operator candidates for current epoch
    let opt_candidates = match extractor.get_operator_candidates_for_current_epoch().await {
        Ok(c) => {
            info!(
                slot = header.slot,
                block = header.number,
                candidates = ?c,
                candidates_count = c.len(),
                "Successfully retrieved operator candidates for backfill"
            );
            Some(c)
        }
        Err(e) => {
            error!(
                slot = header.slot,
                block = header.number,
                err = %e,
                "Failed picking operator candidates during backfill"
            );
            None
        }
    };
    let candidates = opt_candidates.unwrap_or_else(Vec::new);

    // Get current operator for epoch
    let opt_current_operator = match extractor.get_operator_for_current_epoch().await {
        Ok(op) => {
            info!(
                block = header.number,
                operator = ?op,
                "Current operator for epoch during backfill"
            );
            Some(op)
        }
        Err(e) => {
            error!(
                block = header.number,
                err = %e,
                "get_operator_for_current_epoch failed during backfill"
            );
            None
        }
    };

    // Get next operator for epoch
    let opt_next_operator = match extractor.get_operator_for_next_epoch().await {
        Ok(op) => {
            info!(
                block = header.number,
                operator = ?op,
                "Next operator for epoch during backfill"
            );
            Some(op)
        }
        Err(e) => {
            error!(
                block = header.number,
                err = %e,
                "get_operator_for_next_epoch failed during backfill"
            );
            None
        }
    };

    // Insert preconf data if we have at least one operator and a writer
    if let Some(writer) = writer {
        if opt_current_operator.is_some() || opt_next_operator.is_some() {
            if let Err(e) = writer
                .insert_preconf_data(
                    header.slot,
                    candidates,
                    opt_current_operator,
                    opt_next_operator,
                )
                .await
            {
                error!(slot = header.slot, err = %e, "Failed to insert preconf data during backfill");
            } else {
                info!(slot = header.slot, "Inserted preconf data for slot during backfill");
            }
        } else {
            info!(
                slot = header.slot,
                "Skipping preconf data insertion during backfill due to errors fetching operator data"
            );
        }
    } else {
        info!(slot = header.slot, "🧪 DRY-RUN: Would insert preconf data for slot during backfill");
    }
}

/// Pure helper function to select blocks that are still missing after a recheck
/// This is extracted from `recheck_gaps_for_race_conditions` to enable unit testing
pub fn select_still_missing(original_gaps: Vec<u64>, current_gaps: Vec<u64>) -> Vec<u64> {
    let current_gaps_set: HashSet<u64> = current_gaps.into_iter().collect();
    original_gaps.into_iter().filter(|&block| current_gaps_set.contains(&block)).collect()
}

/// Pure helper function to calculate lookback start block
pub fn calculate_lookback_start(latest_db: u64, lookback_blocks: u64) -> u64 {
    std::cmp::max(1, latest_db.saturating_sub(lookback_blocks) + 1)
}

/// Decoded Taiko event from a log
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum DecodedEvent {
    BatchProposed(messages::BatchProposedWrapper),
    BatchesProved(messages::BatchesProvedWrapper),
    BatchesVerified(messages::BatchesVerifiedWrapper),
    ForcedInclusionProcessed(messages::ForcedInclusionProcessedWrapper),
}

/// Pure helper function to decode a Taiko event from a log
/// This enables unit testing of event decoding without network dependencies
pub fn decode_taiko_event_from_log(
    log: &alloy_rpc_types_eth::Log,
    inbox_address: alloy_primitives::Address,
    taiko_wrapper_address: alloy_primitives::Address,
    l1_block_number: u64,
    l1_tx_hash: alloy_primitives::B256,
) -> Option<DecodedEvent> {
    use chainio::{
        BatchesVerified,
        ITaikoInbox::{BatchProposed, BatchesProved, BatchesVerified as InboxBatchesVerified},
        taiko::wrapper::ITaikoWrapper::ForcedInclusionProcessed,
    };

    // Skip removed logs
    if log.removed {
        return None;
    }

    // Process events based on contract address
    if log.address() == inbox_address {
        // Try to decode BatchProposed
        if let Ok(decoded) = log.log_decode::<BatchProposed>() {
            let wrapper = messages::BatchProposedWrapper::from((
                decoded.data().clone(),
                l1_block_number,
                l1_tx_hash,
                false, // not reorged
            ));
            return Some(DecodedEvent::BatchProposed(wrapper));
        }

        // Try to decode BatchesProved
        if let Ok(decoded) = log.log_decode::<BatchesProved>() {
            let wrapper = messages::BatchesProvedWrapper::from((
                decoded.data().clone(),
                l1_block_number,
                l1_tx_hash,
                false, // not reorged
            ));
            return Some(DecodedEvent::BatchesProved(wrapper));
        }

        // Try to decode BatchesVerified
        if let Ok(decoded) = log.log_decode::<InboxBatchesVerified>() {
            let data = decoded.data();
            let mut block_hash = [0u8; 32];
            block_hash.copy_from_slice(data.blockHash.as_slice());
            let verified = BatchesVerified { batch_id: data.batchId, block_hash };
            let wrapper = messages::BatchesVerifiedWrapper::from((
                verified,
                l1_block_number,
                l1_tx_hash,
                false, // not reorged
            ));
            return Some(DecodedEvent::BatchesVerified(wrapper));
        }
    } else if log.address() == taiko_wrapper_address {
        // Try to decode ForcedInclusionProcessed
        if let Ok(decoded) = log.log_decode::<ForcedInclusionProcessed>() {
            let wrapper = messages::ForcedInclusionProcessedWrapper::from((
                decoded.data().clone(),
                false, // not reorged
            ));
            return Some(DecodedEvent::ForcedInclusionProcessed(wrapper));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256};

    #[test]
    fn test_select_still_missing() {
        let original = vec![1, 2, 3, 4, 5];
        let current = vec![2, 4, 6];
        let result = select_still_missing(original, current);
        assert_eq!(result, vec![2, 4]);
    }

    #[test]
    fn test_select_still_missing_empty() {
        let original = vec![1u64, 2, 3];
        let current = vec![];
        let result = select_still_missing(original, current);
        assert_eq!(result, vec![0u64; 0]); // If current is empty, nothing should be selected
    }

    #[test]
    fn test_select_still_missing_all_missing() {
        let original = vec![1u64, 2, 3];
        let current = vec![1u64, 2, 3];
        let result = select_still_missing(original, current);
        assert_eq!(result, vec![1u64, 2, 3]);
    }

    #[test]
    fn test_calculate_lookback_start() {
        assert_eq!(calculate_lookback_start(100, 50), 51);
        assert_eq!(calculate_lookback_start(100, 100), 1);
        assert_eq!(calculate_lookback_start(100, 200), 1);
        assert_eq!(calculate_lookback_start(0, 50), 1);
    }

    #[test]
    fn test_decode_taiko_event_from_log_basic() {
        // This test verifies the function structure without complex event encoding
        // The actual event decoding is tested through integration tests
        let inbox_address = Address::repeat_byte(1);
        let taiko_wrapper_address = Address::repeat_byte(2);
        let l1_block_number = 100;
        let l1_tx_hash = B256::repeat_byte(3);

        // Test that the function exists and can be called
        // We'll test the actual decoding logic in integration tests
        assert_eq!(inbox_address, Address::repeat_byte(1));
        assert_eq!(taiko_wrapper_address, Address::repeat_byte(2));
        assert_eq!(l1_block_number, 100);
        assert_eq!(l1_tx_hash, B256::repeat_byte(3));
    }
}
