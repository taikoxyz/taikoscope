//! Event handler for processing Taiko events

use clickhouse::ClickhouseWriter;
use extractor::Extractor;
use eyre::Result;
use messages::{BatchProposedWrapper, BatchesProvedWrapper, ForcedInclusionProcessedWrapper};
use tracing::info;

/// State for gap detection operations
#[derive(Debug)]
pub struct GapDetectionState {
    /// Latest L1 block number from RPC
    pub latest_l1_rpc: u64,
    /// Latest L2 block number from RPC
    pub latest_l2_rpc: u64,
    /// Latest L1 block number stored in database
    pub latest_l1_db: u64,
    /// Latest L2 block number stored in database
    pub latest_l2_db: u64,
    /// End block number for L1 backfill process
    pub l1_backfill_end: u64,
    /// End block number for L2 backfill process
    pub l2_backfill_end: u64,
}

/// Common event handler for both live processing and backfill operations
#[derive(Debug)]
pub struct EventHandler<'a> {
    writer: &'a ClickhouseWriter,
    extractor: &'a Extractor,
    enable_db_writes: bool,
}

impl<'a> EventHandler<'a> {
    /// Creates a new event handler instance
    pub const fn new(
        writer: &'a ClickhouseWriter,
        extractor: &'a Extractor,
        enable_db_writes: bool,
    ) -> Self {
        Self { writer, extractor, enable_db_writes }
    }

    /// Handles a batch proposed event, inserting the batch and calculating L1 data costs
    pub async fn handle_batch_proposed(&self, wrapper: BatchProposedWrapper) -> Result<()> {
        let batch = &wrapper.batch;
        let l1_block_number = wrapper.l1_block_number;
        let l1_tx_hash = wrapper.l1_tx_hash;

        // Insert batch with error handling
        if self.enable_db_writes {
            crate::event_processing::with_db_error_context(
                self.writer.insert_batch(batch, l1_block_number, l1_tx_hash),
                "insert batch",
                format!("batch_last_block={:?}", batch.last_block_number()),
            )
            .await?;
        } else {
            info!(
                batch_id = batch.meta.batchId,
                last_block = batch.last_block_number(),
                l1_tx_hash = %l1_tx_hash,
                "🧪 DRY-RUN: Would insert batch"
            );
        }

        // Calculate and insert L1 data cost
        if let Some(cost) =
            crate::event_processing::fetch_transaction_cost(self.extractor, l1_tx_hash).await
        {
            if self.enable_db_writes {
                crate::event_processing::with_db_error_context(
                    self.writer.insert_l1_data_cost(l1_block_number, batch.meta.batchId, cost),
                    "insert L1 data cost",
                    format!("l1_block_number={}, tx_hash={:?}", l1_block_number, l1_tx_hash),
                )
                .await?;
            } else {
                info!(
                    l1_block_number = l1_block_number,
                    batch_id = batch.meta.batchId,
                    cost = cost,
                    "🧪 DRY-RUN: Would insert L1 data cost"
                );
            }
        }
        Ok(())
    }

    /// Handles batches proved event, inserting proved batch data and calculating prove costs
    pub async fn handle_batches_proved(&self, wrapper: BatchesProvedWrapper) -> Result<()> {
        let proved = &wrapper.proved;
        let l1_block_number = wrapper.l1_block_number;
        let l1_tx_hash = wrapper.l1_tx_hash;

        // Insert proved batch
        if self.enable_db_writes {
            crate::event_processing::with_db_error_context(
                self.writer.insert_proved_batch(proved, l1_block_number),
                "insert proved batch",
                format!("batch_ids={:?}", proved.batch_ids_proved()),
            )
            .await?;
        } else {
            info!(
                batch_ids = ?proved.batch_ids_proved(),
                l1_block_number = l1_block_number,
                "🧪 DRY-RUN: Would insert proved batch"
            );
        }

        // Calculate and insert prove costs for each batch
        if let Some(cost) =
            crate::event_processing::fetch_transaction_cost(self.extractor, l1_tx_hash).await
        {
            let cost_per_batch = crate::event_processing::average_cost_per_batch(
                cost,
                proved.batch_ids_proved().len(),
            );

            if self.enable_db_writes {
                for batch_id in proved.batch_ids_proved() {
                    crate::event_processing::with_db_error_context(
                        self.writer.insert_prove_cost(l1_block_number, *batch_id, cost_per_batch),
                        "insert prove cost",
                        format!(
                            "l1_block_number={}, batch_id={}, tx_hash={:?}",
                            l1_block_number, batch_id, l1_tx_hash
                        ),
                    )
                    .await?;
                }
            } else {
                info!(
                    l1_block_number = l1_block_number,
                    batch_ids = ?proved.batch_ids_proved(),
                    cost_per_batch = cost_per_batch,
                    "🧪 DRY-RUN: Would insert prove costs for {} batches",
                    proved.batch_ids_proved().len()
                );
            }
        }
        Ok(())
    }

    /// Handles forced inclusion processed event, inserting forced inclusion data
    pub async fn handle_forced_inclusion(
        &self,
        wrapper: ForcedInclusionProcessedWrapper,
    ) -> Result<()> {
        let event = &wrapper.event;

        if self.enable_db_writes {
            crate::event_processing::with_db_error_context(
                self.writer.insert_forced_inclusion(event),
                "insert forced inclusion",
                format!("blob_hash={:?}", event.forcedInclusion.blobHash),
            )
            .await?;
        } else {
            info!(
                blob_hash = ?event.forcedInclusion.blobHash,
                "🧪 DRY-RUN: Would insert forced inclusion"
            );
        }

        Ok(())
    }
}
