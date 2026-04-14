//! Event processing methods for the Driver
#![allow(missing_docs)]

use clickhouse::{AddressBytes, HashBytes, L2HeadEvent};
use extractor::Extractor;
use eyre::Result;
use messages::{
    BatchProposedWrapper, BatchesProvedWrapper, ForcedInclusionProcessedWrapper, TaikoEvent,
};
use tracing::{error, info, warn};

use crate::event_handler::EventHandler;

/// Event processing methods for the Driver
impl crate::driver::Driver {
    /// Process an event and insert it into the database
    pub async fn process_event(&mut self, event: TaikoEvent) -> Result<()> {
        // Handle dry-run mode with detailed logging
        if !self.enable_db_writes {
            return self.process_event_dry_run(event).await;
        }

        // Check writer exists early - this should never happen if configuration is correct
        if self.clickhouse_writer.is_none() {
            return Err(eyre::eyre!(
                "ClickHouse writer not available but database writes are enabled. This indicates a configuration error."
            ));
        }

        // Process each event type with proper error handling
        match event {
            TaikoEvent::L1Header(header) => {
                info!(block_number = header.number, hash = %header.hash, "Processing L1 header");
                self.handle_l1_header_event(header).await
            }
            TaikoEvent::L2Header(header) => {
                info!(block_number = header.number, hash = %header.hash, "Processing L2 header");
                self.handle_l2_header_event(header).await
            }
            TaikoEvent::BatchProposed(wrapper) => {
                info!(last_block = wrapper.batch.last_block_number(), "Processing batch proposed");
                self.handle_batch_proposed_event(wrapper).await
            }
            TaikoEvent::ForcedInclusionProcessed(wrapper) => {
                info!(blob_hash = ?wrapper.event.forcedInclusion.blobHash, "Processing forced inclusion");
                self.handle_forced_inclusion_event(wrapper).await
            }
            TaikoEvent::BatchesProved(wrapper) => {
                info!(batch_ids = ?wrapper.proved.batch_ids_proved(), "Processing batches proved");
                self.handle_batches_proved_event(wrapper).await
            }
        }
    }

    /// Process an event in dry-run mode with detailed logging but no database writes
    pub async fn process_event_dry_run(&mut self, event: TaikoEvent) -> Result<()> {
        match event {
            TaikoEvent::L1Header(header) => {
                info!(
                    block_number = header.number,
                    hash = %header.hash,
                    slot = header.slot,
                    timestamp = header.timestamp,
                    "🧪 DRY-RUN: Would process L1 header"
                );

                // Simulate preconf data processing
                info!(
                    block_number = header.number,
                    "🧪 DRY-RUN: Would insert L1 header and process preconf data"
                );

                // Still run preconf data logic for validation (but won't write to DB)
                crate::preconf::process_preconf_data(
                    &self.extractor,
                    &self.clickhouse_writer,
                    &header,
                    self.enable_db_writes,
                )
                .await;

                Ok(())
            }
            TaikoEvent::L2Header(header) => {
                info!(
                    block_number = header.number,
                    hash = %header.hash,
                    beneficiary = %header.beneficiary,
                    gas_used = header.gas_used,
                    base_fee = header.base_fee_per_gas,
                    timestamp = header.timestamp,
                    "🧪 DRY-RUN: Would process L2 header"
                );

                // Still run reorg detection for validation (but won't write to DB)
                crate::reorg_detection::process_reorg_detection(
                    &mut self.reorg_detector,
                    &mut self.last_l2_header,
                    &self.clickhouse_writer,
                    &self.clickhouse_reader,
                    &header,
                )
                .await;

                // Simulate stats calculation
                let (sum_gas_used, sum_tx, sum_priority_fee) = self.extractor
                    .get_l2_block_stats(alloy_primitives::B256::from(*header.hash), header.base_fee_per_gas)
                    .await
                    .unwrap_or_else(|e| {
                        warn!(header_number = header.number, err = %e, "🧪 DRY-RUN: Failed to get L2 block stats");
                        (0, 0, 0)
                    });

                let sum_base_fee = sum_gas_used.saturating_mul(header.base_fee_per_gas as u128);

                info!(
                    block_number = header.number,
                    sum_gas_used = sum_gas_used,
                    sum_tx = sum_tx,
                    sum_priority_fee = sum_priority_fee,
                    sum_base_fee = sum_base_fee,
                    "🧪 DRY-RUN: Would insert L2 header with calculated stats"
                );

                Ok(())
            }
            TaikoEvent::BatchProposed(wrapper) => {
                let batch = &wrapper.batch;
                info!(
                    batch_id = batch.meta.batchId,
                    last_block = batch.last_block_number(),
                    l1_tx_hash = %wrapper.l1_tx_hash,
                    block_count = batch.info.blocks.len(),
                    "🧪 DRY-RUN: Would process BatchProposed"
                );

                // Simulate cost calculation
                if let Some(cost) =
                    fetch_transaction_cost(&self.extractor, wrapper.l1_tx_hash).await
                {
                    info!(
                        batch_id = batch.meta.batchId,
                        l1_data_cost = cost,
                        "🧪 DRY-RUN: Would insert L1 data cost"
                    );
                }

                info!(batch_id = batch.meta.batchId, "🧪 DRY-RUN: Would insert batch row");

                Ok(())
            }
            TaikoEvent::ForcedInclusionProcessed(wrapper) => {
                info!(
                    blob_hash = ?wrapper.event.forcedInclusion.blobHash,
                    "🧪 DRY-RUN: Would process ForcedInclusionProcessed"
                );

                info!(
                    blob_hash = ?wrapper.event.forcedInclusion.blobHash,
                    "🧪 DRY-RUN: Would insert forced inclusion record"
                );

                Ok(())
            }
            TaikoEvent::BatchesProved(wrapper) => {
                let proved = &wrapper.proved;
                info!(
                    batch_ids = ?proved.batch_ids_proved(),
                    l1_block_number = wrapper.l1_block_number,
                    l1_tx_hash = %wrapper.l1_tx_hash,
                    "🧪 DRY-RUN: Would process BatchesProved"
                );

                // Simulate cost calculation
                if let Some(cost) =
                    fetch_transaction_cost(&self.extractor, wrapper.l1_tx_hash).await
                {
                    let cost_per_batch =
                        average_cost_per_batch(cost, proved.batch_ids_proved().len());
                    info!(
                        batch_count = proved.batch_ids_proved().len(),
                        total_cost = cost,
                        cost_per_batch = cost_per_batch,
                        "🧪 DRY-RUN: Would insert prove costs"
                    );
                }

                info!(
                    batch_ids = ?proved.batch_ids_proved(),
                    "🧪 DRY-RUN: Would insert proved batch records"
                );

                Ok(())
            }
        }
    }

    // Event handler methods
    pub async fn handle_l1_header_event(
        &self,
        header: primitives::headers::L1Header,
    ) -> Result<()> {
        let writer = self.clickhouse_writer.as_ref().ok_or_else(|| {
            eyre::eyre!("ClickHouse writer not available for L1 header processing")
        })?;

        // Insert L1 header
        with_db_error_context(
            writer.insert_l1_header(&header),
            "insert L1 header",
            format!("header_number={}", header.number),
        )
        .await?;

        // Process preconfirmation data
        crate::preconf::process_preconf_data(
            &self.extractor,
            &self.clickhouse_writer,
            &header,
            self.enable_db_writes,
        )
        .await;

        Ok(())
    }

    pub async fn handle_l2_header_event(
        &mut self,
        header: primitives::headers::L2Header,
    ) -> Result<()> {
        // Process reorg detection
        crate::reorg_detection::process_reorg_detection(
            &mut self.reorg_detector,
            &mut self.last_l2_header,
            &self.clickhouse_writer,
            &self.clickhouse_reader,
            &header,
        )
        .await;

        // Insert L2 header with block statistics
        self.insert_l2_header_with_stats(&header).await;

        Ok(())
    }

    pub async fn handle_batch_proposed_event(&self, wrapper: BatchProposedWrapper) -> Result<()> {
        let writer = self.clickhouse_writer.as_ref().ok_or_else(|| {
            eyre::eyre!("ClickHouse writer not available for batch proposed processing")
        })?;

        let handler = EventHandler::new(writer, &self.extractor, self.enable_db_writes);
        handler.handle_batch_proposed(wrapper).await
    }

    pub async fn handle_forced_inclusion_event(
        &self,
        wrapper: ForcedInclusionProcessedWrapper,
    ) -> Result<()> {
        let writer = self.clickhouse_writer.as_ref().ok_or_else(|| {
            eyre::eyre!("ClickHouse writer not available for forced inclusion processing")
        })?;

        let handler = EventHandler::new(writer, &self.extractor, self.enable_db_writes);
        handler.handle_forced_inclusion(wrapper).await
    }

    pub async fn handle_batches_proved_event(&self, wrapper: BatchesProvedWrapper) -> Result<()> {
        let writer = self.clickhouse_writer.as_ref().ok_or_else(|| {
            eyre::eyre!("ClickHouse writer not available for batches proved processing")
        })?;

        let handler = EventHandler::new(writer, &self.extractor, self.enable_db_writes);
        handler.handle_batches_proved(wrapper).await
    }

    pub async fn insert_l2_header_with_stats(&self, header: &primitives::headers::L2Header) {
        let writer = match &self.clickhouse_writer {
            Some(w) => w,
            None => return,
        };

        let (sum_gas_used, sum_tx, sum_priority_fee) = self.extractor
            .get_l2_block_stats(alloy_primitives::B256::from(*header.hash), header.base_fee_per_gas)
            .await
            .unwrap_or_else(|e| {
                error!(header_number = header.number, err = %e, "Failed to get L2 block stats, using defaults");
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

        if let Err(e) = writer.insert_l2_header(&event).await {
            error!(header_number = header.number, err = %e, "Failed to insert L2 header");
        } else {
            info!(header_number = header.number, "Inserted L2 header with stats");
        }
    }
}

// Helper functions
pub async fn with_db_error_context<F, T>(future: F, operation: &str, context: String) -> Result<T>
where
    F: std::future::Future<Output = Result<T, eyre::Error>>,
{
    future.await.map_err(|e| {
        error!(
            err = %e,
            operation = operation,
            context = context,
            "Database operation failed"
        );
        eyre::eyre!("Failed to {}: {} - {}", operation, context, e)
    })
}

pub async fn fetch_transaction_cost(
    extractor: &Extractor,
    tx_hash: alloy_primitives::B256,
) -> Option<u128> {
    if tx_hash == alloy_primitives::B256::ZERO {
        return None;
    }

    match extractor.get_receipt(tx_hash).await {
        Ok(receipt) => Some(primitives::l1_data_cost::cost_from_receipt(&receipt)),
        Err(e) => {
            warn!(err = %e, tx_hash = %tx_hash, "Failed to fetch transaction receipt");
            None
        }
    }
}

pub const fn average_cost_per_batch(total_cost: u128, num_batches: usize) -> u128 {
    if num_batches == 0 { 0 } else { total_cost / num_batches as u128 }
}
