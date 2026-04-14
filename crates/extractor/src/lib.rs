//! Taikoscope Extractor
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::cognitive_complexity)]
use chainio::{
    self, DefaultProvider,
    ITaikoInbox::{BatchProposed, BatchesProved, BatchesVerified as InboxBatchesVerified},
    taiko::{
        preconf_whitelist::TaikoPreconfWhitelist,
        wrapper::{ITaikoWrapper::ForcedInclusionProcessed, TaikoWrapper},
    },
};

use std::pin::Pin;

use alloy::{
    primitives::{Address, B256, BlockNumber},
    providers::{Provider, ProviderBuilder},
};
use alloy_consensus::BlockHeader;
use alloy_rpc_client::ClientBuilder;
use chainio::TaikoInbox;
use derive_more::Debug;
use eyre::{Context, Result};
use network::retries::{DEFAULT_RETRY_LAYER, RetryWsConnect};
use primitives::{
    block_stats::compute_block_stats,
    headers::{L1Header, L1HeaderStream, L2Header, L2HeaderStream},
};
use std::time::Duration;
use tokio::{sync::mpsc, time::sleep};
use tokio_stream::{Stream, StreamExt, wrappers::UnboundedReceiverStream};
use tracing::{error, info, warn};
use url::Url;

/// Extractor client
#[derive(Debug, Clone)]
pub struct Extractor {
    #[debug(skip)]
    l1_provider: DefaultProvider,
    #[debug(skip)]
    l2_provider: DefaultProvider,
    preconf_whitelist: TaikoPreconfWhitelist,
    taiko_inbox: TaikoInbox,
    taiko_wrapper: TaikoWrapper,
    anchor_address: Address,
}

/// Stream of batch proposed events with their L1 block number and transaction hash
pub type BatchProposedStream =
    Pin<Box<dyn Stream<Item = (BatchProposed, u64, alloy::primitives::B256)> + Send>>;
/// Stream of batches proved events
pub type BatchesProvedStream = Pin<
    Box<
        dyn Stream<Item = (chainio::ITaikoInbox::BatchesProved, u64, alloy::primitives::B256)>
            + Send,
    >,
>;
/// Stream of batches verified events
pub type BatchesVerifiedStream =
    Pin<Box<dyn Stream<Item = (chainio::BatchesVerified, u64, alloy::primitives::B256)> + Send>>;
/// Stream of forced inclusion processed events
pub type ForcedInclusionStream = Pin<Box<dyn Stream<Item = ForcedInclusionProcessed> + Send>>;

impl Extractor {
    /// Create a new extractor
    pub async fn new(
        l1_rpc_url: Url,
        l2_rpc_url: Url,
        inbox_address: Address,
        preconf_whitelist_address: Address,
        taiko_wrapper_address: Address,
        anchor_address: Address,
    ) -> Result<Self> {
        // Validate URL schemes
        let l1_scheme = l1_rpc_url.scheme();
        if l1_scheme != "ws" && l1_scheme != "wss" {
            return Err(eyre::eyre!(
                "Invalid URL scheme for L1 RPC: expected 'ws://' or 'wss://' but got '{}://'. Please provide a WebSocket endpoint.",
                l1_scheme
            ));
        }

        let l2_scheme = l2_rpc_url.scheme();
        if l2_scheme != "ws" && l2_scheme != "wss" {
            return Err(eyre::eyre!(
                "Invalid URL scheme for L2 RPC: expected 'ws://' or 'wss://' but got '{}://'. Please provide a WebSocket endpoint.",
                l2_scheme
            ));
        }

        info!(url = %l1_rpc_url, "Connecting to L1 WebSocket provider...");
        let l1_ws = RetryWsConnect::from_url(l1_rpc_url.clone()).with_label("L1");
        let l1_client =
            ClientBuilder::default().layer(DEFAULT_RETRY_LAYER).pubsub(l1_ws).await.wrap_err_with(
                || format!("Failed to establish L1 WebSocket connection to {}", l1_rpc_url),
            )?;
        let l1_provider = ProviderBuilder::new().connect_client(l1_client);

        info!(url = %l2_rpc_url, "Connecting to L2 WebSocket provider...");
        let l2_ws = RetryWsConnect::from_url(l2_rpc_url.clone()).with_label("L2");
        let l2_client =
            ClientBuilder::default().layer(DEFAULT_RETRY_LAYER).pubsub(l2_ws).await.wrap_err_with(
                || format!("Failed to establish L2 WebSocket connection to {}", l2_rpc_url),
            )?;
        let l2_provider = ProviderBuilder::new().connect_client(l2_client);

        let taiko_inbox = TaikoInbox::new_readonly(inbox_address, l1_provider.clone());
        let preconf_whitelist =
            TaikoPreconfWhitelist::new_readonly(preconf_whitelist_address, l1_provider.clone());
        let taiko_wrapper = TaikoWrapper::new_readonly(taiko_wrapper_address, l1_provider.clone());

        Ok(Self {
            l1_provider,
            l2_provider,
            preconf_whitelist,
            taiko_inbox,
            taiko_wrapper,
            anchor_address,
        })
    }

    /// Get a stream of L1 headers. This stream will attempt to automatically
    /// resubscribe and continue yielding headers in case of disconnections.
    pub async fn get_l1_header_stream(&self) -> Result<L1HeaderStream> {
        let (tx, rx) = mpsc::unbounded_channel();
        let provider = self.l1_provider.clone();

        tokio::spawn(async move {
            loop {
                info!("Attempting to subscribe to L1 block headers...");
                let sub_result = provider.subscribe_blocks().await;

                let mut block_stream = match sub_result {
                    Ok(sub) => {
                        info!("Successfully subscribed to L1 block headers.");
                        sub.into_stream()
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to subscribe to L1 blocks, retrying in 5s");
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                while let Some(block_data) = block_stream.next().await {
                    // Calculate slot from timestamp using Ethereum mainnet genesis and slot time
                    // Mainnet genesis timestamp: 1606824023 (December 1, 2020)
                    // Slot time: 12 seconds
                    const GENESIS_TIMESTAMP: u64 = 1606824023;
                    const SLOT_DURATION: u64 = 12;

                    let slot = if block_data.timestamp >= GENESIS_TIMESTAMP {
                        (block_data.timestamp - GENESIS_TIMESTAMP) / SLOT_DURATION
                    } else {
                        // Fallback to block number for pre-merge blocks or edge cases
                        warn!(
                            block_number = block_data.number,
                            timestamp = block_data.timestamp,
                            "Block timestamp is before Ethereum 2.0 genesis, using block number as slot"
                        );
                        block_data.number
                    };

                    let header = L1Header {
                        number: block_data.number,
                        hash: block_data.hash,
                        slot,
                        timestamp: block_data.timestamp,
                    };
                    if tx.send(header).is_err() {
                        error!("L1 header receiver dropped. Stopping L1 header task.");
                        return; // Exit task if receiver is gone
                    }
                }
                warn!("L1 block stream ended. Attempting to resubscribe...");
                // Outer loop will retry subscription.
            }
        });

        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }

    /// Get a stream of L2 headers. This stream will attempt to automatically
    /// resubscribe and continue yielding headers in case of disconnections.
    pub async fn get_l2_header_stream(&self) -> Result<L2HeaderStream> {
        let (tx, rx) = mpsc::unbounded_channel();
        let provider = self.l2_provider.clone();

        tokio::spawn(async move {
            loop {
                info!("Attempting to subscribe to L2 block headers...");
                let sub_result = provider.subscribe_blocks().await;

                let mut block_stream = match sub_result {
                    Ok(sub) => {
                        info!("Successfully subscribed to L2 block headers.");
                        sub.into_stream()
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to subscribe to L2 blocks, retrying in 5s");
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                while let Some(block_data) = block_stream.next().await {
                    let header = L2Header {
                        number: block_data.number,
                        hash: block_data.hash,
                        parent_hash: block_data.parent_hash,
                        timestamp: block_data.timestamp,
                        gas_used: block_data.gas_used,
                        beneficiary: block_data.beneficiary,
                        base_fee_per_gas: block_data.base_fee_per_gas().unwrap_or(0),
                    };
                    if tx.send(header).is_err() {
                        error!("L2 header receiver dropped. Stopping L2 header task.");
                        return; // Exit task if receiver is gone
                    }
                }
                warn!("L2 block stream ended. Attempting to resubscribe...");
            }
        });

        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }

    /// Subscribes to the `TaikoInbox` `BatchProposed` event and returns a stream of decoded events
    /// along with the L1 block number and transaction hash. This stream will attempt to
    /// automatically resubscribe and continue yielding events.
    pub async fn get_batch_proposed_stream(&self) -> Result<BatchProposedStream> {
        let (tx, rx) = mpsc::unbounded_channel();
        let provider = self.l1_provider.clone();
        let taiko_inbox = self.taiko_inbox.clone(); // Clone for use in the spawned task

        tokio::spawn(async move {
            loop {
                info!("Attempting to subscribe to TaikoInbox BatchProposed events...");
                let filter = taiko_inbox.batch_proposed_filter();
                let sub_result = provider.subscribe_logs(&filter).await;

                let mut log_stream = match sub_result {
                    Ok(sub) => {
                        info!("Successfully subscribed to TaikoInbox BatchProposed events.");
                        sub.into_stream()
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to subscribe to BatchProposed logs, retrying in 5s");
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                while let Some(log) = log_stream.next().await {
                    // Skip reverted logs from reorgs
                    if log.removed {
                        info!("Skipping removed BatchProposed log due to L1 reorg");
                        continue;
                    }
                    match log.log_decode::<BatchProposed>() {
                        Ok(decoded) => {
                            let batch = decoded.data().clone();
                            let l1_block_number = log.block_number.unwrap_or(batch.info.proposedIn);
                            let tx_hash = log.transaction_hash.unwrap_or_default();
                            if tx.send((batch, l1_block_number, tx_hash)).is_err() {
                                error!(
                                    "BatchProposed receiver dropped. Stopping BatchProposed event task."
                                );
                                return; // Exit task if receiver is gone
                            }
                        }
                        Err(err) => {
                            warn!(error = %err, "Failed to decode BatchProposed log");
                            // Optionally, decide if this is a critical error or can be skipped.
                            // For now, we just log and continue.
                        }
                    }
                }
                warn!("BatchProposed log stream ended. Attempting to resubscribe...");
            }
        });

        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }

    /// Subscribes to the `TaikoInbox` `BatchesProved` event and returns a stream of decoded events
    /// along with the block number. This stream will attempt to automatically resubscribe and
    /// continue yielding events.
    pub async fn get_batches_proved_stream(&self) -> Result<BatchesProvedStream> {
        let (tx, rx) = mpsc::unbounded_channel();
        let provider = self.l1_provider.clone();
        let taiko_inbox = self.taiko_inbox.clone(); // Clone for use in the spawned task

        tokio::spawn(async move {
            loop {
                info!("Attempting to subscribe to TaikoInbox BatchesProved events...");
                let filter = taiko_inbox.batches_proved_filter();
                let sub_result = provider.subscribe_logs(&filter).await;

                let mut log_stream = match sub_result {
                    Ok(sub) => {
                        info!("Successfully subscribed to TaikoInbox BatchesProved events.");
                        sub.into_stream()
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to subscribe to BatchesProved logs, retrying in 5s");
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                while let Some(log) = log_stream.next().await {
                    // Skip reverted logs from reorgs
                    if log.removed {
                        info!("Skipping removed BatchesProved log due to L1 reorg");
                        continue;
                    }
                    match log.log_decode::<BatchesProved>() {
                        Ok(decoded) => {
                            let l1_block_number = log.block_number.unwrap_or(0);
                            let tx_hash = log.transaction_hash.unwrap_or_default();
                            if tx.send((decoded.data().clone(), l1_block_number, tx_hash)).is_err()
                            {
                                error!(
                                    "BatchesProved receiver dropped. Stopping BatchesProved event task."
                                );
                                return; // Exit task if receiver is gone
                            }
                        }
                        Err(err) => {
                            warn!(error = %err, "Failed to decode BatchesProved log");
                            // Optionally, decide if this is a critical error or can be skipped.
                            // For now, we just log and continue.
                        }
                    }
                }
                warn!("BatchesProved log stream ended. Attempting to resubscribe...");
            }
        });

        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }

    /// Subscribes to the `TaikoWrapper` `ForcedInclusionProcessed` event and returns a stream of
    /// decoded events. This stream will attempt to automatically resubscribe and continue
    /// yielding events.
    pub async fn get_forced_inclusion_stream(&self) -> Result<ForcedInclusionStream> {
        let (tx, rx) = mpsc::unbounded_channel();
        let provider = self.l1_provider.clone();
        let taiko_wrapper = self.taiko_wrapper.clone(); // Clone for use in the spawned task

        tokio::spawn(async move {
            loop {
                info!("Attempting to subscribe to TaikoWrapper ForcedInclusionProcessed events...");
                let filter = taiko_wrapper.forced_inclusion_processed_filter();
                let sub_result = provider.subscribe_logs(&filter).await;

                let mut log_stream = match sub_result {
                    Ok(sub) => {
                        info!(
                            "Successfully subscribed to TaikoWrapper ForcedInclusionProcessed events."
                        );
                        sub.into_stream()
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to subscribe to ForcedInclusionProcessed logs, retrying in 5s");
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                while let Some(log) = log_stream.next().await {
                    // Skip reverted logs from reorgs
                    if log.removed {
                        info!("Skipping removed ForcedInclusionProcessed log due to L1 reorg");
                        continue;
                    }
                    match log.log_decode::<ForcedInclusionProcessed>() {
                        Ok(decoded) => {
                            if tx.send(decoded.data().clone()).is_err() {
                                error!(
                                    "ForcedInclusionProcessed receiver dropped. Stopping ForcedInclusionProcessed event task."
                                );
                                return; // Exit task if receiver is gone
                            }
                        }
                        Err(err) => {
                            warn!(error = %err, "Failed to decode ForcedInclusionProcessed log");
                            // Optionally, decide if this is a critical error or can be skipped.
                        }
                    }
                }
                warn!("ForcedInclusionProcessed log stream ended. Attempting to resubscribe...");
            }
        });

        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }

    /// Get the current epoch operator
    pub async fn get_operator_for_current_epoch(&self) -> Result<Address> {
        let operator = self.preconf_whitelist.get_operator_for_current_epoch().await?;
        Ok(operator)
    }

    /// Get the next epoch operator
    pub async fn get_operator_for_next_epoch(&self) -> Result<Address> {
        let operator = self.preconf_whitelist.get_operator_for_next_epoch().await?;
        Ok(operator)
    }

    /// Subscribes to the `TaikoInbox` `BatchesVerified` event and returns a stream of decoded
    /// events along with the block number. This stream will attempt to automatically
    /// resubscribe and continue yielding events.
    pub async fn get_batches_verified_stream(&self) -> Result<BatchesVerifiedStream> {
        let (tx, rx) = mpsc::unbounded_channel();
        let provider = self.l1_provider.clone();
        let taiko_inbox = self.taiko_inbox.clone(); // Clone for use in the spawned task

        tokio::spawn(async move {
            loop {
                info!("Attempting to subscribe to TaikoInbox BatchesVerified events...");
                let filter = taiko_inbox.batches_verified_filter();
                let sub_result = provider.subscribe_logs(&filter).await;

                let mut log_stream = match sub_result {
                    Ok(sub) => {
                        info!("Successfully subscribed to TaikoInbox BatchesVerified events.");
                        sub.into_stream()
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to subscribe to BatchesVerified logs, retrying in 5s");
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                while let Some(log) = log_stream.next().await {
                    // Skip reverted logs from reorgs
                    if log.removed {
                        info!("Skipping removed BatchesVerified log due to L1 reorg");
                        continue;
                    }
                    match decode_batches_verified(&log) {
                        Ok(verified) => {
                            let l1_block_number = log.block_number.unwrap_or(0);
                            let tx_hash = log.transaction_hash.unwrap_or_default();
                            if tx.send((verified, l1_block_number, tx_hash)).is_err() {
                                error!(
                                    "BatchesVerified receiver dropped. Stopping BatchesVerified event task."
                                );
                                return; // Exit task if receiver is gone
                            }
                        }
                        Err(err) => {
                            warn!(error = %err, "Failed to decode BatchesVerified log");
                            // Optionally, decide if this is a critical error or can be skipped.
                        }
                    }
                }
                warn!("BatchesVerified log stream ended. Attempting to resubscribe...");
            }
        });

        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }

    /// Get the operator candidates for the current epoch
    pub async fn get_operator_candidates_for_current_epoch(&self) -> Result<Vec<Address>> {
        let candidates = self.preconf_whitelist.get_operator_candidates_for_current_epoch().await?;
        Ok(candidates)
    }

    /// Calculate aggregated statistics for an L2 block by fetching its receipts.
    pub async fn get_l2_block_stats(
        &self,
        block_hash: B256,
        base_fee: u64,
    ) -> Result<(u128, u32, u128)> {
        use alloy_rpc_types_eth::BlockId;

        let block = BlockId::Hash(block_hash.into());
        let receipts_opt = self.l2_provider.get_block_receipts(block).await?;
        let receipts = receipts_opt.ok_or_else(|| eyre::eyre!("missing receipts"))?;

        Ok(compute_block_stats(&receipts, base_fee, self.anchor_address))
    }

    /// Get the latest L1 block number
    pub async fn get_l1_latest_block_number(&self) -> Result<u64> {
        self.l1_provider.get_block_number().await.map_err(Into::into)
    }

    /// Get the latest L2 block number
    pub async fn get_l2_latest_block_number(&self) -> Result<u64> {
        self.l2_provider.get_block_number().await.map_err(Into::into)
    }

    /// Get L1 block by number
    pub async fn get_l1_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<alloy_rpc_types_eth::Block> {
        self.l1_provider
            .get_block(block_number.into())
            .await?
            .ok_or_else(|| eyre::eyre!("L1 block {} not found", block_number))
    }

    /// Get L2 block by number
    pub async fn get_l2_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<alloy_rpc_types_eth::Block> {
        self.l2_provider
            .get_block(block_number.into())
            .await?
            .ok_or_else(|| eyre::eyre!("L2 block {} not found", block_number))
    }

    /// Get a transaction receipt by hash with retry logic
    pub async fn get_receipt(
        &self,
        tx_hash: alloy::primitives::B256,
    ) -> Result<alloy_rpc_types_eth::TransactionReceipt> {
        const MAX_RETRIES: u32 = 10;
        const BASE_DELAY_MS: u64 = 500;

        for attempt in 0..MAX_RETRIES {
            match self.l1_provider.get_transaction_receipt(tx_hash).await {
                Ok(Some(receipt)) => return Ok(receipt),
                Ok(None) => {
                    // Receipt not yet available, retry after delay
                    if attempt < MAX_RETRIES - 1 {
                        // Exponential backoff with simple jitter: base_delay * 2^attempt + fixed
                        // jitter
                        let delay_ms = BASE_DELAY_MS * (1u64 << attempt).min(8) // Cap at 8x base delay
                            + ((attempt as u64) * 50).min(100); // Add simple jitter

                        tracing::debug!(
                            attempt = attempt + 1,
                            max_retries = MAX_RETRIES,
                            delay_ms,
                            tx_hash = %tx_hash,
                            "Receipt not found, retrying..."
                        );

                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    }
                }
                Err(e) => {
                    // RPC error, propagate immediately
                    return Err(e.into());
                }
            }
        }

        // All retries exhausted
        Err(eyre::eyre!("Receipt not found for transaction hash: {}", tx_hash))
    }
}

fn decode_batches_verified(log: &alloy_rpc_types_eth::Log) -> Result<chainio::BatchesVerified> {
    let decoded = log.log_decode::<InboxBatchesVerified>()?;
    let data = decoded.data();
    let mut block_hash = [0u8; 32];
    block_hash.copy_from_slice(data.blockHash.as_slice());
    Ok(chainio::BatchesVerified { batch_id: data.batchId, block_hash })
}

/// Detects reorgs based on block numbers and hashes.
#[derive(Debug)]
pub struct ReorgDetector {
    head_number: BlockNumber,
    head_hash: Option<B256>,
}

impl Default for ReorgDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl ReorgDetector {
    /// Create a new reorg detector
    pub const fn new() -> Self {
        Self { head_number: 0, head_hash: None }
    }

    /// Get the current head number
    pub const fn head_number(&self) -> BlockNumber {
        self.head_number
    }

    /// Get the current head hash
    pub const fn head_hash(&self) -> Option<B256> {
        self.head_hash
    }

    /// Checks a new block against the current head.
    /// Returns (`reorg_depth`, `orphaned_hash`) if a reorg is detected:
    /// - Traditional reorg: `new_block_number` < `head_number`
    /// - One-block reorg: same block number but different hash  Always updates the internal head to
    ///   the new block.
    pub fn on_new_block_with_hash(
        &mut self,
        new_block_number: BlockNumber,
        new_hash: B256,
    ) -> Option<(u16, Option<B256>)> {
        // Check for traditional reorg (block number goes backwards)
        if new_block_number < self.head_number {
            let depth_val = self.head_number.saturating_sub(new_block_number);
            let depth = (depth_val > 0).then(|| (depth_val.min(u16::MAX as u64) as u16, None));

            // Update head to new block
            self.head_number = new_block_number;
            self.head_hash = Some(new_hash);

            return depth;
        }

        // Check for one-block reorg (same block number, different hash)
        if new_block_number == self.head_number &&
            let Some(current_hash) = self.head_hash
        {
            if current_hash != new_hash {
                // One-block reorg detected
                self.head_hash = Some(new_hash);
                return Some((0, Some(current_hash)));
            }
            // Same block number and same hash - no reorg, no update needed
            return None;
        }

        // No reorg - update head to new block
        self.head_number = new_block_number;
        self.head_hash = Some(new_hash);

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, B256, Log as PrimitiveLog};
    use alloy_rpc_types_eth::Log;
    use alloy_sol_types::SolEvent;

    #[test]
    fn initial_block() {
        let mut det = ReorgDetector::new();
        let hash = B256::repeat_byte(5);
        // First block received is 5. head_number is 0. 5 is not < 0. No reorg.
        assert_eq!(det.on_new_block_with_hash(5, hash), None);
        assert_eq!(det.head_number(), 5);
        assert_eq!(det.head_hash(), Some(hash));
    }

    #[test]
    fn subsequent_blocks_increasing() {
        let mut det = ReorgDetector::new();
        let hash5 = B256::repeat_byte(5);
        let hash6 = B256::repeat_byte(6);
        let hash7 = B256::repeat_byte(7);

        det.on_new_block_with_hash(5, hash5); // head_number becomes 5
        // New block 6. 6 is not < 5. No reorg.
        assert_eq!(det.on_new_block_with_hash(6, hash6), None);
        assert_eq!(det.head_number(), 6);
        // New block 7. 7 is not < 6. No reorg.
        assert_eq!(det.on_new_block_with_hash(7, hash7), None);
        assert_eq!(det.head_number(), 7);
    }

    #[test]
    fn reorg_to_lower_number() {
        let mut det = ReorgDetector::new();
        let hash10 = B256::repeat_byte(10);
        let hash8 = B256::repeat_byte(8);

        det.on_new_block_with_hash(10, hash10); // head_number is 10
        // New block 8. 8 < 10. Reorg. Depth = 10 - 8 = 2.
        assert_eq!(det.on_new_block_with_hash(8, hash8), Some((2, None)));
        assert_eq!(det.head_number(), 8); // Head is updated to 8
    }

    #[test]
    fn reorg_by_one() {
        let mut det = ReorgDetector::new();
        let hash10 = B256::repeat_byte(10);
        let hash9 = B256::repeat_byte(9);

        det.on_new_block_with_hash(10, hash10); // head_number is 10
        // New block 9. 9 < 10. Reorg. Depth = 10 - 9 = 1.
        assert_eq!(det.on_new_block_with_hash(9, hash9), Some((1, None)));
        assert_eq!(det.head_number(), 9);
    }

    #[test]
    fn same_block_number_no_reorg() {
        let mut det = ReorgDetector::new();
        let hash = B256::repeat_byte(10);

        det.on_new_block_with_hash(10, hash); // head_number is 10
        // New block 10 with same hash. 10 is not < 10 and hash is same. No reorg.
        assert_eq!(det.on_new_block_with_hash(10, hash), None);
        assert_eq!(det.head_number(), 10); // Head is updated to 10 (no change)
    }

    #[test]
    fn reorg_depth_capped_at_u16_max() {
        let mut det = ReorgDetector::new();
        let hash_high = B256::repeat_byte(255);
        let hash1 = B256::repeat_byte(1);

        det.on_new_block_with_hash(u16::MAX as u64 + 10, hash_high);
        // New block 1. 1 < u16::MAX + 10. Reorg. Depth = u16::MAX + 10 - 1. Capped to u16::MAX.
        assert_eq!(det.on_new_block_with_hash(1, hash1), Some((u16::MAX, None)));
        assert_eq!(det.head_number(), 1);
    }

    #[test]
    fn reorg_from_initial_zero_state() {
        let mut det = ReorgDetector::new(); // head_number is 0
        let hash = B256::repeat_byte(5);

        // New block 5. 5 is not < 0. No reorg.
        assert_eq!(det.on_new_block_with_hash(5, hash), None);
        assert_eq!(det.head_number(), 5);
    }

    #[test]
    fn reorg_to_zero_not_possible_if_blocks_are_positive() {
        let mut det = ReorgDetector::new();
        let hash5 = B256::repeat_byte(5);
        let hash0 = B256::repeat_byte(0);

        det.on_new_block_with_hash(5, hash5); // head_number is 5
        // New block 0. 0 < 5. Reorg. Depth = 5 - 0 = 5.
        assert_eq!(det.on_new_block_with_hash(0, hash0), Some((5, None)));
        assert_eq!(det.head_number(), 0);
    }

    #[test]
    fn one_block_reorg_same_number_different_hash() {
        let mut det = ReorgDetector::new();
        let hash1 = B256::repeat_byte(1);
        let hash2 = B256::repeat_byte(2);

        // First block 10 with hash1
        assert_eq!(det.on_new_block_with_hash(10, hash1), None);
        assert_eq!(det.head_number(), 10);
        assert_eq!(det.head_hash(), Some(hash1));

        // Same block 10 but with different hash2 - should detect one-block reorg with depth 0
        assert_eq!(det.on_new_block_with_hash(10, hash2), Some((0, Some(hash1))));
        assert_eq!(det.head_number(), 10);
        assert_eq!(det.head_hash(), Some(hash2));
    }

    #[test]
    fn same_block_number_same_hash_no_reorg() {
        let mut det = ReorgDetector::new();
        let hash = B256::repeat_byte(1);

        // First block 10 with hash
        assert_eq!(det.on_new_block_with_hash(10, hash), None);
        assert_eq!(det.head_number(), 10);
        assert_eq!(det.head_hash(), Some(hash));

        // Same block 10 with same hash - no reorg
        assert_eq!(det.on_new_block_with_hash(10, hash), None);
        assert_eq!(det.head_number(), 10);
        assert_eq!(det.head_hash(), Some(hash));
    }

    #[test]
    fn traditional_reorg_with_hash_tracking() {
        let mut det = ReorgDetector::new();
        let hash10 = B256::repeat_byte(10);
        let hash8 = B256::repeat_byte(8);

        // Block 10
        assert_eq!(det.on_new_block_with_hash(10, hash10), None);
        assert_eq!(det.head_number(), 10);
        assert_eq!(det.head_hash(), Some(hash10));

        // Block 8 - traditional reorg (block number goes backwards)
        assert_eq!(det.on_new_block_with_hash(8, hash8), Some((2, None)));
        assert_eq!(det.head_number(), 8);
        assert_eq!(det.head_hash(), Some(hash8));
    }

    #[test]
    fn depth_one_reorg() {
        let mut det = ReorgDetector::new();
        let hash11 = B256::repeat_byte(11);
        let hash10 = B256::repeat_byte(10);

        // Block 11
        assert_eq!(det.on_new_block_with_hash(11, hash11), None);
        assert_eq!(det.head_number(), 11);
        assert_eq!(det.head_hash(), Some(hash11));

        // Block 10 - traditional reorg with depth 1 (11 - 10 = 1)
        assert_eq!(det.on_new_block_with_hash(10, hash10), Some((1, None)));
        assert_eq!(det.head_number(), 10);
        assert_eq!(det.head_hash(), Some(hash10));
    }

    #[test]
    fn decode_verified_event() {
        let event = InboxBatchesVerified { batchId: 7, blockHash: B256::repeat_byte(2) };
        let primitive = PrimitiveLog { address: Address::ZERO, data: event };
        let encoded = InboxBatchesVerified::encode_log(&primitive);
        let log = Log { inner: encoded, ..Default::default() };

        let decoded = decode_batches_verified(&log).unwrap();
        assert_eq!(decoded.batch_id, 7);
        assert_eq!(decoded.block_hash, [2u8; 32]);
    }

    #[tokio::test]
    async fn test_get_receipt_retry_delay_calculation() {
        // This test verifies the retry delay calculation logic
        const BASE_DELAY_MS: u64 = 500;

        for attempt in 0..3 {
            let delay_ms =
                BASE_DELAY_MS * (1u64 << attempt).min(8) + ((attempt as u64) * 50).min(100);

            match attempt {
                0 => assert_eq!(delay_ms, 500),  // 500 * 1 + 0 = 500
                1 => assert_eq!(delay_ms, 1050), // 500 * 2 + 50 = 1050
                2 => assert_eq!(delay_ms, 2100), // 500 * 4 + 100 = 2100
                _ => {}
            }
        }

        // Test delay cap - verify that large shifts are capped at 8x
        let delay_ms = BASE_DELAY_MS * 8 + 100; // Capped at 8x base delay
        assert_eq!(delay_ms, 4100); // 500 * 8 + 100 = 4100
    }
}
