//! Taikoscope Extractor
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::cognitive_complexity)]
use chainio::{
    self, DefaultProvider,
    IInbox::{Proposed as InboxBatchProposed, Proved as InboxBatchesProved},
    taiko::preconf_whitelist::TaikoPreconfWhitelist,
};

use std::{borrow::Cow, pin::Pin, sync::Arc, time::Duration};

use alloy::{
    primitives::{Address, B256, BlockNumber, U256},
    providers::{Provider, ProviderBuilder},
    sol_types::SolCall,
};
use alloy_consensus::{BlockHeader, Transaction};
use alloy_rpc_client::{ClientBuilder, RpcClient};
use alloy_rpc_types_engine::JwtSecret;
use alloy_transport_http::{AuthLayer, Http, HyperClient};
use chainio::TaikoInbox;
use dashmap::DashMap;
use derive_more::Debug;
use eyre::{Context, Result};
use http_body_util::Full;
use hyper::body::Bytes as HyperBytes;
use hyper_util::{client::legacy::Client as HyperLegacyClient, rt::TokioExecutor};
use network::retries::{DEFAULT_RETRY_LAYER, RetryWsConnect};
use primitives::{
    block_stats::compute_block_stats,
    headers::{L1Header, L1HeaderStream, L2Header, L2HeaderStream},
};
use tokio::{sync::mpsc, time::sleep};
use tokio_stream::{Stream, StreamExt, wrappers::UnboundedReceiverStream};
use tower::ServiceBuilder;
use tracing::{debug, error, info, warn};
use url::Url;

const L1_BLOCK_CACHE_DEPTH: u64 = 10_000;

/// Extractor client
#[derive(Debug, Clone)]
pub struct Extractor {
    #[debug(skip)]
    l1_provider: DefaultProvider,
    #[debug(skip)]
    l2_provider: DefaultProvider,
    /// Optional JWT-authenticated L2 HTTP provider used exclusively for
    /// Shasta-era `taikoAuth_*` RPC methods (e.g. `taikoAuth_lastBlockIDByBatchID`).
    /// When `None`, the extractor falls back to `l2_provider`, which works on
    /// Pacaya-era networks but will return `None` for Shasta proposals.
    #[debug(skip)]
    l2_auth_provider: Option<DefaultProvider>,
    preconf_whitelist: TaikoPreconfWhitelist,
    taiko_inbox: TaikoInbox,
    anchor_address: Address,
    l1_block_cache: Arc<DashMap<u64, u64>>,
}

/// Stream of batch proposed events with their L1 block number and transaction hash
pub type BatchProposedStream =
    Pin<Box<dyn Stream<Item = (chainio::BatchProposed, u64, alloy::primitives::B256)> + Send>>;
/// Stream of batches proved events
pub type BatchesProvedStream =
    Pin<Box<dyn Stream<Item = (chainio::BatchesProved, u64, alloy::primitives::B256)> + Send>>;
/// Stream of forced inclusion processed events
pub type ForcedInclusionStream =
    Pin<Box<dyn Stream<Item = chainio::ForcedInclusionProcessed> + Send>>;

/// Decoded Shasta `Proposed` log with all Taikoscope events derived from the payload.
#[derive(Debug)]
pub struct DecodedBatchProposed {
    /// Proposal translated into the legacy batch-shaped model.
    pub batch: chainio::BatchProposed,
    /// L1 transaction hash that emitted the proposal log.
    pub tx_hash: B256,
    /// Forced inclusions derived from the proposal sources.
    pub forced_inclusions: Vec<chainio::ForcedInclusionProcessed>,
}

impl Extractor {
    /// Create a new extractor.
    ///
    /// `l2_auth_rpc_url` and `l2_jwt_secret_hex` are both optional and must be
    /// supplied together. When present they configure a dedicated JWT-signing
    /// HTTP provider used for Shasta-era `taikoAuth_*` methods (see
    /// [`Self::get_last_block_id_by_batch_id_via_rpc`]).
    pub async fn new(
        l1_rpc_url: Url,
        l2_rpc_url: Url,
        l2_auth_rpc_url: Option<Url>,
        l2_jwt_secret_hex: Option<String>,
        inbox_address: Address,
        preconf_whitelist_address: Address,
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

        let l2_auth_provider = match (l2_auth_rpc_url.as_ref(), l2_jwt_secret_hex.as_ref()) {
            (Some(url), Some(secret_hex)) => Some(build_l2_auth_provider(url.clone(), secret_hex)?),
            (None, None) => None,
            (Some(_), None) => {
                return Err(eyre::eyre!(
                    "L2_AUTH_RPC_URL is set but L2_JWT_SECRET_PATH is not — both must be provided together"
                ));
            }
            (None, Some(_)) => {
                return Err(eyre::eyre!(
                    "L2_JWT_SECRET_PATH is set but L2_AUTH_RPC_URL is not — both must be provided together"
                ));
            }
        };

        let taiko_inbox = TaikoInbox::new_readonly(inbox_address, l1_provider.clone());
        let preconf_whitelist =
            TaikoPreconfWhitelist::new_readonly(preconf_whitelist_address, l1_provider.clone());

        let l1_block_cache = Arc::new(DashMap::<u64, u64>::new());

        Ok(Self {
            l1_provider,
            l2_provider,
            l2_auth_provider,
            preconf_whitelist,
            taiko_inbox,
            anchor_address,
            l1_block_cache,
        })
    }

    /// Get a stream of L1 headers. This stream will attempt to automatically
    /// resubscribe and continue yielding headers in case of disconnections.
    pub async fn get_l1_header_stream(&self) -> Result<L1HeaderStream> {
        let (tx, rx) = mpsc::unbounded_channel();
        let provider = self.l1_provider.clone();
        let l1_block_cache = Arc::clone(&self.l1_block_cache);

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
                    insert_l1_block_timestamp(&l1_block_cache, header.number, header.timestamp);
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
        let extractor = self.clone();
        let taiko_inbox = self.taiko_inbox.clone();

        tokio::spawn(async move {
            loop {
                info!("Attempting to subscribe to TaikoInbox Proposed events...");
                let filter = taiko_inbox.batch_proposed_filter();
                let sub_result = provider.subscribe_logs(&filter).await;

                let mut log_stream = match sub_result {
                    Ok(sub) => {
                        info!("Successfully subscribed to TaikoInbox Proposed events.");
                        sub.into_stream()
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to subscribe to Proposed logs, retrying in 5s");
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                while let Some(log) = log_stream.next().await {
                    if log.removed {
                        info!("Skipping removed Proposed log due to L1 reorg");
                        continue;
                    }
                    match extractor.decode_batch_proposed_log(&log).await {
                        Ok(Some(decoded)) => {
                            let l1_block_number = decoded.batch.info.proposedIn;
                            if tx.send((decoded.batch, l1_block_number, decoded.tx_hash)).is_err() {
                                error!(
                                    "BatchProposed receiver dropped. Stopping Proposed event task."
                                );
                                return;
                            }
                        }
                        Ok(None) => {}
                        Err(err) => {
                            warn!(error = %err, "Failed to decode Proposed log");
                        }
                    }
                }
                warn!("Proposed log stream ended. Attempting to resubscribe...");
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
        let extractor = self.clone();
        let taiko_inbox = self.taiko_inbox.clone();

        tokio::spawn(async move {
            loop {
                info!("Attempting to subscribe to TaikoInbox Proved events...");
                let filter = taiko_inbox.batches_proved_filter();
                let sub_result = provider.subscribe_logs(&filter).await;

                let mut log_stream = match sub_result {
                    Ok(sub) => {
                        info!("Successfully subscribed to TaikoInbox Proved events.");
                        sub.into_stream()
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to subscribe to Proved logs, retrying in 5s");
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                while let Some(log) = log_stream.next().await {
                    if log.removed {
                        info!("Skipping removed Proved log due to L1 reorg");
                        continue;
                    }
                    match extractor.decode_batches_proved_log(&log).await {
                        Ok(Some((proved, l1_block_number, tx_hash))) => {
                            if tx.send((proved, l1_block_number, tx_hash)).is_err() {
                                error!(
                                    "BatchesProved receiver dropped. Stopping Proved event task."
                                );
                                return;
                            }
                        }
                        Ok(None) => {}
                        Err(err) => {
                            warn!(error = %err, "Failed to decode Proved log");
                        }
                    }
                }
                warn!("Proved log stream ended. Attempting to resubscribe...");
            }
        });

        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }

    /// Subscribes to the `ForcedInclusionProcessed` event and returns a stream of
    /// decoded events. This stream will attempt to automatically resubscribe and continue
    /// yielding events.
    pub async fn get_forced_inclusion_stream(&self) -> Result<ForcedInclusionStream> {
        let (tx, rx) = mpsc::unbounded_channel();
        let provider = self.l1_provider.clone();
        let taiko_inbox = self.taiko_inbox.clone();

        tokio::spawn(async move {
            loop {
                info!(
                    "Attempting to subscribe to TaikoInbox Proposed events for forced inclusions..."
                );
                let filter = taiko_inbox.batch_proposed_filter();
                let sub_result = provider.subscribe_logs(&filter).await;

                let mut log_stream = match sub_result {
                    Ok(sub) => {
                        info!("Successfully subscribed to TaikoInbox Proposed events.");
                        sub.into_stream()
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to subscribe to Proposed logs for forced inclusions, retrying in 5s");
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                while let Some(log) = log_stream.next().await {
                    if log.removed {
                        info!("Skipping removed Proposed log due to L1 reorg");
                        continue;
                    }
                    match log.log_decode::<InboxBatchProposed>() {
                        Ok(decoded) => {
                            let events = forced_inclusions_from_proposed(decoded.data());
                            for event in events {
                                if tx.send(event).is_err() {
                                    error!(
                                        "ForcedInclusionProcessed receiver dropped. Stopping forced inclusion task."
                                    );
                                    return;
                                }
                            }
                        }
                        Err(err) => {
                            warn!(error = %err, "Failed to decode Proposed log for forced inclusions");
                        }
                    }
                }
                warn!("Forced inclusion Proposed log stream ended. Attempting to resubscribe...");
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

    /// Decode a Shasta `Proposed` log into Taikoscope's internal batch model.
    pub async fn decode_batch_proposed_log(
        &self,
        log: &alloy_rpc_types_eth::Log,
    ) -> Result<Option<DecodedBatchProposed>> {
        let decoded = match log.log_decode::<InboxBatchProposed>() {
            Ok(decoded) => decoded,
            Err(_) => return Ok(None),
        };

        let proposed = decoded.data();
        let tx_hash = log.transaction_hash.unwrap_or_default();
        let l1_block_number = log
            .block_number
            .ok_or_else(|| eyre::eyre!("missing L1 block number for Proposed log"))?;
        let forced_inclusions = forced_inclusions_from_proposed(proposed);
        let batch = self.build_batch_from_proposed(proposed, l1_block_number).await?;

        Ok(Some(DecodedBatchProposed { batch, tx_hash, forced_inclusions }))
    }

    /// Decode a Shasta `Proved` log plus prove calldata into Taikoscope's internal proved model.
    pub async fn decode_batches_proved_log(
        &self,
        log: &alloy_rpc_types_eth::Log,
    ) -> Result<Option<(chainio::BatchesProved, u64, B256)>> {
        let decoded = match log.log_decode::<InboxBatchesProved>() {
            Ok(decoded) => decoded,
            Err(_) => return Ok(None),
        };

        let tx_hash = log.transaction_hash.unwrap_or_default();
        let tx = self.get_transaction_by_hash_with_retry(tx_hash).await?;
        let proved = decode_batches_proved(decoded.data(), tx.input())?;

        Ok(Some((proved, log.block_number.unwrap_or_default(), tx_hash)))
    }

    async fn build_batch_from_proposed(
        &self,
        proposed: &InboxBatchProposed,
        l1_block_number: u64,
    ) -> Result<chainio::BatchProposed> {
        let batch_id = proposed.id.to::<u64>();
        let last_l2_block_number = self.get_last_block_id_by_batch_id_with_retry(batch_id).await?;
        let batch_size = if batch_id == 0 {
            last_l2_block_number.saturating_add(1)
        } else {
            let previous_last =
                self.get_last_block_id_by_batch_id_with_retry(batch_id.saturating_sub(1)).await?;
            last_l2_block_number.saturating_sub(previous_last)
        };
        let batch_size = u16::try_from(batch_size)
            .wrap_err_with(|| format!("proposal {} exceeds supported batch size", batch_id))?;
        let blob_hashes = flatten_blob_hashes(&proposed.sources);
        let blob_byte_size = approximate_blob_bytes(blob_hashes.len());
        let last_block_timestamp = self.get_l1_timestamp_cached(l1_block_number).await?;

        Ok(chainio::BatchProposed {
            info: chainio::BatchInfo {
                blocks: vec![chainio::BlockParams; usize::from(batch_size)],
                blobHashes: blob_hashes,
                proposedIn: l1_block_number,
                blobByteSize: blob_byte_size,
                lastBlockId: last_l2_block_number,
                lastBlockTimestamp: last_block_timestamp,
            },
            meta: chainio::BatchMetadata { proposer: proposed.proposer, batchId: batch_id },
        })
    }

    async fn get_last_block_id_by_batch_id_with_retry(&self, batch_id: u64) -> Result<u64> {
        // Observed on mainnet Shasta (2026-04-15): the L2 node sometimes needs
        // longer than 10s (= 20 * 500ms) to populate the
        // `taikoAuth_lastBlockIDByBatchID` mapping for a fresh proposal,
        // because it waits for the L2 blocks derived from the proposal's
        // blob sources to be seen and executed. When the mapping isn't ready
        // in time, the retry loop gives up and the proposal is dropped,
        // which widens the gap between ingested batches and trips false
        // "No BatchProposed events" alerts.
        //
        // Give the node up to 3 minutes (360 attempts * 500ms). The loop
        // short-circuits immediately on Ok(Some), so this only increases the
        // latency for proposals that actually need to wait.
        const MAX_RETRIES: u32 = 360;
        const DELAY_MS: u64 = 500;

        for attempt in 0..MAX_RETRIES {
            match self.get_last_block_id_by_batch_id(batch_id).await {
                Ok(Some(block_id)) => return Ok(block_id),
                Ok(None) if attempt < MAX_RETRIES - 1 => {
                    debug!(
                        batch_id,
                        attempt = attempt + 1,
                        max_retries = MAX_RETRIES,
                        "lastBlockIDByBatchID not yet available, retrying"
                    );
                    sleep(Duration::from_millis(DELAY_MS)).await;
                }
                Ok(None) => break,
                Err(err) if attempt < MAX_RETRIES - 1 => {
                    warn!(
                        error = %err,
                        batch_id,
                        attempt = attempt + 1,
                        max_retries = MAX_RETRIES,
                        "failed to resolve lastBlockIDByBatchID, retrying"
                    );
                    sleep(Duration::from_millis(DELAY_MS)).await;
                }
                Err(err) => return Err(err),
            }
        }

        Err(eyre::eyre!("missing taikoAuth_lastBlockIDByBatchID mapping for proposal {}", batch_id))
    }

    /// Fetch the last L2 block ID associated with a proposal.
    ///
    /// Prefer Taiko's authenticated RPC method when available, but fall back to the
    /// on-chain inbox contract so standard RPC endpoints keep working.
    pub async fn get_last_block_id_by_batch_id(&self, batch_id: u64) -> Result<Option<u64>> {
        match self.get_last_block_id_by_batch_id_via_rpc(batch_id).await {
            Ok(Some(block_id)) => Ok(Some(block_id)),
            Ok(None) | Err(_) => self.get_last_block_id_by_batch_id_via_contract(batch_id).await,
        }
    }

    async fn get_last_block_id_by_batch_id_via_rpc(&self, batch_id: u64) -> Result<Option<u64>> {
        // The `taikoAuth_*` namespace only exists on Taiko geth's JWT-protected
        // engine endpoint. Prefer the dedicated auth provider when configured;
        // otherwise fall back to the main L2 provider for backwards compat with
        // Pacaya-era networks where the method was exposed unauthenticated.
        let provider = self.l2_auth_provider.as_ref().unwrap_or(&self.l2_provider);
        let result: Option<U256> = provider
            .raw_request(Cow::Borrowed("taikoAuth_lastBlockIDByBatchID"), (U256::from(batch_id),))
            .await?;
        Ok(result.map(|value| value.to::<u64>()))
    }

    async fn get_last_block_id_by_batch_id_via_contract(
        &self,
        batch_id: u64,
    ) -> Result<Option<u64>> {
        match self.taiko_inbox.getBatch(batch_id).call().await {
            Ok(batch) => Ok(Some(batch.lastBlockId)),
            Err(err) => {
                let message = err.to_string();
                if message.contains("BatchNotFound") || message.contains("execution reverted") {
                    Ok(None)
                } else {
                    Err(err.into())
                }
            }
        }
    }

    async fn get_l1_timestamp_cached(&self, block_number: u64) -> Result<u64> {
        if let Some(timestamp) = self.l1_block_cache.get(&block_number) {
            return Ok(*timestamp);
        }

        let block = self.get_l1_block_by_number(block_number).await?;
        let timestamp = block.header.timestamp;
        insert_l1_block_timestamp(&self.l1_block_cache, block_number, timestamp);
        Ok(timestamp)
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

        for attempt in 0..MAX_RETRIES {
            match self.l1_provider.get_transaction_receipt(tx_hash).await {
                Ok(Some(receipt)) => return Ok(receipt),
                Ok(None) => {
                    // Receipt not yet available, retry after delay
                    if attempt < MAX_RETRIES - 1 {
                        let delay_ms = retry_delay_ms(attempt);

                        debug!(
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

    async fn get_transaction_by_hash_with_retry(
        &self,
        tx_hash: alloy::primitives::B256,
    ) -> Result<alloy_rpc_types_eth::Transaction> {
        const MAX_RETRIES: u32 = 10;

        for attempt in 0..MAX_RETRIES {
            match self.l1_provider.get_transaction_by_hash(tx_hash).await {
                Ok(Some(tx)) => return Ok(tx),
                Ok(None) if attempt < MAX_RETRIES - 1 => {
                    let delay_ms = retry_delay_ms(attempt);
                    debug!(
                        attempt = attempt + 1,
                        max_retries = MAX_RETRIES,
                        delay_ms,
                        tx_hash = %tx_hash,
                        "Transaction not found yet, retrying..."
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
                Ok(None) => break,
                Err(err) if attempt < MAX_RETRIES - 1 => {
                    let delay_ms = retry_delay_ms(attempt);
                    warn!(
                        error = %err,
                        attempt = attempt + 1,
                        max_retries = MAX_RETRIES,
                        delay_ms,
                        tx_hash = %tx_hash,
                        "Failed to fetch transaction by hash, retrying..."
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
                Err(err) => return Err(err.into()),
            }
        }

        Err(eyre::eyre!("Transaction not found for transaction hash: {}", tx_hash))
    }
}

/// Build a JWT-signing HTTP provider against Taiko geth's engine-auth endpoint.
///
/// Uses [`alloy_transport_http::AuthLayer`] via a `hyper_util` legacy client,
/// following the pattern documented in `alloy_provider`'s `test_auth_layer_transport`
/// test. The resulting provider is a drop-in replacement for any other
/// [`DefaultProvider`] — it signs a fresh JWT bearer per request and sends it in
/// the `Authorization` header.
///
/// `secret_hex` may be a 64-character hex string with or without a `0x` prefix.
fn build_l2_auth_provider(url: Url, secret_hex: &str) -> Result<DefaultProvider> {
    // Tolerate whitespace from file reads and an optional 0x prefix.
    let trimmed = secret_hex.trim();
    let trimmed =
        trimmed.strip_prefix("0x").or_else(|| trimmed.strip_prefix("0X")).unwrap_or(trimmed);
    let secret = JwtSecret::from_hex(trimmed)
        .wrap_err("invalid L2 JWT secret: expected 32-byte hex string")?;

    let hyper_client =
        HyperLegacyClient::builder(TokioExecutor::new()).build_http::<Full<HyperBytes>>();
    let service = ServiceBuilder::new().layer(AuthLayer::new(secret)).service(hyper_client);
    let hyper_transport = HyperClient::with_service(service);
    let http = Http::with_client(hyper_transport, url);
    // The engine-auth endpoint is effectively local from the extractor's
    // perspective (single-hop to a Taiko geth we control), so `is_local=true`
    // is fine — it only affects batching heuristics in alloy.
    let rpc_client = RpcClient::new(http, true);
    Ok(ProviderBuilder::new().connect_client(rpc_client))
}

fn retry_delay_ms(attempt: u32) -> u64 {
    const BASE_DELAY_MS: u64 = 500;
    BASE_DELAY_MS * (1u64 << attempt).min(8) + ((attempt as u64) * 50).min(100)
}

fn flatten_blob_hashes(sources: &[chainio::IInbox::DerivationSource]) -> Vec<B256> {
    sources.iter().flat_map(|source| source.blobSlice.blobHashes.iter().copied()).collect()
}

fn approximate_blob_bytes(blob_count: usize) -> u32 {
    const BLOB_BYTES: u64 = 4096 * 32;
    let blob_bytes = (blob_count as u64).saturating_mul(BLOB_BYTES);
    blob_bytes.min(u64::from(u32::MAX)) as u32
}

fn insert_l1_block_timestamp(cache: &DashMap<u64, u64>, block_number: u64, timestamp: u64) {
    cache.insert(block_number, timestamp);
    let oldest_kept = block_number.saturating_sub(L1_BLOCK_CACHE_DEPTH);
    cache.retain(|number, _| *number >= oldest_kept);
}

fn forced_inclusions_from_proposed(
    proposed: &InboxBatchProposed,
) -> Vec<chainio::ForcedInclusionProcessed> {
    proposed
        .sources
        .iter()
        .filter(|source| source.isForcedInclusion)
        .flat_map(|source| source.blobSlice.blobHashes.iter().copied())
        .map(|blob_hash| chainio::ForcedInclusionProcessed {
            forcedInclusion: chainio::ForcedInclusion { blobHash: blob_hash },
        })
        .collect()
}

fn decode_batches_proved(
    proved: &InboxBatchesProved,
    calldata: &[u8],
) -> Result<chainio::BatchesProved> {
    let call = chainio::IInbox::proveCall::abi_decode(calldata)
        .map_err(|err| eyre::eyre!("decode prove calldata failed: {}", err))?;
    let commitment = decode_shasta_commitment_packed(&call._data)
        .map_err(|err| eyre::eyre!("decode prove commitment failed: {}", err))?;
    let first_proposal_id = proved.firstProposalId.to::<u64>();
    let first_new_proposal_id = proved.firstNewProposalId.to::<u64>();
    let last_proposal_id = proved.lastProposalId.to::<u64>();
    let first_new_index = first_new_proposal_id.saturating_sub(first_proposal_id) as usize;
    let batch_ids: Vec<u64> = (first_new_proposal_id..=last_proposal_id).collect();
    let total = batch_ids.len();

    let transitions = batch_ids
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let transition = commitment.transitions.get(first_new_index + i);
            let parent_hash = if i == 0 && first_new_index == 0 {
                commitment.first_proposal_parent_block_hash
            } else {
                B256::ZERO
            };
            let state_root = if i + 1 == total { commitment.end_state_root } else { B256::ZERO };

            chainio::Transition {
                parentHash: parent_hash,
                blockHash: transition.map(|transition| transition.block_hash).unwrap_or(B256::ZERO),
                stateRoot: state_root,
            }
        })
        .collect();

    Ok(chainio::BatchesProved { verifier: proved.actualProver, batchIds: batch_ids, transitions })
}

/// In-memory representation of a Shasta [`Commitment`] decoded from the packed
/// binary `_data` blob passed to `IInbox.prove(bytes _data, bytes _proof)`.
///
/// Shasta does NOT ABI-encode `_data` — it is a tight, manually packed layout
/// (confirmed against mainnet prove tx `0x70a609d0…` in L1 block 24882287):
///
/// ```text
/// offset  field                           size
/// ------  -----                           ----
///   0     firstProposalId                 uint48 (6 bytes, big-endian)
///   6     firstProposalParentBlockHash    bytes32
///  38     lastProposalHash                bytes32
///  70     actualProver                    address (20 bytes)
///  90     endBlockNumber                  uint48 (6 bytes, big-endian)
///  96     endStateRoot                    bytes32
/// 128     transitionCount                 uint16 (2 bytes, big-endian)
/// 130     transitions[]                   58 bytes each
///                                         { address proposer (20)
///                                         , uint48 timestamp (6)
///                                         , bytes32 blockHash (32) }
/// ```
///
/// Total size = `130 + transitionCount * 58`. Trying to `abi_decode` this as
/// a Solidity struct fails with `type check failed for "offset (usize)"`
/// because the first word is not an ABI offset — it's the packed
/// `firstProposalId` followed by a raw hash. This is the root cause of the
/// "No prove / verify data since 2026-04-02" freeze on the health dashboard.
#[derive(Debug, Default, Clone)]
struct ShastaCommitment {
    first_proposal_parent_block_hash: B256,
    end_state_root: B256,
    transitions: Vec<ShastaTransition>,
}

#[derive(Debug, Default, Clone)]
struct ShastaTransition {
    block_hash: B256,
}

const SHASTA_COMMITMENT_HEADER_LEN: usize = 6 + 32 + 32 + 20 + 6 + 32 + 2;
const SHASTA_TRANSITION_LEN: usize = 20 + 6 + 32;

fn decode_shasta_commitment_packed(data: &[u8]) -> Result<ShastaCommitment> {
    if data.len() < SHASTA_COMMITMENT_HEADER_LEN {
        return Err(eyre::eyre!(
            "commitment payload too short: got {} bytes, need at least {}",
            data.len(),
            SHASTA_COMMITMENT_HEADER_LEN
        ));
    }

    // Header: [firstProposalId(6) | firstProposalParentBlockHash(32)
    //        | lastProposalHash(32) | actualProver(20) | endBlockNumber(6)
    //        | endStateRoot(32) | transitionCount(2)]
    //
    // We only need firstProposalParentBlockHash, endStateRoot, and the
    // transitions' blockHashes downstream, but we still validate the full
    // header length so malformed payloads error cleanly instead of slicing
    // into transition data.
    let first_proposal_parent_block_hash = B256::from_slice(&data[6..38]);
    let end_state_root = B256::from_slice(&data[96..128]);
    let transition_count = u16::from_be_bytes([data[128], data[129]]) as usize;

    let expected_body_len = transition_count * SHASTA_TRANSITION_LEN;
    let actual_body_len = data.len() - SHASTA_COMMITMENT_HEADER_LEN;
    if actual_body_len != expected_body_len {
        return Err(eyre::eyre!(
            "commitment transitions length mismatch: header declared {} transitions \
             ({} bytes), but payload has {} trailing bytes",
            transition_count,
            expected_body_len,
            actual_body_len
        ));
    }

    let mut transitions = Vec::with_capacity(transition_count);
    let mut cursor = SHASTA_COMMITMENT_HEADER_LEN;
    for _ in 0..transition_count {
        // Transition body = [proposer(20) | timestamp(6) | blockHash(32)].
        // Only blockHash is consumed downstream; proposer and timestamp are
        // parsed implicitly by advancing the cursor.
        let block_hash_start = cursor + 20 + 6;
        let block_hash_end = block_hash_start + 32;
        transitions.push(ShastaTransition {
            block_hash: B256::from_slice(&data[block_hash_start..block_hash_end]),
        });
        cursor = block_hash_end;
    }

    Ok(ShastaCommitment { first_proposal_parent_block_hash, end_state_root, transitions })
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
    use alloy::{hex, primitives::B256};

    /// Regression test for the shasta `Proved` decode freeze observed on mainnet
    /// 2026-04-15: the packed `_data` blob passed to `IInbox.prove(bytes,bytes)`
    /// is NOT ABI-encoded and must be parsed by hand. Payload captured from
    /// mainnet L1 block 24882287, tx 0x70a609d0...09f6.
    #[test]
    fn decodes_mainnet_shasta_commitment_payload() {
        // 420-byte `_data` blob: 130-byte header + 5 * 58-byte transitions.
        let hex_data = concat!(
            // firstProposalId (uint48 = 2856)
            "000000000b28",
            // firstProposalParentBlockHash (bytes32)
            "5bf3688d86e60634c8b510d30d8ad854a8d30d6c1bb3882b9670d0715e381748",
            // lastProposalHash (bytes32)
            "c1f858c956088438b7b9c86b04eccd6830c0c85ef2fee0b25f35a275d6900b98",
            // actualProver (address)
            "a5cb34b75bd72f15290ef37a01f06183e8036875",
            // endBlockNumber (uint48 = 5534760)
            "000000547428",
            // endStateRoot (bytes32)
            "13188b0792af6271e7b3ae61eaede8acb18f7111e91188c746d4513046b96f1d",
            // transitionCount (uint16 = 5)
            "0005",
            // transition[0]: proposer(20) + timestamp(6) + blockHash(32)
            "cbeb5d484b54498d3893a0c3eb790331962e9e9d",
            "000069df2503",
            "71eab342b226d8b772b46c50536e954ca7555e6cc748ff517d1d305722c4d157",
            // transition[1]
            "cbeb5d484b54498d3893a0c3eb790331962e9e9d",
            "000069df2683",
            "da3d513c7aa1a18f21ca86299b393f6e5aeb876e8f5e118dfaa76ea56f12da31",
            // transition[2]
            "cbeb5d484b54498d3893a0c3eb790331962e9e9d",
            "000069df2803",
            "3c1badb865ab6431221396e4b942a976c4a3e5fd4440b173ca36eb808bd973f5",
            // transition[3]
            "cbeb5d484b54498d3893a0c3eb790331962e9e9d",
            "000069df2983",
            "41b2bacbf5a61d1bc8dbd4fd1d60e0e5531c4534541d710f55829d9469250313",
            // transition[4]
            "cbeb5d484b54498d3893a0c3eb790331962e9e9d",
            "000069df2b03",
            "a84dfd955cb3380518eb6bc3bf51d89b8bc7108987b73e6acf438b068e73781d",
        );
        let data = hex::decode(hex_data).expect("hex payload is well-formed");
        assert_eq!(data.len(), 420);

        let c = decode_shasta_commitment_packed(&data).expect("decode succeeds");
        assert_eq!(
            format!("{:?}", c.first_proposal_parent_block_hash),
            "0x5bf3688d86e60634c8b510d30d8ad854a8d30d6c1bb3882b9670d0715e381748"
        );
        assert_eq!(
            format!("{:?}", c.end_state_root),
            "0x13188b0792af6271e7b3ae61eaede8acb18f7111e91188c746d4513046b96f1d"
        );
        assert_eq!(c.transitions.len(), 5);
        assert_eq!(
            format!("{:?}", c.transitions[0].block_hash),
            "0x71eab342b226d8b772b46c50536e954ca7555e6cc748ff517d1d305722c4d157"
        );
        assert_eq!(
            format!("{:?}", c.transitions[4].block_hash),
            "0xa84dfd955cb3380518eb6bc3bf51d89b8bc7108987b73e6acf438b068e73781d"
        );
    }

    #[test]
    fn decodes_zero_transition_shasta_commitment() {
        // Minimum valid commitment: header only, zero transitions.
        let mut data = vec![0u8; SHASTA_COMMITMENT_HEADER_LEN];
        // transitionCount = 0 is already the default.
        let c = decode_shasta_commitment_packed(&data).expect("decode succeeds");
        assert_eq!(c.transitions.len(), 0);
        // And one-byte-short payloads should error cleanly instead of panicking.
        data.pop();
        let err = decode_shasta_commitment_packed(&data).unwrap_err();
        assert!(err.to_string().contains("too short"), "got: {err}");
    }

    #[test]
    fn rejects_shasta_commitment_with_trailing_garbage() {
        let mut data = vec![0u8; SHASTA_COMMITMENT_HEADER_LEN];
        // Declare 1 transition but leave zero body bytes.
        data[128] = 0;
        data[129] = 1;
        let err = decode_shasta_commitment_packed(&data).unwrap_err();
        assert!(err.to_string().contains("length mismatch"), "got: {err}");
    }

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
    fn l1_block_timestamp_cache_eviction_keeps_recent_blocks() {
        let cache = DashMap::new();
        insert_l1_block_timestamp(&cache, 1, 10);
        insert_l1_block_timestamp(&cache, L1_BLOCK_CACHE_DEPTH + 2, 20);

        assert!(!cache.contains_key(&1));
        assert_eq!(cache.get(&(L1_BLOCK_CACHE_DEPTH + 2)).map(|ts| *ts), Some(20));
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
