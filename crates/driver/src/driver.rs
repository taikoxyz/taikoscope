//! Taikoscope Driver - combines ingestor and processor

use alloy_primitives::Address;
use clickhouse::{ClickhouseReader, ClickhouseWriter};
use config::Opts;
use extractor::{
    BatchProposedStream, BatchesProvedStream, Extractor, ForcedInclusionStream, ReorgDetector,
};
use eyre::{Context, Result};
use incident::client::Client as IncidentClient;
use messages::TaikoEvent;
use primitives::headers::{L1HeaderStream, L2HeaderStream};
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tracing::{error, info, warn};
use url::Url;

use crate::{gap_detection::run_initial_gap_catchup, subscription::subscribe_with_retry};

/// Driver that combines ingestor and processor functionality
#[derive(Debug)]
#[allow(dead_code)]
#[allow(missing_docs)]
pub struct Driver {
    pub extractor: Extractor,
    pub clickhouse_writer: Option<ClickhouseWriter>,
    pub clickhouse_reader: Option<ClickhouseReader>,
    pub reorg_detector: ReorgDetector,
    pub last_l2_header: Option<(u64, Address)>,
    pub enable_db_writes: bool,
    pub enable_gap_detection: bool,
    pub gap_finalization_buffer_blocks: u64,
    pub gap_startup_lookback_blocks: u64,
    pub gap_continuous_lookback_blocks: u64,
    pub gap_poll_interval_secs: u64,
    pub gap_initial_delay_secs: u64,
    pub gap_dry_run: bool,
    pub gap_min_l1_block: u64,
    pub gap_min_l2_block: u64,
    pub incident_client: IncidentClient,
    pub instatus_batch_submission_component_id: String,
    pub instatus_proof_submission_component_id: String,
    pub instatus_transaction_sequencing_component_id: String,
    pub instatus_public_api_component_id: String,
    pub instatus_monitors_enabled: bool,
    pub instatus_monitor_poll_interval_secs: u64,
    pub instatus_l1_monitor_threshold_secs: u64,
    pub instatus_l2_monitor_threshold_secs: u64,
    pub batch_proof_timeout_secs: u64,
    pub public_rpc_url: Option<Url>,
    pub inbox_address: Address,
}

impl Driver {
    /// Create a new driver with the given configuration
    pub async fn new(opts: Opts) -> Result<Self> {
        info!("Initializing driver");

        // verify monitoring configuration before doing any heavy work
        if opts.instatus.monitors_enabled && !opts.instatus.enabled() {
            return Err(eyre::eyre!(
                "Instatus configuration missing; set the INSTATUS_* environment variables"
            ));
        }

        // Validate ClickHouse configuration when database writes are enabled
        if opts.enable_db_writes {
            if opts.clickhouse.url.as_str().is_empty() {
                return Err(eyre::eyre!(
                    "ClickHouse URL is required when database writes are enabled"
                ));
            }
            if opts.clickhouse.db.is_empty() {
                return Err(eyre::eyre!(
                    "ClickHouse database name is required when database writes are enabled"
                ));
            }
            if opts.clickhouse.username.is_empty() {
                return Err(eyre::eyre!(
                    "ClickHouse username is required when database writes are enabled"
                ));
            }
            // Note: password can be empty for some configurations, so we don't validate it
        }

        if !opts.instatus.monitors_enabled {
            info!("Instatus monitors disabled; no incidents will be reported");
        }

        let extractor = Extractor::new(
            opts.rpc.l1_url.clone(),
            opts.rpc.l2_url.clone(),
            opts.taiko_addresses.inbox_address,
            opts.taiko_addresses.preconf_whitelist_address,
            opts.taiko_addresses.anchor_address,
        )
        .await
        .wrap_err("Failed to initialize blockchain extractor. Ensure RPC URLs are WebSocket endpoints (ws:// or wss://)")?;

        // Always create a ClickhouseWriter for migrations, regardless of enable_db_writes
        let migration_writer = ClickhouseWriter::new(
            opts.clickhouse.url.clone(),
            opts.clickhouse.db.clone(),
            opts.clickhouse.username.clone(),
            opts.clickhouse.password.clone(),
        );

        // Handle dry-run mode (when database writes are disabled)
        if !opts.enable_db_writes {
            info!("🧪 DRY-RUN MODE: Database writes disabled");
            info!("   - Events will be processed and logged but not written to database");
            info!("   - Gap detection will run but not perform backfill operations");
            info!("   - All database writes will be simulated with detailed logging");
            info!("⚠️  Skipping database migrations (database writes disabled)");
        } else if opts.skip_migrations {
            info!("⚠️  Skipping database migrations");
        } else {
            info!("🚀 Running database migrations...");
            migration_writer.init_db(opts.reset_db).await?;
            info!("✅ Database migrations completed");
        }

        // Only keep the writer for event processing if database writes are enabled
        let clickhouse_writer = opts.enable_db_writes.then(|| {
            ClickhouseWriter::new(
                opts.clickhouse.url.clone(),
                opts.clickhouse.db.clone(),
                opts.clickhouse.username.clone(),
                opts.clickhouse.password.clone(),
            )
        });

        // Create ClickhouseReader for gap detection and reorg detection (always create if gap
        // detection is enabled)
        let clickhouse_reader = opts
            .enable_gap_detection
            .then(|| {
                ClickhouseReader::new(
                    opts.clickhouse.url.clone(),
                    opts.clickhouse.db.clone(),
                    opts.clickhouse.username.clone(),
                    opts.clickhouse.password.clone(),
                )
            })
            .transpose()?;

        // Initialize reorg detector
        let reorg_detector = ReorgDetector::new();

        // init incident client and component IDs if monitors are enabled
        let (
            instatus_batch_submission_component_id,
            instatus_proof_submission_component_id,
            instatus_transaction_sequencing_component_id,
            instatus_public_api_component_id,
            incident_client,
        ) = if opts.instatus.monitors_enabled {
            (
                opts.instatus.batch_submission_component_id.clone(),
                opts.instatus.proof_submission_component_id.clone(),
                opts.instatus.transaction_sequencing_component_id.clone(),
                opts.instatus.public_api_component_id.clone(),
                IncidentClient::new(opts.instatus.api_key.clone(), opts.instatus.page_id.clone()),
            )
        } else {
            (
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                IncidentClient::new(String::new(), String::new()),
            )
        };

        Ok(Self {
            extractor,
            clickhouse_writer,
            clickhouse_reader,
            reorg_detector,
            last_l2_header: None,
            enable_db_writes: opts.enable_db_writes,
            enable_gap_detection: opts.enable_gap_detection,
            gap_finalization_buffer_blocks: opts.gap_finalization_buffer_blocks,
            gap_startup_lookback_blocks: opts.gap_startup_lookback_blocks,
            gap_continuous_lookback_blocks: opts.gap_continuous_lookback_blocks,
            gap_poll_interval_secs: opts.gap_poll_interval_secs,
            gap_initial_delay_secs: opts.gap_initial_delay_secs,
            gap_dry_run: opts.gap_dry_run,
            gap_min_l1_block: opts.gap_min_l1_block,
            gap_min_l2_block: opts.gap_min_l2_block,
            incident_client,
            instatus_batch_submission_component_id,
            instatus_proof_submission_component_id,
            instatus_transaction_sequencing_component_id,
            instatus_public_api_component_id,
            instatus_monitors_enabled: opts.instatus.monitors_enabled,
            instatus_monitor_poll_interval_secs: opts.instatus.monitor_poll_interval_secs,
            instatus_l1_monitor_threshold_secs: opts.instatus.l1_monitor_threshold_secs,
            instatus_l2_monitor_threshold_secs: opts.instatus.l2_monitor_threshold_secs,
            batch_proof_timeout_secs: opts.instatus.batch_proof_timeout_secs,
            public_rpc_url: opts.rpc.public_url,
            inbox_address: opts.taiko_addresses.inbox_address,
        })
    }

    async fn get_l1_headers(&self) -> L1HeaderStream {
        subscribe_with_retry(|| self.extractor.get_l1_header_stream(), "l1 headers").await
    }

    async fn get_l2_headers(&self) -> L2HeaderStream {
        subscribe_with_retry(|| self.extractor.get_l2_header_stream(), "l2 headers").await
    }

    async fn get_batch_proposed(&self) -> BatchProposedStream {
        subscribe_with_retry(|| self.extractor.get_batch_proposed_stream(), "batch proposed").await
    }

    async fn get_forced_inclusion(&self) -> ForcedInclusionStream {
        subscribe_with_retry(|| self.extractor.get_forced_inclusion_stream(), "forced inclusion")
            .await
    }

    async fn get_batches_proved(&self) -> BatchesProvedStream {
        subscribe_with_retry(|| self.extractor.get_batches_proved_stream(), "batches proved").await
    }

    /// Start the driver event loop
    pub async fn start(self) -> Result<()> {
        self.start_with_shutdown(None).await
    }

    /// Start the driver event loop with graceful shutdown support
    pub async fn start_with_shutdown(
        mut self,
        shutdown_rx: Option<broadcast::Receiver<()>>,
    ) -> Result<()> {
        info!("Starting driver event loop");

        // Start initial gap catch-up in background with delay
        #[allow(clippy::if_then_some_else_none)]
        let initial_catchup_handle = if self.enable_gap_detection {
            let reader = self.clickhouse_reader.clone();
            let writer = self.clickhouse_writer.clone();
            let extractor = self.extractor.clone();
            let enable_db_writes = self.enable_db_writes;
            let gap_dry_run = self.gap_dry_run;
            let gap_finalization_buffer_blocks = self.gap_finalization_buffer_blocks;
            let gap_startup_lookback_blocks = self.gap_startup_lookback_blocks;
            let gap_min_l1_block = self.gap_min_l1_block;
            let gap_min_l2_block = self.gap_min_l2_block;
            let gap_initial_delay_secs = self.gap_initial_delay_secs;
            let inbox_address = self.inbox_address;

            info!(
                "Will start initial gap catch-up after {} second delay...",
                gap_initial_delay_secs
            );

            Some(tokio::spawn(async move {
                use std::time::Duration;

                // Wait before starting to let live processing catch up first
                tokio::time::sleep(Duration::from_secs(gap_initial_delay_secs)).await;

                info!("Starting initial gap catch-up after delay...");

                if let (Some(reader), writer) = (reader, writer) {
                    let result = run_initial_gap_catchup(
                        &reader,
                        writer.as_ref(),
                        &extractor,
                        inbox_address,
                        enable_db_writes && !gap_dry_run,
                        gap_finalization_buffer_blocks,
                        gap_startup_lookback_blocks,
                        gap_min_l1_block,
                        gap_min_l2_block,
                    )
                    .await;

                    match result {
                        Ok(()) => info!("Initial gap catch-up completed"),
                        Err(e) => error!(err = %e, "Initial gap catch-up failed"),
                    }
                } else {
                    warn!("Skipping initial gap catch-up - reader or writer not available");
                }
            }))
        } else {
            None
        };

        // Start monitors if enabled
        let monitor_handles =
            if self.instatus_monitors_enabled { self.start_monitors().await } else { Vec::new() };

        // Start gap detection task if enabled
        let gap_detection_handle = if self.enable_gap_detection {
            self.start_gap_detection_task().await
        } else {
            info!("Gap detection disabled via configuration");
            None
        };

        let l1_stream = self.get_l1_headers().await;
        let l2_stream = self.get_l2_headers().await;
        let batch_stream = self.get_batch_proposed().await;
        let forced_stream = self.get_forced_inclusion().await;
        let proved_stream = self.get_batches_proved().await;

        let result = self
            .event_loop(
                l1_stream,
                l2_stream,
                batch_stream,
                forced_stream,
                proved_stream,
                shutdown_rx,
            )
            .await;

        // Clean up monitors, gap detection, and initial catchup
        for handle in monitor_handles {
            handle.abort();
        }
        if let Some(handle) = gap_detection_handle {
            handle.abort();
        }
        if let Some(handle) = initial_catchup_handle {
            handle.abort();
        }

        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn event_loop(
        &mut self,
        mut l1_stream: L1HeaderStream,
        mut l2_stream: L2HeaderStream,
        mut batch_stream: BatchProposedStream,
        mut forced_stream: ForcedInclusionStream,
        mut proved_stream: BatchesProvedStream,
        mut shutdown_rx: Option<broadcast::Receiver<()>>,
    ) -> Result<()> {
        info!("Starting event loop - processing events directly to database");

        loop {
            tokio::select! {
                // Check for shutdown signal
                _ = async {
                    if let Some(ref mut shutdown_rx) = shutdown_rx {
                        shutdown_rx.recv().await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    info!("Received shutdown signal, stopping event loop");
                    break;
                }

                maybe_l1 = l1_stream.next() => {
                    match maybe_l1 {
                        Some(header) => {
                            info!(block_number = header.number, hash = %header.hash, "Processing L1 header");
                            let event = TaikoEvent::L1Header(header);
                            if let Err(e) = self.process_event(event).await {
                                error!(err = %e, "Failed to process L1Header");
                            }
                        }
                        None => {
                            warn!("L1 header stream ended; re-subscribing…");
                            l1_stream = self.get_l1_headers().await;
                        }
                    }
                }
                maybe_l2 = l2_stream.next() => {
                    match maybe_l2 {
                        Some(header) => {
                            info!(block_number = header.number, hash = %header.hash, "Processing L2 header");
                            let event = TaikoEvent::L2Header(header);
                            if let Err(e) = self.process_event(event).await {
                                error!(err = %e, "Failed to process L2Header");
                            }
                        }
                        None => {
                            warn!("L2 header stream ended; re-subscribing…");
                            l2_stream = self.get_l2_headers().await;
                        }
                    }
                }
                maybe_batch = batch_stream.next() => {
                    match maybe_batch {
                        Some((batch, l1_block_number, l1_tx_hash)) => {
                            info!(
                                l1_block_number,
                                block_number = batch.last_block_number(),
                                "Processing BatchProposed"
                            );
                            let wrapper = messages::BatchProposedWrapper::from((
                                batch,
                                l1_block_number,
                                l1_tx_hash,
                                false,
                            ));
                            let event = TaikoEvent::BatchProposed(wrapper);
                            if let Err(e) = self.process_event(event).await {
                                error!(err = %e, "Failed to process BatchProposed");
                            }
                        }
                        None => {
                            warn!("Batch proposed stream ended; re-subscribing…");
                            batch_stream = self.get_batch_proposed().await;
                        }
                    }
                }
                maybe_fi = forced_stream.next() => {
                    match maybe_fi {
                        Some(fi) => {
                            info!(blob_hash = ?fi.forcedInclusion.blobHash, "Processing forced inclusion processed");
                            let wrapper = messages::ForcedInclusionProcessedWrapper::from((fi, false));
                            let event = TaikoEvent::ForcedInclusionProcessed(wrapper);
                            if let Err(e) = self.process_event(event).await {
                                error!(err = %e, "Failed to process ForcedInclusionProcessed");
                            }
                        }
                        None => {
                            warn!("Forced inclusion stream ended; re-subscribing…");
                            forced_stream = self.get_forced_inclusion().await;
                        }
                    }
                }
                maybe_proved = proved_stream.next() => {
                    match maybe_proved {
                        Some((proved, l1_block_number, l1_tx_hash)) => {
                            info!(batch_ids = ?proved.batch_ids_proved(), "Processing batches proved");
                            let wrapper = messages::BatchesProvedWrapper::from((proved, l1_block_number, l1_tx_hash, false));
                            let event = TaikoEvent::BatchesProved(wrapper);
                            if let Err(e) = self.process_event(event).await {
                                error!(err = %e, "Failed to process BatchesProved");
                            }
                        }
                        None => {
                            warn!("Batches proved stream ended; re-subscribing…");
                            proved_stream = self.get_batches_proved().await;
                        }
                    }
                }
                else => {
                    error!("All event streams ended and failed to re-subscribe. Shutting down driver loop");
                    break;
                }
            }
        }
        Ok(())
    }
}
