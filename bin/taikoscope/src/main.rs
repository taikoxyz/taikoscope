#![allow(missing_docs)]

use std::time::Duration;

use clap::Parser;
use config::Opts;
use dotenvy::dotenv;
use driver::driver::Driver;
use runtime::shutdown::{ShutdownSignal, run_until_shutdown_graceful};
use tokio::sync::broadcast;
use tracing::info;
use tracing_subscriber::filter::EnvFilter;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    if let Ok(custom_env_file) = std::env::var("ENV_FILE") {
        dotenvy::from_filename(custom_env_file)?;
    } else {
        dotenv().ok();
    }

    let opts = Opts::parse();
    let repair_lookback_batches = opts.repair_batch_data_lookback_batches;
    let use_auto_repair = opts.repair_batch_data_start_l1_block.is_none() &&
        opts.repair_batch_data_end_l1_block.is_none();
    let repair_range = opts.repair_batch_data_range().map_err(|message| eyre::eyre!(message))?;

    let mut opts = opts;
    if repair_range.is_some() {
        opts.instatus.monitors_enabled = false;
    }

    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    info!("Starting Taikoscope");

    let driver = Driver::new(opts).await?;

    if let Some((start_block, end_block)) = repair_range {
        info!(
            start_block = start_block,
            end_block = end_block,
            "Running one-shot BatchProposed data repair"
        );
        driver.repair_batch_proposed_data_range(start_block, end_block).await?;
        info!("BatchProposed data repair completed");
        return Ok(());
    }

    if use_auto_repair {
        driver.auto_repair_batch_proposed_data(repair_lookback_batches).await?;
    }

    // Create broadcast channel for graceful shutdown communication
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let shutdown_signal = ShutdownSignal::new();
    let shutdown_timeout = Duration::from_secs(10);

    let on_shutdown = move || {
        info!("Driver shutting down gracefully...");
        // Send shutdown signal to processor
        let _ = shutdown_tx.send(());
    };

    run_until_shutdown_graceful(
        async move { driver.start_with_shutdown(Some(shutdown_rx)).await },
        shutdown_signal,
        shutdown_timeout,
        on_shutdown,
    )
    .await
}
