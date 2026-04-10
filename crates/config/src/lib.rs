//! Taikoscope configuration
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::cognitive_complexity)]
use alloy_primitives::Address;
use clap::Parser;
use url::Url;

/// Default origins allowed to access the API.
pub const DEFAULT_ALLOWED_ORIGINS: &str = "https://taikoscope.xyz,https://www.taikoscope.xyz,https://hekla.taikoscope.xyz,https://www.hekla.taikoscope.xyz";
/// Clickhouse database configuration options
#[derive(Debug, Clone, Parser)]
pub struct ClickhouseOpts {
    /// Clickhouse URL
    #[clap(long, env = "CLICKHOUSE_URL")]
    pub url: Url,
    /// Clickhouse database
    #[clap(long, env = "CLICKHOUSE_DB")]
    pub db: String,
    /// Clickhouse username
    #[clap(long, env = "CLICKHOUSE_USERNAME")]
    pub username: String,
    /// Clickhouse password
    #[clap(long, env = "CLICKHOUSE_PASSWORD")]
    pub password: String,
}

/// RPC endpoint configuration options
#[derive(Debug, Clone, Parser)]
pub struct RpcOpts {
    /// L1 WebSocket RPC URL (must use ws:// or wss:// scheme)
    #[clap(long, env = "L1_RPC_URL")]
    pub l1_url: Url,
    /// L2 WebSocket RPC URL (must use ws:// or wss:// scheme)
    #[clap(long, env = "L2_RPC_URL")]
    pub l2_url: Url,
    /// Public RPC URL for health checks
    #[clap(long, env = "PUBLIC_RPC")]
    pub public_url: Option<Url>,
}

/// Taiko contract address configuration options
#[derive(Debug, Clone, Parser)]
pub struct TaikoAddressOpts {
    /// Taiko inbox contract address
    #[clap(long, env = "TAIKO_INBOX_ADDRESS")]
    pub inbox_address: Address,
    /// Taiko preconf whitelist contract address
    #[clap(long, env = "TAIKO_PRECONF_WHITELIST_ADDRESS")]
    pub preconf_whitelist_address: Address,
    /// Taiko anchor contract address
    #[clap(long, env = "TAIKO_ANCHOR_ADDRESS")]
    pub anchor_address: Address,
}

/// Instatus monitoring configuration options
#[derive(Debug, Clone, Parser)]
pub struct InstatusOpts {
    /// Instatus API key
    #[clap(long, env = "INSTATUS_API_KEY", default_value = "")]
    pub api_key: String,
    /// Instatus page ID
    #[clap(long, env = "INSTATUS_PAGE_ID", default_value = "")]
    pub page_id: String,
    /// Instatus component ID for batch submission monitor
    #[clap(long, env = "INSTATUS_BATCH_SUBMISSION_COMPONENT_ID", default_value = "")]
    pub batch_submission_component_id: String,
    /// Instatus component ID for proof submission timeout monitor
    #[clap(long, env = "INSTATUS_PROOF_SUBMISSION_COMPONENT_ID", default_value = "")]
    pub proof_submission_component_id: String,
    /// Instatus component ID for transaction sequencing monitor
    #[clap(long, env = "INSTATUS_TRANSACTION_SEQUENCING_COMPONENT_ID", default_value = "")]
    pub transaction_sequencing_component_id: String,
    /// Instatus component ID for the public API monitor
    #[clap(long, env = "INSTATUS_PUBLIC_API_COMPONENT_ID", default_value = "")]
    pub public_api_component_id: String,
    /// Enable all Instatus monitors
    #[clap(long = "enable-monitors", env = "INSTATUS_MONITORS_ENABLED", default_value_t = true)]
    pub monitors_enabled: bool,
    /// Instatus monitor poll interval in seconds
    #[clap(long, env = "INSTATUS_MONITOR_POLL_INTERVAL_SECS", default_value = "30")]
    pub monitor_poll_interval_secs: u64,
    /// Threshold in seconds for detecting missing `BatchProposed` events
    #[clap(long, env = "INSTATUS_L1_MONITOR_THRESHOLD_SECS", default_value = "600")]
    pub l1_monitor_threshold_secs: u64,

    /// Threshold in seconds for detecting missing L2 head events
    #[clap(long, env = "INSTATUS_L2_MONITOR_THRESHOLD_SECS", default_value = "600")]
    pub l2_monitor_threshold_secs: u64,

    /// Batch proof timeout threshold in seconds (default 3 hours)
    #[clap(long, env = "BATCH_PROOF_TIMEOUT_SECS", default_value = "10800")]
    pub batch_proof_timeout_secs: u64,
}

impl InstatusOpts {
    /// Returns `true` if all required values are set.
    #[allow(clippy::missing_const_for_fn)]
    pub fn enabled(&self) -> bool {
        if self.api_key.is_empty() ||
            self.page_id.is_empty() ||
            self.batch_submission_component_id.is_empty() ||
            self.proof_submission_component_id.is_empty() ||
            self.transaction_sequencing_component_id.is_empty() ||
            self.public_api_component_id.is_empty()
        {
            return false;
        }

        true
    }
}

/// API server configuration options
#[derive(Debug, Clone, Parser)]
pub struct ApiOpts {
    /// API server host
    #[clap(long = "api-host", env = "API_HOST", default_value = "127.0.0.1")]
    pub host: String,
    /// API server port
    #[clap(long = "api-port", env = "API_PORT", default_value = "3000")]
    pub port: u16,
    /// Allowed CORS origins (comma separated)
    #[clap(
        long = "allowed-origin",
        env = "ALLOWED_ORIGINS",
        value_delimiter = ',',
        default_value = DEFAULT_ALLOWED_ORIGINS
    )]
    pub allowed_origins: Vec<String>,

    /// Maximum number of requests allowed during the rate limiting period
    #[clap(
        long = "rate-limit-max-requests",
        env = "RATE_LIMIT_MAX_REQUESTS",
        default_value = "1000"
    )]
    pub rate_limit_max_requests: u64,

    /// Duration of the rate limiting window in seconds
    #[clap(long = "rate-limit-period-secs", env = "RATE_LIMIT_PERIOD_SECS", default_value = "60")]
    pub rate_limit_period_secs: u64,
}

/// CLI options for taikoscope
#[derive(Debug, Clone, Parser)]
pub struct Opts {
    /// Clickhouse database configuration
    #[clap(flatten)]
    pub clickhouse: ClickhouseOpts,

    /// RPC endpoint configuration
    #[clap(flatten)]
    pub rpc: RpcOpts,

    /// Taiko contract address configuration
    #[clap(flatten)]
    pub taiko_addresses: TaikoAddressOpts,

    /// Instatus monitoring configuration
    #[clap(flatten)]
    pub instatus: InstatusOpts,

    /// API server configuration
    #[clap(flatten)]
    pub api: ApiOpts,

    /// Enable database writes in processor (default: false, processor will log and drop events)
    #[clap(long, env = "ENABLE_DB_WRITES", default_value = "true")]
    pub enable_db_writes: bool,

    /// If set, drop & re-create all tables (local/dev only)
    #[clap(long)]
    pub reset_db: bool,

    /// Skip database migrations on startup (useful for development)
    #[clap(long, env = "SKIP_MIGRATIONS", default_value = "false")]
    pub skip_migrations: bool,

    /// Enable gap detection and backfill (default: true)
    #[clap(long, env = "ENABLE_GAP_DETECTION", default_value = "true")]
    pub enable_gap_detection: bool,

    /// Number of blocks to wait for finalization before backfilling (default: 12)
    #[clap(long, env = "GAP_FINALIZATION_BUFFER_BLOCKS", default_value = "12")]
    pub gap_finalization_buffer_blocks: u64,

    /// Number of blocks to look back on startup for initial catch-up (default: 128)
    #[clap(long, env = "GAP_STARTUP_LOOKBACK_BLOCKS", default_value = "128")]
    pub gap_startup_lookback_blocks: u64,

    /// Number of blocks to look back during continuous gap detection (default: 32)
    #[clap(long, env = "GAP_CONTINUOUS_LOOKBACK_BLOCKS", default_value = "32")]
    pub gap_continuous_lookback_blocks: u64,

    /// Gap detection poll interval in seconds (default: 30)
    #[clap(long, env = "GAP_POLL_INTERVAL_SECS", default_value = "30")]
    pub gap_poll_interval_secs: u64,

    /// Initial delay before starting gap detection in seconds (default: 30)
    #[clap(long, env = "GAP_INITIAL_DELAY_SECS", default_value = "30")]
    pub gap_initial_delay_secs: u64,

    /// Enable gap detection dry-run mode (default: false)
    #[clap(long, env = "GAP_DRY_RUN", default_value = "false")]
    pub gap_dry_run: bool,

    /// Minimum L1 block number to backfill
    #[clap(long, env = "GAP_MIN_L1_BLOCK")]
    pub gap_min_l1_block: u64,

    /// Minimum L2 block number to backfill
    #[clap(long, env = "GAP_MIN_L2_BLOCK")]
    pub gap_min_l2_block: u64,
}

#[cfg(test)]
mod tests {
    //! Tests that modify environment variables need to be run with --test-threads=1
    //! to avoid interference between parallel test execution.
    use super::Opts;
    use clap::Parser;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_verify_cli() {
        use clap::CommandFactory;
        Opts::command().debug_assert()
    }

    fn base_args() -> Vec<&'static str> {
        vec![
            "prog",
            "--url",
            "http://localhost:8123",
            "--db",
            "test-db",
            "--username",
            "user",
            "--password",
            "pass",
            "--l1-url",
            "http://l1",
            "--l2-url",
            "http://l2",
            "--inbox-address",
            "0x0000000000000000000000000000000000000001",
            "--preconf-whitelist-address",
            "0x0000000000000000000000000000000000000002",
            "--anchor-address",
            "0x0000000000000000000000000000000000000004",
            "--api-key",
            "key",
            "--page-id",
            "page",
            "--batch-submission-component-id",
            "batch",
            "--proof-submission-component-id",
            "proof",
            "--transaction-sequencing-component-id",
            "l2",
            "--public-api-component-id",
            "api",
            "--api-host",
            "127.0.0.1",
            "--api-port",
            "3000",
            "--gap-min-l1-block",
            "1",
            "--gap-min-l2-block",
            "1",
        ]
    }

    #[test]
    #[serial]
    fn test_default_values() {
        // Clean up any environment variables that might affect this test
        use std::env;
        unsafe {
            env::remove_var("INSTATUS_MONITOR_POLL_INTERVAL_SECS");
            env::remove_var("INSTATUS_L1_MONITOR_THRESHOLD_SECS");
            env::remove_var("INSTATUS_L2_MONITOR_THRESHOLD_SECS");
            env::remove_var("BATCH_PROOF_TIMEOUT_SECS");
            env::remove_var("INSTATUS_MONITORS_ENABLED");
            env::remove_var("ALLOWED_ORIGINS");
            env::remove_var("RATE_LIMIT_MAX_REQUESTS");
            env::remove_var("RATE_LIMIT_PERIOD_SECS");
            env::remove_var("GAP_FINALIZATION_BUFFER_BLOCKS");
            env::remove_var("GAP_STARTUP_LOOKBACK_BLOCKS");
            env::remove_var("GAP_CONTINUOUS_LOOKBACK_BLOCKS");
            env::remove_var("GAP_POLL_INTERVAL_SECS");
            env::remove_var("GAP_DRY_RUN");
        }

        let args = base_args();
        let opts = Opts::try_parse_from(args).unwrap();

        assert_eq!(opts.instatus.monitor_poll_interval_secs, 30);
        assert_eq!(opts.instatus.l1_monitor_threshold_secs, 600);
        assert_eq!(opts.instatus.l2_monitor_threshold_secs, 600);
        assert_eq!(opts.instatus.batch_proof_timeout_secs, 10800);
        assert_eq!(opts.gap_finalization_buffer_blocks, 12);
        assert_eq!(opts.gap_startup_lookback_blocks, 128);
        assert_eq!(opts.gap_continuous_lookback_blocks, 32);
        assert_eq!(opts.gap_poll_interval_secs, 30);
        assert_eq!(opts.gap_initial_delay_secs, 30);
        assert!(!opts.gap_dry_run);
        assert_eq!(opts.gap_min_l1_block, 1);
        assert_eq!(opts.gap_min_l2_block, 1);
    }

    #[test]
    #[serial]
    fn test_env_overrides() {
        use std::env;

        // Clean up first to ensure clean state
        unsafe {
            env::remove_var("INSTATUS_MONITOR_POLL_INTERVAL_SECS");
            env::remove_var("INSTATUS_L1_MONITOR_THRESHOLD_SECS");
            env::remove_var("INSTATUS_L2_MONITOR_THRESHOLD_SECS");
            env::remove_var("BATCH_PROOF_TIMEOUT_SECS");
            env::remove_var("INSTATUS_MONITORS_ENABLED");
            env::remove_var("ALLOWED_ORIGINS");
            env::remove_var("RATE_LIMIT_MAX_REQUESTS");
            env::remove_var("RATE_LIMIT_PERIOD_SECS");
            env::remove_var("GAP_FINALIZATION_BUFFER_BLOCKS");
            env::remove_var("GAP_STARTUP_LOOKBACK_BLOCKS");
            env::remove_var("GAP_CONTINUOUS_LOOKBACK_BLOCKS");
            env::remove_var("GAP_POLL_INTERVAL_SECS");
            env::remove_var("GAP_DRY_RUN");
        }

        unsafe {
            env::set_var("INSTATUS_MONITOR_POLL_INTERVAL_SECS", "42");
            env::set_var("INSTATUS_L1_MONITOR_THRESHOLD_SECS", "33");
            env::set_var("INSTATUS_L2_MONITOR_THRESHOLD_SECS", "44");
            env::set_var("BATCH_PROOF_TIMEOUT_SECS", "99");
            env::set_var("INSTATUS_MONITORS_ENABLED", "false");
            env::set_var("ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:5173");
            env::set_var("RATE_LIMIT_MAX_REQUESTS", "500");
            env::set_var("RATE_LIMIT_PERIOD_SECS", "120");
            env::set_var("GAP_FINALIZATION_BUFFER_BLOCKS", "20");
            env::set_var("GAP_STARTUP_LOOKBACK_BLOCKS", "256");
            env::set_var("GAP_CONTINUOUS_LOOKBACK_BLOCKS", "64");
            env::set_var("GAP_POLL_INTERVAL_SECS", "60");
            env::set_var("GAP_DRY_RUN", "true");
        }

        let mut args = base_args();
        args.push("--reset-db");

        let opts = Opts::try_parse_from(&args).expect("failed to parse opts");

        assert_eq!(opts.instatus.monitor_poll_interval_secs, 42);
        assert_eq!(opts.instatus.l1_monitor_threshold_secs, 33);
        assert_eq!(opts.instatus.l2_monitor_threshold_secs, 44);
        assert_eq!(opts.instatus.batch_proof_timeout_secs, 99);
        assert_eq!(opts.api.host, "127.0.0.1");
        assert_eq!(opts.api.port, 3000);
        assert_eq!(
            opts.api.allowed_origins,
            vec!["http://localhost:3000", "http://localhost:5173",]
        );
        assert_eq!(opts.api.rate_limit_max_requests, 500);
        assert_eq!(opts.api.rate_limit_period_secs, 120);
        assert!(opts.reset_db);
        assert_eq!(opts.gap_finalization_buffer_blocks, 20);
        assert_eq!(opts.gap_startup_lookback_blocks, 256);
        assert_eq!(opts.gap_continuous_lookback_blocks, 64);
        assert_eq!(opts.gap_poll_interval_secs, 60);
        assert_eq!(opts.gap_initial_delay_secs, 30);
        assert!(opts.gap_dry_run);
        assert_eq!(opts.gap_min_l1_block, 1);
        assert_eq!(opts.gap_min_l2_block, 1);

        // Clean up after test
        unsafe {
            env::remove_var("INSTATUS_MONITOR_POLL_INTERVAL_SECS");
            env::remove_var("INSTATUS_L1_MONITOR_THRESHOLD_SECS");
            env::remove_var("INSTATUS_L2_MONITOR_THRESHOLD_SECS");
            env::remove_var("BATCH_PROOF_TIMEOUT_SECS");
            env::remove_var("INSTATUS_MONITORS_ENABLED");
            env::remove_var("ALLOWED_ORIGINS");
            env::remove_var("RATE_LIMIT_MAX_REQUESTS");
            env::remove_var("RATE_LIMIT_PERIOD_SECS");
            env::remove_var("GAP_FINALIZATION_BUFFER_BLOCKS");
            env::remove_var("GAP_STARTUP_LOOKBACK_BLOCKS");
            env::remove_var("GAP_CONTINUOUS_LOOKBACK_BLOCKS");
            env::remove_var("GAP_POLL_INTERVAL_SECS");
            env::remove_var("GAP_DRY_RUN");
        }
    }

    #[test]
    #[serial]
    fn test_all_origins_included() {
        use super::DEFAULT_ALLOWED_ORIGINS;

        assert!(DEFAULT_ALLOWED_ORIGINS.contains("taikoscope.xyz"));
        assert!(DEFAULT_ALLOWED_ORIGINS.contains("www.taikoscope.xyz"));
        assert!(DEFAULT_ALLOWED_ORIGINS.contains("hekla.taikoscope.xyz"));
        assert!(DEFAULT_ALLOWED_ORIGINS.contains("www.hekla.taikoscope.xyz"));

        // Verify all origins are present
        let origins: Vec<&str> = DEFAULT_ALLOWED_ORIGINS.split(',').collect();
        assert_eq!(origins.len(), 4);
        assert!(origins.contains(&"https://taikoscope.xyz"));
        assert!(origins.contains(&"https://www.taikoscope.xyz"));
        assert!(origins.contains(&"https://hekla.taikoscope.xyz"));
        assert!(origins.contains(&"https://www.hekla.taikoscope.xyz"));
    }
}
