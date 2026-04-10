//! Monitoring and incident management functionality

use std::time::Duration;

use incident::{
    BatchProofTimeoutMonitor, InstatusL1Monitor, InstatusMonitor, Monitor,
    monitor::spawn_public_rpc_monitor,
};
use tracing::{info, warn};

/// Monitoring methods for the Driver
impl crate::driver::Driver {
    /// Spawn all background monitors used by the driver.
    ///
    /// Each monitor runs in its own task and reports incidents via the
    /// [`IncidentClient`].
    pub async fn start_monitors(&self) -> Vec<tokio::task::JoinHandle<()>> {
        // Always spawn monitors. When `instatus_monitors_enabled` is false,
        // monitors run in dry-run mode (no API calls), but still log warnings
        // when an incident would have been created.

        let mut handles = Vec::new();

        if let Some(url) = &self.public_rpc_url {
            info!(url = url.as_str(), "public rpc monitor enabled");
            let incident = self.instatus_monitors_enabled.then(|| {
                (self.incident_client.clone(), self.instatus_public_api_component_id.clone())
            });
            // When disabled, incident will be None; monitor will still log.
            let handle = spawn_public_rpc_monitor(url.clone(), incident);
            handles.push(handle);
        }

        // Only spawn monitors if we have a clickhouse reader (database writes enabled)
        if let Some(reader) = &self.clickhouse_reader {
            let handle = InstatusL1Monitor::new(
                reader.clone(),
                self.incident_client.clone(),
                self.instatus_batch_submission_component_id.clone(),
                Duration::from_secs(self.instatus_l1_monitor_threshold_secs),
                Duration::from_secs(self.instatus_monitor_poll_interval_secs),
            )
            .spawn();
            handles.push(handle);

            let handle = InstatusMonitor::new(
                reader.clone(),
                self.incident_client.clone(),
                self.instatus_transaction_sequencing_component_id.clone(),
                Duration::from_secs(self.instatus_l2_monitor_threshold_secs),
                Duration::from_secs(self.instatus_monitor_poll_interval_secs),
            )
            .spawn();
            handles.push(handle);

            let handle = BatchProofTimeoutMonitor::new(
                reader.clone(),
                self.incident_client.clone(),
                self.instatus_proof_submission_component_id.clone(),
                Duration::from_secs(self.batch_proof_timeout_secs),
                Duration::from_secs(60),
            )
            .spawn();
            handles.push(handle);
        } else if self.instatus_monitors_enabled {
            warn!(
                "Instatus monitors enabled but no ClickHouse reader available (database writes disabled)"
            );
        } else {
            info!(
                "Instatus monitors disabled and no ClickHouse reader available; monitors will not run"
            );
        }

        handles
    }
}
