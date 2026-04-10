use serde::Serialize;

/// Incident‐level state sent to Instatus.
#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IncidentState {
    /// Incident is being investigated.
    Investigating,
    /// Incident is identified.
    Identified,
    /// Incident is being monitored.
    Monitoring,
    /// Incident is resolved.
    Resolved,
}

/// Component health inside an incident update.
#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum ComponentHealth {
    /// Component is operational.
    Operational,
    /// Component is experiencing a partial outage.
    PartialOutage,
    /// Component is experiencing a major outage.
    MajorOutage,
}

/// Status for a single component.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ComponentStatus {
    /// Component ID
    pub id: String,
    /// Status (e.g. MAJOROUTAGE, OPERATIONAL)
    pub status: ComponentHealth,
}

impl ComponentStatus {
    /// Create a new component status for a major outage.
    pub fn major_outage(id: &str) -> Self {
        Self { id: id.into(), status: ComponentHealth::MajorOutage }
    }

    /// Create a new component status for an operational component.
    pub fn operational(id: &str) -> Self {
        Self { id: id.into(), status: ComponentHealth::Operational }
    }
}

/// Payload for creating a new incident.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct NewIncident {
    /// Incident name
    pub name: String,
    /// Incident message/description
    pub message: String,
    /// Incident status (e.g. INVESTIGATING)
    pub status: IncidentState,
    /// Affected component IDs
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub components: Vec<String>,
    /// Component statuses
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub statuses: Vec<ComponentStatus>,
    /// Whether to notify subscribers
    pub notify: bool,
    /// Optional start timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started: Option<String>,
}

/// Payload for resolving an incident.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ResolveIncident {
    /// Status (should be RESOLVED)
    pub status: IncidentState,
    /// Affected component IDs
    pub components: Vec<String>,
    /// Component statuses
    pub statuses: Vec<ComponentStatus>,
    /// Whether to notify subscribers
    pub notify: bool,
    /// Time the incident started in RFC3339 format
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started: Option<String>,
}

mod batch_proof_timeout;
mod instatus;
mod instatus_l1;
mod public_rpc;

pub use batch_proof_timeout::BatchProofTimeoutMonitor;
pub use instatus::InstatusMonitor;
pub use instatus_l1::InstatusL1Monitor;
pub use public_rpc::spawn_public_rpc_monitor;

#[cfg(test)]
mod tests;
