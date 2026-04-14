#![allow(missing_docs)]
#![allow(clippy::large_enum_variant)]
use alloy_primitives::B256;
use chainio::BatchesVerified;
use primitives::headers::{L1Header, L2Header};
use serde::{Deserialize, Serialize};

// Updated wrappers to preserve L1 transaction hash and block number metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchProposedWrapper {
    pub batch: chainio::ITaikoInbox::BatchProposed,
    pub l1_block_number: u64,
    pub l1_tx_hash: B256,
    pub removed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchesProvedWrapper {
    pub proved: chainio::ITaikoInbox::BatchesProved,
    pub l1_block_number: u64,
    pub l1_tx_hash: B256,
    pub removed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchesVerifiedWrapper {
    pub verified: BatchesVerified,
    pub l1_block_number: u64,
    pub l1_tx_hash: B256,
    pub removed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForcedInclusionProcessedWrapper {
    pub event: chainio::taiko::wrapper::ITaikoWrapper::ForcedInclusionProcessed,
    pub removed: bool,
}

// Updated From implementations to preserve all metadata
impl From<(chainio::ITaikoInbox::BatchProposed, u64, B256, bool)> for BatchProposedWrapper {
    fn from(data: (chainio::ITaikoInbox::BatchProposed, u64, B256, bool)) -> Self {
        Self { batch: data.0, l1_block_number: data.1, l1_tx_hash: data.2, removed: data.3 }
    }
}

impl From<(chainio::ITaikoInbox::BatchesProved, u64, B256, bool)> for BatchesProvedWrapper {
    fn from(data: (chainio::ITaikoInbox::BatchesProved, u64, B256, bool)) -> Self {
        Self { proved: data.0, l1_block_number: data.1, l1_tx_hash: data.2, removed: data.3 }
    }
}

impl From<(BatchesVerified, u64, B256, bool)> for BatchesVerifiedWrapper {
    fn from(data: (BatchesVerified, u64, B256, bool)) -> Self {
        Self { verified: data.0, l1_block_number: data.1, l1_tx_hash: data.2, removed: data.3 }
    }
}

impl From<(chainio::taiko::wrapper::ITaikoWrapper::ForcedInclusionProcessed, bool)>
    for ForcedInclusionProcessedWrapper
{
    fn from(
        data: (chainio::taiko::wrapper::ITaikoWrapper::ForcedInclusionProcessed, bool),
    ) -> Self {
        Self { event: data.0, removed: data.1 }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TaikoEvent {
    L1Header(L1Header),
    L2Header(L2Header),
    BatchProposed(BatchProposedWrapper),
    BatchesProved(BatchesProvedWrapper),
    BatchesVerified(BatchesVerifiedWrapper),
    ForcedInclusionProcessed(ForcedInclusionProcessedWrapper),
}
