//! `ChainIO` is a library for interacting with on-chain contracts.
#![allow(clippy::cognitive_complexity)]
#![allow(clippy::uninlined_format_args)]
pub mod taiko;

use IInbox::IInboxInstance;

use alloy::{
    primitives::{Address, B256},
    providers::{RootProvider, fillers::FillProvider, utils::JoinedRecommendedFillers},
    rpc::types::Filter,
    sol,
};
use derive_more::derive::Deref;
use serde::{Deserialize, Serialize};

/// Alias to the default provider with all recommended fillers (read-only).
pub type DefaultProvider = FillProvider<JoinedRecommendedFillers, RootProvider>;

/// A wrapper over the Shasta `IInbox` contract that exposes various utility methods.
#[derive(Debug, Clone, Deref)]
pub struct TaikoInbox(IInboxInstance<DefaultProvider>);

impl TaikoInbox {
    /// Create a new `TaikoInbox` instance at the given contract address.
    pub const fn new_readonly(address: Address, provider: DefaultProvider) -> Self {
        Self(IInboxInstance::new(address, provider))
    }

    /// Returns a log [`Filter`] based on the `Proposed` event.
    pub fn batch_proposed_filter(&self) -> Filter {
        self.0.Proposed_filter().filter
    }

    /// Returns a log [`Filter`] based on the `Proved` event.
    pub fn batches_proved_filter(&self) -> Filter {
        self.0.Proved_filter().filter
    }
}

sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    interface IInbox {
        #[derive(Default)]
        struct BlobSlice {
            bytes32[] blobHashes;
            uint24 offset;
            uint48 timestamp;
        }

        #[derive(Default)]
        struct DerivationSource {
            bool isForcedInclusion;
            BlobSlice blobSlice;
        }

        #[derive(Default)]
        struct Proposal {
            uint48 id;
            uint48 timestamp;
            uint48 endOfSubmissionWindowTimestamp;
            address proposer;
            bytes32 parentProposalHash;
            uint48 originBlockNumber;
            bytes32 originBlockHash;
            uint8 basefeeSharingPctg;
            DerivationSource[] sources;
        }

        #[derive(Default)]
        struct Transition {
            address proposer;
            uint48 timestamp;
            bytes32 blockHash;
        }

        #[derive(Default)]
        struct Commitment {
            uint48 firstProposalId;
            bytes32 firstProposalParentBlockHash;
            bytes32 lastProposalHash;
            address actualProver;
            uint48 endBlockNumber;
            bytes32 endStateRoot;
            Transition[] transitions;
        }

        #[derive(Default)]
        struct ProveInput {
            Commitment commitment;
        }

        #[derive(Default)]
        event Proposed(
            uint48 indexed id,
            address indexed proposer,
            bytes32 parentProposalHash,
            uint48 endOfSubmissionWindowTimestamp,
            uint8 basefeeSharingPctg,
            DerivationSource[] sources
        );

        #[derive(Default)]
        event Proved(
            uint48 firstProposalId,
            uint48 firstNewProposalId,
            uint48 lastProposalId,
            address indexed actualProver
        );

        function prove(bytes calldata _data, bytes calldata _proof) external;
    }
}

/// Placeholder block params used to preserve the existing internal batch-shaped model.
#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct BlockParams;

/// Internal batch info used by Taikoscope after translating Shasta proposals.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[allow(non_snake_case)]
pub struct BatchInfo {
    /// Placeholder blocks for the proposal. Only the length matters.
    pub blocks: Vec<BlockParams>,
    /// Flattened blob hashes across all proposal sources.
    pub blobHashes: Vec<B256>,
    /// L1 block number where the proposal was accepted.
    pub proposedIn: u64,
    /// Estimated total bytes referenced by the proposal's blobs.
    pub blobByteSize: u32,
    /// Last L2 block number contained in the proposal.
    pub lastBlockId: u64,
    /// Currently not derivable from the Shasta event alone.
    pub lastBlockTimestamp: u64,
}

/// Internal batch metadata used by Taikoscope after translating Shasta proposals.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[allow(non_snake_case)]
pub struct BatchMetadata {
    /// Proposer address.
    pub proposer: Address,
    /// Proposal ID, kept under the legacy `batchId` field name to minimize churn.
    pub batchId: u64,
}

/// Shasta proposal translated into Taikoscope's existing batch-shaped model.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BatchProposed {
    /// Proposal info.
    pub info: BatchInfo,
    /// Proposal metadata.
    pub meta: BatchMetadata,
}

impl BatchProposed {
    /// Returns the block numbers that were proposed in this batch.
    pub fn block_numbers_proposed(&self) -> Vec<u64> {
        let last = self.info.lastBlockId;
        let count = self.info.blocks.len() as u64;

        if count == 0 {
            return Vec::new();
        }

        if last == 0 {
            return vec![0];
        }

        let first = last.saturating_sub(count) + 1;
        (first..=last).collect()
    }

    /// Returns the last block number proposed in this batch.
    pub const fn last_block_number(&self) -> u64 {
        self.info.lastBlockId
    }

    /// Returns the last block timestamp proposed in this batch.
    pub const fn last_block_timestamp(&self) -> u64 {
        self.info.lastBlockTimestamp
    }
}

/// Internal transition model used by Taikoscope after translating Shasta proofs.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[allow(non_snake_case)]
pub struct Transition {
    /// Parent block hash when available.
    pub parentHash: B256,
    /// Proposed or finalized block hash.
    pub blockHash: B256,
    /// End state root when available.
    pub stateRoot: B256,
}

/// Shasta proof translated into Taikoscope's existing proved-batch model.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[allow(non_snake_case)]
pub struct BatchesProved {
    /// Prover address.
    pub verifier: Address,
    /// Newly proven proposal IDs, kept under the legacy `batchIds` name.
    pub batchIds: Vec<u64>,
    /// Per-proposal transition data.
    pub transitions: Vec<Transition>,
}

impl BatchesProved {
    /// Returns the batch IDs proved in this event.
    pub fn batch_ids_proved(&self) -> &[u64] {
        &self.batchIds
    }

    /// Returns the transitions proved in this event.
    pub fn transitions_proved(&self) -> &[Transition] {
        &self.transitions
    }
}

/// Forced inclusion payload preserved under the old wrapper-shaped model.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[allow(non_snake_case)]
pub struct ForcedInclusion {
    /// Blob hash for the forced inclusion payload.
    pub blobHash: B256,
}

/// Shasta forced inclusion translated into Taikoscope's legacy event shape.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[allow(non_snake_case)]
pub struct ForcedInclusionProcessed {
    /// Forced inclusion payload.
    pub forcedInclusion: ForcedInclusion,
}
