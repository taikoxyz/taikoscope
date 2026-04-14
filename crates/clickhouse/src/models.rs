use chrono::{DateTime, Utc};
use clickhouse::Row;
use derive_more::Debug;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::types::{AddressBytes, HashBytes};

/// L1 head event
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq)]
pub struct L1HeadEvent {
    /// L1 block number
    pub l1_block_number: u64,
    /// Block hash
    pub block_hash: HashBytes,
    /// Slot
    pub slot: u64,
    /// Block timestamp
    pub block_ts: u64,
}

/// Preconf data
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq)]
pub struct PreconfData {
    /// Slot
    pub slot: u64,
    /// Candidates
    pub candidates: Vec<AddressBytes>,
    /// Current operator
    pub current_operator: Option<AddressBytes>,
    /// Next operator
    pub next_operator: Option<AddressBytes>,
}

/// L2 head event
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq)]
pub struct L2HeadEvent {
    /// L2 block number
    pub l2_block_number: u64,
    /// Block hash
    pub block_hash: HashBytes,
    /// Block timestamp
    pub block_ts: u64,
    /// Sum of gas used in the block
    pub sum_gas_used: u128,
    /// Number of transactions
    pub sum_tx: u32,
    /// Sum of priority fees paid
    pub sum_priority_fee: u128,
    /// Sum of base fees paid
    pub sum_base_fee: u128,
    /// Sequencer sequencing the block
    pub sequencer: AddressBytes,
}

/// Batch row
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchRow {
    /// L1 block number
    pub l1_block_number: u64,
    /// Transaction hash that proposed the batch
    pub l1_tx_hash: HashBytes,
    /// Batch ID
    pub batch_id: u64,
    /// Batch size
    pub batch_size: u16,
    /// Last L2 block number in this batch
    pub last_l2_block_number: u64,
    /// Proposer address
    pub proposer_addr: AddressBytes,
    /// Blob count
    pub blob_count: u8,
    /// Blob total bytes
    pub blob_total_bytes: u32,
}

impl BatchRow {
    /// Returns the L2 block numbers that belong to this batch.
    /// Calculates the range based on `last_l2_block_number` and `batch_size`.
    pub fn l2_block_numbers(&self) -> Vec<u64> {
        let last = self.last_l2_block_number;
        let count = self.batch_size as u64;

        // When the last block id is 0 but there are blocks, the batch must
        // contain the genesis block. In this case we simply return `[0]`.
        if last == 0 && count > 0 {
            return vec![0];
        }

        // Add 1 to avoid off-by-one errors.
        // Example: `last == 3`, `count == 3`, then `first == 1`.
        let first = last.saturating_sub(count) + 1;

        (first..=last).collect()
    }

    /// Returns the first L2 block number in this batch.
    pub const fn first_l2_block_number(&self) -> u64 {
        let count = self.batch_size as u64;
        if self.last_l2_block_number == 0 && count > 0 {
            return 0;
        }
        self.last_l2_block_number.saturating_sub(count) + 1
    }
}

/// Batch block mapping row
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchBlockRow {
    /// Batch ID
    pub batch_id: u64,
    /// L2 block number
    pub l2_block_number: u64,
}

/// Proved batch row
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProvedBatchRow {
    /// L1 block number
    pub l1_block_number: u64,
    /// Batch ID
    pub batch_id: u64,
    /// Verifier address
    pub verifier_addr: AddressBytes,
    /// Parent hash
    pub parent_hash: HashBytes,
    /// Block hash
    pub block_hash: HashBytes,
    /// State root
    pub state_root: HashBytes,
}

/// L2 reorg row for insertion (without `inserted_at`)
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq)]
pub struct L2ReorgInsertRow {
    /// Block number
    pub l2_block_number: u64,
    /// Depth
    pub depth: u16,
    /// Sequencer that produced the replaced block
    pub old_sequencer: AddressBytes,
    /// Sequencer that produced the new block
    pub new_sequencer: AddressBytes,
}

/// L2 reorg row
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct L2ReorgRow {
    /// Block number
    pub l2_block_number: u64,
    /// Depth
    pub depth: u16,
    /// Sequencer that produced the replaced block
    pub old_sequencer: AddressBytes,
    /// Sequencer that produced the new block
    pub new_sequencer: AddressBytes,
    /// Time the reorg was recorded.
    /// This is populated when reading from the database.
    pub inserted_at: DateTime<Utc>,
}

/// Forced inclusion processed row
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ForcedInclusionProcessedRow {
    /// Blob hash
    pub blob_hash: HashBytes,
}

/// Orphaned L2 block hash row
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrphanedL2HashRow {
    /// Block hash of orphaned block
    pub block_hash: HashBytes,
    /// L2 block number of orphaned block
    pub l2_block_number: u64,
}

/// Slashing event row
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct SlashingEventRow {
    /// L1 block number where slashing occurred
    pub l1_block_number: u64,
    /// Address of the validator that was slashed
    pub validator_addr: AddressBytes,
}

/// Row representing a failed proposal where a batch was posted by a different sequencer
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct FailedProposalRow {
    /// Batch ID that was posted
    pub batch_id: u64,
    /// Address of the sequencer that produced the (last) block in the batch
    pub original_sequencer: AddressBytes,
    /// Address of the sequencer that posted the batch on L1
    pub proposer: AddressBytes,
    /// L1 block number where the batch was posted
    pub l1_block_number: u64,
    /// Time the batch was posted
    pub inserted_at: DateTime<Utc>,
}

/// Row representing the number of blocks produced by a sequencer
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq)]
pub struct SequencerDistributionRow {
    /// Sequencer address
    pub sequencer: AddressBytes,
    /// Number of blocks produced by the sequencer
    pub blocks: u64,
    /// Number of batches proposed by the sequencer
    pub batches: u64,
    /// Earliest block timestamp for the sequencer in the selected range
    pub min_ts: u64,
    /// Latest block timestamp for the sequencer in the selected range
    pub max_ts: u64,
    /// Sum of transactions across all blocks proposed by the sequencer
    pub tx_sum: u64,
}

/// Row representing a single block proposed by a sequencer
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq)]
pub struct SequencerBlockRow {
    /// Sequencer address
    pub sequencer: AddressBytes,
    /// L2 block number proposed by the sequencer
    pub l2_block_number: u64,
}

/// Row representing grouped blocks by sequencer (database-aggregated)
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq)]
pub struct SequencerBlocksGrouped {
    /// Sequencer address
    pub sequencer: AddressBytes,
    /// Array of L2 block numbers proposed by the sequencer
    pub blocks: Vec<u64>,
}

/// Row representing the transaction count of a block and its sequencer
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockTransactionRow {
    /// Sequencer address
    pub sequencer: AddressBytes,
    /// L2 block number
    pub l2_block_number: u64,
    /// Timestamp of the L2 block
    pub block_time: DateTime<Utc>,
    /// Number of transactions in the block
    pub sum_tx: u32,
}

/// Row representing the time it took for a batch to be proven
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct BatchProveTimeRow {
    /// Batch ID
    pub batch_id: u64,
    /// Seconds between proposal and proof
    pub seconds_to_prove: u64,
}

/// Row representing the time it took for a batch to be verified
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct BatchVerifyTimeRow {
    /// Batch ID
    pub batch_id: u64,
    /// Seconds between proof and verification
    pub seconds_to_verify: u64,
}

/// Row representing the block number seen at a given minute
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct L1BlockTimeRow {
    /// Minute timestamp (unix seconds)
    pub minute: u64,
    /// Highest L1 block number within that minute
    pub l1_block_number: u64,
}

/// Row representing the time between consecutive L2 blocks
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct L2BlockTimeRow {
    /// L2 block number
    pub l2_block_number: u64,
    /// Timestamp of the L2 block
    pub block_time: DateTime<Utc>,
    /// Seconds since the previous block
    pub s_since_prev_block: u64,
}

/// Row representing the gas used in each L2 block
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct L2GasUsedRow {
    /// L2 block number
    pub l2_block_number: u64,
    /// Timestamp of the L2 block
    pub block_time: DateTime<Utc>,
    /// Total gas used in the block
    pub gas_used: u64,
}

/// Row representing the total L1 data posting cost for a block
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct L1DataCostRow {
    /// L1 block number
    pub l1_block_number: u64,
    /// Total cost in gwei for data posting transactions
    pub cost: u128,
}

/// Row used for inserting L1 data cost for a batch
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq)]
pub struct L1DataCostInsertRow {
    /// L1 block number
    pub l1_block_number: u64,
    /// Batch ID this cost corresponds to
    pub batch_id: u64,
    /// Total cost in gwei for data posting transactions
    pub cost: u128,
}

/// Row representing the prover cost for a batch
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ProveCostRow {
    /// L1 block number
    pub l1_block_number: u64,
    /// Batch ID
    pub batch_id: u64,
    /// Cost in gwei for proving the batch
    pub cost: u128,
}

/// Row used for inserting prover cost
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProveCostInsertRow {
    /// L1 block number
    pub l1_block_number: u64,
    /// Batch ID
    pub batch_id: u64,
    /// Cost in gwei for proving the batch
    pub cost: u128,
}

/// Row representing the fee components for an L2 block
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct BlockFeeComponentRow {
    /// L2 block number
    pub l2_block_number: u64,
    /// Total priority fee for the block
    pub priority_fee: u128,
    /// Total base fee for the block
    pub base_fee: u128,
    /// L1 data posting cost associated with the block, if available
    pub l1_data_cost: Option<u128>,
}

/// Row representing aggregated L2 fees for a sequencer
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct SequencerFeeRow {
    /// Sequencer address
    pub sequencer: AddressBytes,
    /// Sum of priority fees paid by the sequencer
    pub priority_fee: u128,
    /// Sum of base fees paid by the sequencer
    pub base_fee: u128,
    /// Total L1 data posting cost attributed to the sequencer
    pub l1_data_cost: u128,
    /// Total proving cost attributed to the sequencer
    pub prove_cost: u128,
}

/// Row representing the fee components for a batch
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct BatchFeeComponentRow {
    /// Batch ID
    pub batch_id: u64,
    /// L1 block number that included the batch
    pub l1_block_number: u64,
    /// Transaction hash that proposed the batch
    pub l1_tx_hash: HashBytes,
    /// Sequencer address that proposed the batch
    pub sequencer: AddressBytes,
    /// Total priority fee for the batch
    pub priority_fee: u128,
    /// Total base fee for the batch
    pub base_fee: u128,
    /// L1 data posting cost associated with the batch, if available
    pub l1_data_cost: Option<u128>,
    /// Prover cost associated with the batch, if available
    pub prove_cost: Option<u128>,
}

/// Row representing the transactions per second for an L2 block
#[derive(Debug, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct L2TpsRow {
    /// L2 block number
    pub l2_block_number: u64,
    /// Transactions per second between this and the previous block
    pub tps: f64,
}

/// Row representing the blob count for each batch
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct BatchBlobCountRow {
    /// L1 block number
    pub l1_block_number: u64,
    /// Batch ID
    pub batch_id: u64,
    /// Number of blobs in the batch
    pub blob_count: u8,
}

/// Row representing the interval between consecutive batch proposals
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct BatchPostingTimeRow {
    /// Batch ID
    pub batch_id: u64,
    /// Time the batch was inserted
    pub inserted_at: DateTime<Utc>,
    /// Milliseconds since the previous batch
    pub ms_since_prev_batch: u64,
}

/// Schema migration row
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaVersion {
    /// Migration version number
    pub version: u32,
    /// Migration filename
    pub name: String,
    /// Timestamp when the migration was applied
    pub applied_at: DateTime<Utc>,
    /// Checksum of the migration content
    pub checksum: String,
}

/// Schema migration row for insertion (without `applied_at`, relies on DEFAULT)
#[derive(Debug, Row, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaVersionInsert {
    /// Migration version number
    pub version: u32,
    /// Migration filename
    pub name: String,
    /// Checksum of the migration content
    pub checksum: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_row_l2_block_numbers() {
        // Test normal case
        let batch = BatchRow {
            l1_block_number: 1,
            l1_tx_hash: HashBytes([0u8; 32]),
            batch_id: 1,
            batch_size: 3,
            last_l2_block_number: 5,
            proposer_addr: AddressBytes([0u8; 20]),
            blob_count: 1,
            blob_total_bytes: 100,
        };
        assert_eq!(batch.l2_block_numbers(), vec![3, 4, 5]);
        assert_eq!(batch.first_l2_block_number(), 3);

        // Test genesis block case
        let genesis_batch = BatchRow {
            l1_block_number: 1,
            l1_tx_hash: HashBytes([0u8; 32]),
            batch_id: 1,
            batch_size: 1,
            last_l2_block_number: 0,
            proposer_addr: AddressBytes([0u8; 20]),
            blob_count: 1,
            blob_total_bytes: 100,
        };
        assert_eq!(genesis_batch.l2_block_numbers(), vec![0]);
        assert_eq!(genesis_batch.first_l2_block_number(), 0);

        // Test single block case
        let single_batch = BatchRow {
            l1_block_number: 1,
            l1_tx_hash: HashBytes([0u8; 32]),
            batch_id: 1,
            batch_size: 1,
            last_l2_block_number: 10,
            proposer_addr: AddressBytes([0u8; 20]),
            blob_count: 1,
            blob_total_bytes: 100,
        };
        assert_eq!(single_batch.l2_block_numbers(), vec![10]);
        assert_eq!(single_batch.first_l2_block_number(), 10);

        // Test edge case with zero blocks
        let empty_batch = BatchRow {
            l1_block_number: 1,
            l1_tx_hash: HashBytes([0u8; 32]),
            batch_id: 1,
            batch_size: 0,
            last_l2_block_number: 5,
            proposer_addr: AddressBytes([0u8; 20]),
            blob_count: 1,
            blob_total_bytes: 100,
        };
        assert_eq!(empty_batch.l2_block_numbers(), Vec::<u64>::new());
        assert_eq!(empty_batch.first_l2_block_number(), 6);
    }
}
