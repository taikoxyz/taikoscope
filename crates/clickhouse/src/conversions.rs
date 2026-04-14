use crate::{
    models::{BatchRow, ForcedInclusionProcessedRow, ProvedBatchRow, VerifiedBatchRow},
    types::{AddressBytes, HashBytes},
};
use alloy::primitives::B256;
use chainio::{ITaikoInbox, taiko::wrapper::ITaikoWrapper};
use eyre::{Error, Result, eyre};
use std::convert::TryFrom;

// Conversion from L2Header to L2HeadEvent is intentionally omitted. The
// extractor provides additional block statistics which are used when
// constructing `L2HeadEvent`, so the direct conversion from a header would
// omit important values.

// Conversion from BatchProposed to BatchRow
impl TryFrom<(&ITaikoInbox::BatchProposed, u64, B256)> for BatchRow {
    type Error = Error;

    fn try_from(input: (&ITaikoInbox::BatchProposed, u64, B256)) -> Result<Self, Self::Error> {
        let (batch, l1_block_number, tx_hash) = input;
        let batch_size = u16::try_from(batch.info.blocks.len())?;
        let blob_count = u8::try_from(batch.info.blobHashes.len())?;

        let proposer_addr = AddressBytes::from(batch.meta.proposer);

        Ok(Self {
            l1_block_number,
            l1_tx_hash: HashBytes::from(tx_hash),
            batch_id: batch.meta.batchId,
            batch_size,
            last_l2_block_number: batch.info.lastBlockId,
            proposer_addr,
            blob_count,
            blob_total_bytes: batch.info.blobByteSize,
        })
    }
}

// Conversion from (BatchesProved, u64) to ProvedBatchRow
impl TryFrom<(&ITaikoInbox::BatchesProved, u64)> for ProvedBatchRow {
    type Error = Error;

    fn try_from(input: (&ITaikoInbox::BatchesProved, u64)) -> Result<Self, Self::Error> {
        let (proved, l1_block_number) = input;

        if proved.batchIds.is_empty() || proved.transitions.is_empty() {
            return Err(eyre!("Empty batch IDs or transitions"));
        }

        // For the example, we're just taking the first transition, but you might want to handle
        // all transitions in a real implementation
        let batch_id = proved.batchIds[0];
        let transition = &proved.transitions[0];
        let verifier_addr = AddressBytes::from(proved.verifier);

        let mut parent = [0u8; 32];
        parent.copy_from_slice(transition.parentHash.as_slice());
        let mut block = [0u8; 32];
        block.copy_from_slice(transition.blockHash.as_slice());
        let mut state = [0u8; 32];
        state.copy_from_slice(transition.stateRoot.as_slice());

        Ok(Self {
            l1_block_number,
            batch_id,
            verifier_addr,
            parent_hash: HashBytes::from(parent),
            block_hash: HashBytes::from(block),
            state_root: HashBytes::from(state),
        })
    }
}

// Conversion from ForcedInclusionProcessed to ForcedInclusionProcessedRow
impl TryFrom<&ITaikoWrapper::ForcedInclusionProcessed> for ForcedInclusionProcessedRow {
    type Error = Error;

    fn try_from(event: &ITaikoWrapper::ForcedInclusionProcessed) -> Result<Self, Self::Error> {
        let mut hash_bytes = [0u8; 32];
        hash_bytes.copy_from_slice(event.forcedInclusion.blobHash.as_slice());

        Ok(Self { blob_hash: HashBytes::from(hash_bytes) })
    }
}

// Conversion from (BatchesVerified, u64) to VerifiedBatchRow
impl TryFrom<(&chainio::BatchesVerified, u64)> for VerifiedBatchRow {
    type Error = Error;

    fn try_from(input: (&chainio::BatchesVerified, u64)) -> Result<Self, Self::Error> {
        let (verified, l1_block_number) = input;

        Ok(Self {
            l1_block_number,
            batch_id: verified.batch_id,
            block_hash: HashBytes::from(verified.block_hash),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, B256};
    use chainio::{self, ITaikoInbox, taiko::wrapper::ITaikoWrapper};

    #[test]
    fn batch_proposed_into_row() {
        let batch = ITaikoInbox::BatchProposed {
            info: ITaikoInbox::BatchInfo {
                proposedIn: 7,
                blobByteSize: 100,
                blocks: vec![ITaikoInbox::BlockParams::default(); 2],
                blobHashes: vec![B256::repeat_byte(1)],
                lastBlockId: 105, // Adding a test value for the last block ID
                ..Default::default()
            },
            meta: ITaikoInbox::BatchMetadata {
                proposer: Address::repeat_byte(9),
                batchId: 42,
                ..Default::default()
            },
            ..Default::default()
        };

        let row = BatchRow::try_from((&batch, 9, B256::ZERO)).unwrap();
        assert_eq!(
            row,
            BatchRow {
                l1_block_number: 9,
                l1_tx_hash: HashBytes::from([0u8; 32]),
                batch_id: 42,
                batch_size: 2,
                last_l2_block_number: 105,
                proposer_addr: AddressBytes::from(Address::repeat_byte(9)),
                blob_count: 1,
                blob_total_bytes: 100,
            }
        );
    }

    #[test]
    fn batches_proved_into_row() {
        let transition = ITaikoInbox::Transition {
            parentHash: B256::repeat_byte(1),
            blockHash: B256::repeat_byte(2),
            stateRoot: B256::repeat_byte(3),
        };

        let proved = ITaikoInbox::BatchesProved {
            verifier: Address::repeat_byte(4),
            batchIds: vec![5],
            transitions: vec![transition],
        };

        let row = ProvedBatchRow::try_from((&proved, 11)).unwrap();
        assert_eq!(
            row,
            ProvedBatchRow {
                l1_block_number: 11,
                batch_id: 5,
                verifier_addr: AddressBytes::from(Address::repeat_byte(4)),
                parent_hash: HashBytes::from([1u8; 32]),
                block_hash: HashBytes::from([2u8; 32]),
                state_root: HashBytes::from([3u8; 32]),
            }
        );
    }

    #[test]
    fn forced_inclusion_into_row() {
        let event = ITaikoWrapper::ForcedInclusionProcessed {
            forcedInclusion: ITaikoWrapper::ForcedInclusion {
                blobHash: B256::repeat_byte(7),
                feeInGwei: 1,
                createdAtBatchId: 0,
                blobByteOffset: 0,
                blobByteSize: 0,
                blobCreatedIn: 0,
            },
        };

        let row = ForcedInclusionProcessedRow::try_from(&event).unwrap();
        assert_eq!(row, ForcedInclusionProcessedRow { blob_hash: HashBytes::from([7u8; 32]) });
    }

    #[test]
    fn batches_verified_into_row() {
        let verified = chainio::BatchesVerified { batch_id: 9, block_hash: [6u8; 32] };

        let row = VerifiedBatchRow::try_from((&verified, 15)).unwrap();
        assert_eq!(
            row,
            VerifiedBatchRow {
                l1_block_number: 15,
                batch_id: 9,
                block_hash: HashBytes::from([6u8; 32])
            }
        );
    }
}
