//! Data aggregation utilities

use api_types::{AvgBatchBlobCountRow, BatchFeeComponentRow};
use clickhouse_lib::{BatchBlobCountRow, L2BlockTimeRow, L2TpsRow, TimeRange};
use std::collections::BTreeMap;

/// Determine bucket size based on time range
pub const fn bucket_size_from_range(range: &TimeRange) -> u64 {
    let hours = range.seconds() / 3600;
    if hours <= 1 {
        1
    } else if hours <= 6 {
        5
    } else if hours <= 12 {
        10
    } else if hours <= 24 {
        25
    } else if hours <= 48 {
        50
    } else if hours <= 72 {
        100
    } else {
        250
    }
}

/// Determine bucket size for prove time aggregation. Uses a smaller
/// bucket than [`bucket_size_from_range`] to avoid over-aggregation
/// when prove events are infrequent.
pub const fn prove_bucket_size(range: &TimeRange) -> u64 {
    let base = bucket_size_from_range(range);
    let size = base / 10;
    if size == 0 { 1 } else { size }
}

/// Determine bucket size for verify time aggregation. Uses a much smaller
/// bucket than [`bucket_size_from_range`] to capture more data points
/// since verify events are naturally infrequent.
pub const fn verify_bucket_size(range: &TimeRange) -> u64 {
    let base = bucket_size_from_range(range);
    let size = base / 25;
    if size == 0 { 1 } else { size }
}

/// Determine bucket size for blobs-per-batch aggregation. Uses a slightly
/// smaller bucket than [`bucket_size_from_range`] so that charts show more
/// detail without overwhelming the client.
pub const fn blobs_bucket_size(range: &TimeRange) -> u64 {
    let base = bucket_size_from_range(range);
    let size = base / 5;
    if size == 0 { 1 } else { size }
}

/// Aggregate L2 block times by bucket size
pub fn aggregate_l2_block_times(rows: Vec<L2BlockTimeRow>, bucket: u64) -> Vec<L2BlockTimeRow> {
    let bucket = bucket.max(1);

    if rows.is_empty() {
        return Vec::new();
    }

    // Pre-allocate with estimated capacity
    let mut groups: BTreeMap<u64, Vec<L2BlockTimeRow>> = BTreeMap::new();

    // Group rows by bucket
    for row in rows {
        let bucket_key = row.l2_block_number / bucket;
        groups.entry(bucket_key).or_insert_with(|| Vec::with_capacity(bucket as usize)).push(row);
    }

    // Pre-allocate result vector
    let mut result = Vec::with_capacity(groups.len());

    // Process each group without unnecessary sorting
    for (g, rs) in groups {
        // Find min/max directly without sorting
        let mut last_time = rs[0].block_time;
        let mut max_block_num = rs[0].l2_block_number;
        let mut sum = 0u64;
        let count = rs.len() as u64;

        for r in &rs {
            if r.l2_block_number > max_block_num {
                max_block_num = r.l2_block_number;
                last_time = r.block_time;
            }
            sum += r.s_since_prev_block;
        }

        let avg = if count > 0 { sum / count } else { 0 };
        result.push(L2BlockTimeRow {
            l2_block_number: g * bucket,
            block_time: last_time,
            s_since_prev_block: avg,
        });
    }

    result
}

/// Aggregate batch fee components by bucket size
pub fn aggregate_batch_fee_components(
    rows: Vec<BatchFeeComponentRow>,
    bucket: u64,
) -> Vec<BatchFeeComponentRow> {
    let bucket = bucket.max(1);

    if rows.is_empty() {
        return Vec::new();
    }

    // Pre-allocate with estimated capacity
    let mut groups: BTreeMap<u64, Vec<BatchFeeComponentRow>> = BTreeMap::new();

    // Group rows by bucket
    for row in rows {
        let bucket_key = row.batch_id / bucket;
        groups.entry(bucket_key).or_insert_with(|| Vec::with_capacity(bucket as usize)).push(row);
    }

    // Pre-allocate result vector
    let mut result = Vec::with_capacity(groups.len());

    // Process each group with single iteration
    for (g, rs) in groups {
        let mut sum_priority = 0u128;
        let mut sum_base = 0u128;
        let mut sum_l1 = 0u128;
        let mut sum_prove = 0u128;
        let mut any_l1 = false;
        let mut any_prove = false;
        let mut last_l1 = 0u64;
        let mut last_hash = String::new();
        let mut last_seq = String::new();
        let mut max_batch_id = 0u64;

        // Single pass through all rows in the group
        for r in &rs {
            sum_priority += r.priority_fee;
            sum_base += r.base_fee;

            if let Some(l1_cost) = r.l1_data_cost {
                sum_l1 += l1_cost;
                any_l1 = true;
            }

            if let Some(prove_cost) = r.prove_cost {
                sum_prove += prove_cost;
                any_prove = true;
            }

            // Track the latest entry by batch_id
            if r.batch_id >= max_batch_id {
                max_batch_id = r.batch_id;
                last_l1 = r.l1_block_number;
                // Only clone when we need to update (reduces string allocations)
                if last_hash != r.l1_tx_hash {
                    last_hash = r.l1_tx_hash.clone();
                }
                if last_seq != r.sequencer {
                    last_seq = r.sequencer.clone();
                }
            }
        }

        result.push(BatchFeeComponentRow {
            batch_id: g * bucket,
            l1_block_number: last_l1,
            l1_tx_hash: last_hash,
            sequencer: last_seq,
            priority_fee: sum_priority,
            base_fee: sum_base,
            l1_data_cost: any_l1.then_some(sum_l1),
            prove_cost: any_prove.then_some(sum_prove),
        });
    }

    result
}

/// Aggregate L2 TPS by bucket size
pub fn aggregate_l2_tps(rows: Vec<L2TpsRow>, bucket: u64) -> Vec<L2TpsRow> {
    let bucket = bucket.max(1);

    if rows.is_empty() {
        return Vec::new();
    }

    // Pre-allocate with estimated capacity
    let mut groups: BTreeMap<u64, Vec<L2TpsRow>> = BTreeMap::new();

    // Group rows by bucket
    for row in rows {
        let bucket_key = row.l2_block_number / bucket;
        groups.entry(bucket_key).or_insert_with(|| Vec::with_capacity(bucket as usize)).push(row);
    }

    // Pre-allocate result vector
    let mut result = Vec::with_capacity(groups.len());

    // Process each group directly
    for (g, rs) in groups {
        let mut sum = 0f64;
        let count = rs.len() as f64;

        for r in &rs {
            sum += r.tps;
        }

        let avg = if count > 0.0 { sum / count } else { 0.0 };
        result.push(L2TpsRow { l2_block_number: g * bucket, tps: avg });
    }

    result
}

/// Aggregate blobs per batch by bucket size
pub fn aggregate_blobs_per_batch(
    rows: Vec<BatchBlobCountRow>,
    bucket: u64,
) -> Vec<AvgBatchBlobCountRow> {
    let bucket = bucket.max(1);

    if rows.is_empty() {
        return Vec::new();
    }

    // Pre-allocate with estimated capacity
    let mut groups: BTreeMap<u64, Vec<BatchBlobCountRow>> = BTreeMap::new();

    // Group rows by bucket
    for row in rows {
        let bucket_key = row.batch_id / bucket;
        groups.entry(bucket_key).or_insert_with(|| Vec::with_capacity(bucket as usize)).push(row);
    }

    // Pre-allocate result vector
    let mut result = Vec::with_capacity(groups.len());

    // Process each group directly
    for (g, rs) in groups {
        let mut sum_blobs = 0u32;
        let mut last_l1_block = 0u64;
        let mut max_batch_id = 0u64;
        let count = rs.len();

        for r in &rs {
            sum_blobs += r.blob_count as u32;
            // Track the latest entry by batch_id
            if r.batch_id >= max_batch_id {
                max_batch_id = r.batch_id;
                last_l1_block = r.l1_block_number;
            }
        }

        let avg_blobs = if count > 0 { sum_blobs as f64 / count as f64 } else { 0.0 };
        result.push(AvgBatchBlobCountRow {
            l1_block_number: last_l1_block,
            batch_id: g * bucket,
            blob_count: avg_blobs,
        });
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    // Helper functions for creating test data
    fn create_l2_block_time_row(
        block_num: u64,
        s_since_prev: u64,
        time_offset_secs: i64,
    ) -> L2BlockTimeRow {
        L2BlockTimeRow {
            l2_block_number: block_num,
            block_time: Utc::now() + chrono::Duration::seconds(time_offset_secs),
            s_since_prev_block: s_since_prev,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn create_batch_fee_component_row(
        batch_id: u64,
        l1_block_number: u64,
        l1_tx_hash: String,
        sequencer: &str,
        priority_fee: u128,
        base_fee: u128,
        l1_cost: Option<u128>,
        prove_cost: Option<u128>,
    ) -> BatchFeeComponentRow {
        BatchFeeComponentRow {
            batch_id,
            l1_block_number,
            l1_tx_hash,
            sequencer: sequencer.to_owned(),
            priority_fee,
            base_fee,
            l1_data_cost: l1_cost,
            prove_cost,
        }
    }

    fn create_l2_tps_row(block_num: u64, tps: f64) -> L2TpsRow {
        L2TpsRow { l2_block_number: block_num, tps }
    }

    fn create_batch_blob_count_row(
        batch_id: u64,
        l1_block_number: u64,
        blob_count: u8,
    ) -> BatchBlobCountRow {
        BatchBlobCountRow { batch_id, l1_block_number, blob_count }
    }

    // Tests for bucket_size_from_range
    #[test]
    fn test_bucket_size_from_range_15_min() {
        let range = TimeRange::Last15Min; // 900 seconds = 0.25 hours
        assert_eq!(bucket_size_from_range(&range), 1);
    }

    #[test]
    fn test_bucket_size_from_range_1_hour() {
        let range = TimeRange::LastHour; // 3600 seconds = 1 hour
        assert_eq!(bucket_size_from_range(&range), 1);
    }

    #[test]
    fn test_bucket_size_from_range_6_hours() {
        let range = TimeRange::Custom(6 * 3600); // 6 hours
        assert_eq!(bucket_size_from_range(&range), 5);
    }

    #[test]
    fn test_bucket_size_from_range_12_hours() {
        let range = TimeRange::Custom(12 * 3600); // 12 hours
        assert_eq!(bucket_size_from_range(&range), 10);
    }

    #[test]
    fn test_bucket_size_from_range_24_hours() {
        let range = TimeRange::Last24Hours; // 24 hours
        assert_eq!(bucket_size_from_range(&range), 25);
    }

    #[test]
    fn test_bucket_size_from_range_48_hours() {
        let range = TimeRange::Custom(48 * 3600); // 48 hours
        assert_eq!(bucket_size_from_range(&range), 50);
    }

    #[test]
    fn test_bucket_size_from_range_72_hours() {
        let range = TimeRange::Custom(72 * 3600); // 72 hours
        assert_eq!(bucket_size_from_range(&range), 100);
    }

    #[test]
    fn test_bucket_size_from_range_7_days() {
        let range = TimeRange::Last7Days; // 7 days
        assert_eq!(bucket_size_from_range(&range), 250);
    }

    #[test]
    fn test_bucket_size_from_range_zero_seconds() {
        let range = TimeRange::Custom(0);
        assert_eq!(bucket_size_from_range(&range), 1);
    }

    #[test]
    fn test_prove_bucket_size_smaller() {
        let range = TimeRange::Custom(6 * 3600); // 6 hours
        assert_eq!(prove_bucket_size(&range), 1); // base 5 / 10 = 0 -> 1
    }

    #[test]
    fn test_blobs_bucket_size_smaller() {
        let range = TimeRange::Custom(12 * 3600); // 12 hours
        assert_eq!(blobs_bucket_size(&range), 2); // base 10 / 5 = 2
    }

    // Tests for aggregate_l2_block_times
    #[test]
    fn test_aggregate_l2_block_times_empty() {
        let rows = vec![];
        let result = aggregate_l2_block_times(rows, 5);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_aggregate_l2_block_times_single_row() {
        let rows = vec![create_l2_block_time_row(10, 1, 0)];
        let result = aggregate_l2_block_times(rows, 5);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].l2_block_number, 10); // 10 / 5 * 5 = 10
        assert_eq!(result[0].s_since_prev_block, 1);
    }

    #[test]
    fn test_aggregate_l2_block_times_multiple_in_bucket() {
        let rows = vec![
            create_l2_block_time_row(10, 1, 0),
            create_l2_block_time_row(11, 2, 1),
            create_l2_block_time_row(12, 3, 2),
        ];
        let result = aggregate_l2_block_times(rows, 5);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].l2_block_number, 10); // bucket 2 * 5 = 10
        assert_eq!(result[0].s_since_prev_block, 2); // (1 + 2 + 3) / 3 = 2
    }

    #[test]
    fn test_aggregate_l2_block_times_multiple_buckets() {
        let rows = vec![create_l2_block_time_row(2, 1, 0), create_l2_block_time_row(7, 2, 1)];
        let result = aggregate_l2_block_times(rows, 5);

        assert_eq!(result.len(), 2);
        // First bucket: block 2 -> bucket 0
        assert_eq!(result[0].l2_block_number, 0);
        assert_eq!(result[0].s_since_prev_block, 1);
        // Second bucket: block 7 -> bucket 1
        assert_eq!(result[1].l2_block_number, 5);
        assert_eq!(result[1].s_since_prev_block, 2);
    }

    #[test]
    fn test_aggregate_l2_block_times_zero_bucket() {
        let rows = vec![create_l2_block_time_row(10, 1, 0)];
        let result = aggregate_l2_block_times(rows, 0);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].l2_block_number, 10); // bucket becomes 1
    }

    // Tests for aggregate_batch_fee_components
    #[test]
    fn test_aggregate_batch_fee_components_empty() {
        let rows = vec![];
        let result = aggregate_batch_fee_components(rows, 5);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_aggregate_batch_fee_components_single_row() {
        let rows = vec![create_batch_fee_component_row(
            10,
            100,
            "0x0".into(),
            "seq1",
            1000,
            2000,
            Some(500),
            Some(300),
        )];
        let result = aggregate_batch_fee_components(rows, 5);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].batch_id, 10);
        assert_eq!(result[0].l1_block_number, 100);
        assert_eq!(result[0].l1_tx_hash, "0x0");
        assert_eq!(result[0].sequencer, "seq1");
        assert_eq!(result[0].priority_fee, 1000);
        assert_eq!(result[0].base_fee, 2000);
        assert_eq!(result[0].l1_data_cost, Some(500));
        assert_eq!(result[0].prove_cost, Some(300));
    }

    #[test]
    fn test_aggregate_batch_fee_components_summation() {
        let rows = vec![
            create_batch_fee_component_row(
                10,
                100,
                "0x0".into(),
                "seq1",
                1000,
                2000,
                Some(500),
                Some(300),
            ),
            create_batch_fee_component_row(
                11,
                101,
                "0x1".into(),
                "seq2",
                1500,
                2500,
                Some(600),
                Some(400),
            ),
        ];
        let result = aggregate_batch_fee_components(rows, 5);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].priority_fee, 2500); // 1000 + 1500
        assert_eq!(result[0].base_fee, 4500); // 2000 + 2500
        assert_eq!(result[0].l1_data_cost, Some(1100)); // 500 + 600
        assert_eq!(result[0].prove_cost, Some(700)); // 300 + 400
        assert_eq!(result[0].l1_block_number, 101); // Last value
        assert_eq!(result[0].l1_tx_hash, "0x1"); // Last value
        assert_eq!(result[0].sequencer, "seq2"); // Last value
    }

    #[test]
    fn test_aggregate_batch_fee_components_mixed_optional_fields() {
        let rows = vec![
            create_batch_fee_component_row(
                10,
                100,
                "0x0".into(),
                "seq1",
                1000,
                2000,
                None,
                Some(300),
            ),
            create_batch_fee_component_row(
                11,
                101,
                "0x1".into(),
                "seq2",
                1500,
                2500,
                Some(600),
                None,
            ),
        ];
        let result = aggregate_batch_fee_components(rows, 5);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].l1_data_cost, Some(600)); // any=true due to second row
        assert_eq!(result[0].prove_cost, Some(300)); // any=true due to first row
    }

    // Tests for aggregate_l2_tps
    #[test]
    fn test_aggregate_l2_tps_empty() {
        let rows = vec![];
        let result = aggregate_l2_tps(rows, 5);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_aggregate_l2_tps_single_row() {
        let rows = vec![create_l2_tps_row(10, 15.5)];
        let result = aggregate_l2_tps(rows, 5);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].l2_block_number, 10); // 10 / 5 * 5 = 10
        assert_eq!(result[0].tps, 15.5);
    }

    #[test]
    fn test_aggregate_l2_tps_multiple_in_bucket() {
        let rows = vec![
            create_l2_tps_row(10, 10.0),
            create_l2_tps_row(11, 20.0),
            create_l2_tps_row(12, 30.0),
        ];
        let result = aggregate_l2_tps(rows, 5);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].l2_block_number, 10); // bucket 2 * 5 = 10
        assert_eq!(result[0].tps, 20.0); // (10.0 + 20.0 + 30.0) / 3 = 20.0
    }

    #[test]
    fn test_aggregate_l2_tps_multiple_buckets() {
        let rows = vec![create_l2_tps_row(2, 10.0), create_l2_tps_row(7, 30.0)];
        let result = aggregate_l2_tps(rows, 5);

        assert_eq!(result.len(), 2);
        // First bucket: block 2 -> bucket 0
        assert_eq!(result[0].l2_block_number, 0);
        assert_eq!(result[0].tps, 10.0);
        // Second bucket: block 7 -> bucket 1
        assert_eq!(result[1].l2_block_number, 5);
        assert_eq!(result[1].tps, 30.0);
    }

    #[test]
    fn test_aggregate_l2_tps_zero_tps() {
        let rows = vec![create_l2_tps_row(10, 0.0), create_l2_tps_row(11, 10.0)];
        let result = aggregate_l2_tps(rows, 5);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].tps, 5.0); // (0.0 + 10.0) / 2 = 5.0
    }

    #[test]
    fn test_aggregate_l2_tps_zero_bucket() {
        let rows = vec![create_l2_tps_row(10, 15.5)];
        let result = aggregate_l2_tps(rows, 0);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].l2_block_number, 10); // bucket becomes 1
        assert_eq!(result[0].tps, 15.5);
    }

    #[test]
    fn test_aggregate_l2_tps_fractional_values() {
        let rows = vec![
            create_l2_tps_row(10, 1.25),
            create_l2_tps_row(11, 2.75),
            create_l2_tps_row(12, 3.5),
        ];
        let result = aggregate_l2_tps(rows, 5);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].tps, 2.5); // (1.25 + 2.75 + 3.5) / 3 = 2.5
    }

    // Tests for aggregate_blobs_per_batch
    #[test]
    fn test_aggregate_blobs_per_batch_empty() {
        let rows = vec![];
        let result = aggregate_blobs_per_batch(rows, 5);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_aggregate_blobs_per_batch_single_row() {
        let rows = vec![create_batch_blob_count_row(10, 100, 3)];
        let result = aggregate_blobs_per_batch(rows, 5);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].batch_id, 10);
        assert_eq!(result[0].l1_block_number, 100);
        assert_eq!(result[0].blob_count, 3.0);
    }

    #[test]
    fn test_aggregate_blobs_per_batch_multiple_in_bucket() {
        let rows = vec![
            create_batch_blob_count_row(10, 100, 2),
            create_batch_blob_count_row(11, 101, 4),
            create_batch_blob_count_row(12, 102, 6),
        ];
        let result = aggregate_blobs_per_batch(rows, 5);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].batch_id, 10); // bucket 2 * 5 = 10
        assert_eq!(result[0].l1_block_number, 102); // last l1_block_number
        assert_eq!(result[0].blob_count, 4.0); // (2 + 4 + 6) / 3 = 4
    }

    #[test]
    fn test_aggregate_blobs_per_batch_multiple_buckets() {
        let rows =
            vec![create_batch_blob_count_row(2, 100, 2), create_batch_blob_count_row(7, 105, 6)];
        let result = aggregate_blobs_per_batch(rows, 5);

        assert_eq!(result.len(), 2);
        // First bucket: batch 2 -> bucket 0
        assert_eq!(result[0].batch_id, 0);
        assert_eq!(result[0].l1_block_number, 100);
        assert_eq!(result[0].blob_count, 2.0);
        // Second bucket: batch 7 -> bucket 1
        assert_eq!(result[1].batch_id, 5);
        assert_eq!(result[1].l1_block_number, 105);
        assert_eq!(result[1].blob_count, 6.0);
    }

    #[test]
    fn test_aggregate_blobs_per_batch_zero_blobs() {
        let rows =
            vec![create_batch_blob_count_row(10, 100, 0), create_batch_blob_count_row(11, 101, 4)];
        let result = aggregate_blobs_per_batch(rows, 5);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].blob_count, 2.0); // (0 + 4) / 2 = 2
    }

    #[test]
    fn test_aggregate_blobs_per_batch_zero_bucket() {
        let rows =
            vec![create_batch_blob_count_row(10, 100, 3), create_batch_blob_count_row(20, 200, 5)];
        let result = aggregate_blobs_per_batch(rows, 0);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].batch_id, 10);
        assert_eq!(result[0].blob_count, 3.0);
        assert_eq!(result[1].batch_id, 20);
        assert_eq!(result[1].blob_count, 5.0);
    }

    #[test]
    fn test_aggregate_blobs_per_batch_large_values() {
        let rows = vec![
            create_batch_blob_count_row(1000, 5000, 255), // max u8 value
            create_batch_blob_count_row(1001, 5001, 200),
            create_batch_blob_count_row(1002, 5002, 100),
        ];
        let result = aggregate_blobs_per_batch(rows, 10);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].batch_id, 1000);
        assert_eq!(result[0].l1_block_number, 5002);
        assert_eq!(result[0].blob_count, 185.0); // (255 + 200 + 100) / 3 = 185
    }
}
