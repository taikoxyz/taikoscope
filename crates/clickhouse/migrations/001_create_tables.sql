CREATE TABLE IF NOT EXISTS ${DB}.l1_head_events (
    l1_block_number UInt64,
    block_hash FixedString(32),
    slot UInt64,
    block_ts UInt64,
    inserted_at DateTime64(3) DEFAULT now64()
) ENGINE = MergeTree()
ORDER BY (l1_block_number);

CREATE TABLE IF NOT EXISTS ${DB}.preconf_data (
    slot UInt64,
    current_operator Nullable(FixedString(20)),
    next_operator Nullable(FixedString(20)),
    inserted_at DateTime64(3) DEFAULT now64()
) ENGINE = MergeTree()
ORDER BY (slot);

CREATE TABLE IF NOT EXISTS ${DB}.l2_head_events (
    l2_block_number UInt64,
    block_hash FixedString(32),
    block_ts UInt64,
    sum_gas_used UInt128,
    sum_tx UInt32,
    sum_priority_fee UInt128,
    sum_base_fee UInt128,
    sequencer FixedString(20),
    inserted_at DateTime64(3) DEFAULT now64()
) ENGINE = MergeTree()
ORDER BY (l2_block_number);

CREATE TABLE IF NOT EXISTS ${DB}.batches (
    l1_block_number UInt64,
    batch_id UInt64,
    batch_size UInt16,
    last_l2_block_number UInt64,
    proposer_addr FixedString(20),
    blob_count UInt8,
    blob_total_bytes UInt32,
    inserted_at DateTime64(3) DEFAULT now64()
) ENGINE = MergeTree()
ORDER BY (l1_block_number, batch_id);

CREATE TABLE IF NOT EXISTS ${DB}.proved_batches (
    l1_block_number UInt64,
    batch_id UInt64,
    verifier_addr FixedString(20),
    parent_hash FixedString(32),
    block_hash FixedString(32),
    state_root FixedString(32),
    inserted_at DateTime64(3) DEFAULT now64()
) ENGINE = MergeTree()
ORDER BY (l1_block_number, batch_id);

CREATE TABLE IF NOT EXISTS ${DB}.l2_reorgs (
    l2_block_number UInt64,
    depth UInt16,
    inserted_at DateTime64(3) DEFAULT now64()
) ENGINE = MergeTree()
ORDER BY (inserted_at);

CREATE TABLE IF NOT EXISTS ${DB}.forced_inclusion_processed (
    blob_hash FixedString(32),
    inserted_at DateTime64(3) DEFAULT now64()
) ENGINE = MergeTree()
ORDER BY (inserted_at);

CREATE TABLE IF NOT EXISTS ${DB}.verified_batches (
    l1_block_number UInt64,
    batch_id UInt64,
    block_hash FixedString(32),
    inserted_at DateTime64(3) DEFAULT now64()
) ENGINE = MergeTree()
ORDER BY (l1_block_number, batch_id);

CREATE TABLE IF NOT EXISTS ${DB}.slashing_events (
    l1_block_number UInt64,
    validator_addr FixedString(20),
    inserted_at DateTime64(3) DEFAULT now64()
) ENGINE = MergeTree()
ORDER BY (l1_block_number, validator_addr);

CREATE TABLE IF NOT EXISTS ${DB}.l1_data_costs (
    l1_block_number UInt64,
    cost UInt128,
    inserted_at DateTime64(3) DEFAULT now64()
) ENGINE = MergeTree()
ORDER BY (l1_block_number);
