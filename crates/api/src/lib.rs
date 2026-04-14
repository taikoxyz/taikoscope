//! Thin HTTP API for accessing `ClickHouse` data
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::needless_for_each)]

pub mod helpers;
pub mod routes;
pub mod state;
pub mod validation;

// Re-export public items
pub use routes::router;
pub use state::{
    ApiState, DEFAULT_MAX_REQUESTS, DEFAULT_RATE_PERIOD, MAX_BLOCK_TRANSACTIONS_LIMIT,
    MAX_TABLE_LIMIT,
};

use api_types::*;
use utoipa::OpenApi;

/// `OpenAPI` documentation structure
#[derive(Debug, OpenApi)]
#[openapi(
    paths(
        routes::core::l2_head_block,
        routes::core::l1_head_block,
        routes::core::preconf_data,
        routes::table::reorgs,
        routes::table::slashings,
        routes::table::forced_inclusions,
        routes::table::failed_proposals,
        routes::core::batch_posting_times,

        routes::table::blobs_per_batch,
        routes::core::prove_times,
        routes::core::verify_times,
        routes::core::l1_block_times,
        routes::table::l2_block_times,
        routes::table::l2_gas_used,
        routes::table::l2_tps,
        routes::table::block_transactions,
        routes::core::sequencer_distribution,
        routes::core::sequencer_blocks,
        routes::core::l2_fees_components,
        routes::aggregated::dashboard_data,
        routes::aggregated::prove_costs,
        routes::core::prove_cost,
        routes::core::l1_data_cost,
        routes::core::eth_price
    ),
    components(
        schemas(
            validation::CommonQuery,
            validation::PaginatedQuery,
            validation::BlockPaginatedQuery,
            validation::TimeRangeParams,
            validation::BlockRangeParams,
            L2HeadBlockResponse,
            L1HeadBlockResponse,
            ReorgEventsResponse,
            SlashingEventsResponse,
            ForcedInclusionEventsResponse,
            FailedProposalEventsResponse,
            BatchPostingTimesResponse,
            BatchBlobsResponse,
            ProveTimesResponse,
            VerifyTimesResponse,
            L1BlockTimesResponse,
            L2BlockTimesResponse,
            L2GasUsedResponse,
            L2TpsResponse,
            SequencerDistributionResponse,
            SequencerDistributionItem,
            SequencerBlocksResponse,
            SequencerBlocksItem,
            BlockTransactionsResponse,
            BlockTransactionsItem,
            clickhouse_lib::SlashingEventRow,
            clickhouse_lib::ForcedInclusionProcessedRow,
            clickhouse_lib::L2ReorgRow,
            clickhouse_lib::BatchProveTimeRow,
            clickhouse_lib::BatchVerifyTimeRow,
            clickhouse_lib::L1BlockTimeRow,
            clickhouse_lib::L2BlockTimeRow,
            clickhouse_lib::L2GasUsedRow,
            clickhouse_lib::L2TpsRow,
            clickhouse_lib::BatchBlobCountRow,
            clickhouse_lib::BatchPostingTimeRow,
            HealthResponse,
            PreconfDataResponse,
            L2FeesResponse,
            L2FeesComponentsResponse,
            SequencerFeeRow,
            DashboardDataResponse,
            EthPriceResponse,
            ProposerCostsResponse,
            ProveCostResponse,
            api_types::ErrorResponse,
            L1DataCostResponse
        )
    ),
    tags(
        (name = "taikoscope", description = "Taikoscope API endpoints")
    ),
    info(
        title = "Taikoscope API",
        description = "API for accessing Taiko blockchain metrics and data",
        version = "0.1.0"
    )
)]
pub struct ApiDoc;
