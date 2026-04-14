//! Core simple API endpoints

use crate::{
    helpers::{
        database_error, format_address, parse_address, prove_bucket_size, query_error,
        verify_bucket_size, wei_to_gwei, wei_to_gwei_opt,
    },
    state::{ApiState, MAX_TABLE_LIMIT},
    validation::{
        CommonQuery, PaginatedQuery, QueryMode, UnifiedQuery, has_time_range_params,
        resolve_time_range_bounds, resolve_time_range_enum, resolve_time_range_since,
        validate_pagination, validate_range_exclusivity, validate_time_range,
        validate_unified_query,
    },
};
use alloy_primitives::B256;
use api_types::{
    BatchFeeComponentRow, BatchPostingTimesResponse, ErrorResponse, EthPriceResponse,
    L1BlockTimesResponse, L1DataCostResponse, L1HeadBlockResponse, L2FeesComponentsResponse,
    L2HeadBlockResponse, PreconfDataResponse, ProveCostResponse, ProveTimesResponse,
    SequencerBlocksItem, SequencerBlocksResponse, SequencerDistributionItem,
    SequencerDistributionResponse, SequencerFeeRow, VerifyTimesResponse,
};
use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use clickhouse_lib::{L1DataCostRow, ProveCostRow};

// Legacy type aliases for backward compatibility
type RangeQuery = CommonQuery;

#[utoipa::path(
    get,
    path = "/l2-head-block",
    responses(
        (status = 200, description = "L2 head block number", body = L2HeadBlockResponse),
        (status = 500, description = "Database error", body = ErrorResponse)
    ),
    tag = "taikoscope"
)]
/// Get the block number of the latest L2 block
pub async fn l2_head_block(
    State(state): State<ApiState>,
) -> Result<Json<L2HeadBlockResponse>, ErrorResponse> {
    let num = state
        .client
        .get_last_l2_block_number()
        .await
        .map_err(|e| database_error("get L2 head block number", e))?;
    Ok(Json(L2HeadBlockResponse { l2_head_block: num }))
}

#[utoipa::path(
    get,
    path = "/l1-head-block",
    responses(
        (status = 200, description = "L1 head block number", body = L1HeadBlockResponse),
        (status = 500, description = "Database error", body = ErrorResponse)
    ),
    tag = "taikoscope"
)]
/// Get the block number of the latest L1 block
pub async fn l1_head_block(
    State(state): State<ApiState>,
) -> Result<Json<L1HeadBlockResponse>, ErrorResponse> {
    let num = state
        .client
        .get_last_l1_block_number()
        .await
        .map_err(|e| database_error("get L1 head block number", e))?;
    Ok(Json(L1HeadBlockResponse { l1_head_block: num }))
}

#[utoipa::path(
    get,
    path = "/preconf-data",
    responses(
        (status = 200, description = "Latest preconfiguration data", body = PreconfDataResponse),
        (status = 500, description = "Database error", body = ErrorResponse)
    ),
    tag = "taikoscope"
)]
/// Get the most recent preconfiguration data including candidates and operators
pub async fn preconf_data(
    State(state): State<ApiState>,
) -> Result<Json<PreconfDataResponse>, ErrorResponse> {
    let data = state
        .client
        .get_last_preconf_data()
        .await
        .map_err(|e| database_error("get preconf data", e))?;

    let empty =
        PreconfDataResponse { candidates: Vec::new(), current_operator: None, next_operator: None };

    let resp = data.map_or(empty, |d| PreconfDataResponse {
        candidates: d.candidates.into_iter().map(format_address).collect(),
        current_operator: d.current_operator.map(format_address),
        next_operator: d.next_operator.map(format_address),
    });

    Ok(Json(resp))
}

#[utoipa::path(
    get,
    path = "/batch-posting-times",
    params(
        PaginatedQuery
    ),
    responses(
        (status = 200, description = "Batch posting times", body = BatchPostingTimesResponse),
        (status = 500, description = "Database error", body = ErrorResponse)
    ),
    tag = "taikoscope"
)]
/// Get batch posting timing metrics for the specified time range.
///
/// Results are ordered by batch id in descending order.
pub async fn batch_posting_times(
    Query(params): Query<PaginatedQuery>,
    State(state): State<ApiState>,
) -> Result<Json<BatchPostingTimesResponse>, ErrorResponse> {
    // Validate time range parameters
    validate_time_range(&params.common.time_range)?;

    // Check for range exclusivity
    let limit = validate_pagination(
        params.starting_after.as_ref(),
        params.ending_before.as_ref(),
        params.limit.as_ref(),
        MAX_TABLE_LIMIT,
    )?;
    let has_time_range = has_time_range_params(&params.common.time_range);
    let has_slot_range = params.starting_after.is_some() || params.ending_before.is_some();
    validate_range_exclusivity(has_time_range, has_slot_range)?;

    let since = resolve_time_range_since(&params.common.time_range);
    let rows = match state
        .client
        .get_batch_posting_times_paginated(
            since,
            limit,
            params.starting_after,
            params.ending_before,
        )
        .await
    {
        Ok(r) => r,
        Err(e) => return Err(query_error("batch posting times", e)),
    };
    tracing::info!(count = rows.len(), "Returning batch posting times");
    Ok(Json(BatchPostingTimesResponse { batches: rows }))
}

#[utoipa::path(
    get,
    path = "/prove-times",
    params(
        UnifiedQuery
    ),
    responses(
        (status = 200, description = "Prove times (regular or aggregated)", body = ProveTimesResponse),
        (status = 500, description = "Database error", body = ErrorResponse)
    ),
    tag = "taikoscope"
)]
/// Get batch proving time metrics.
///
/// Use ?aggregated for aggregated data with automatic bucketing based on time range.
/// Without ?aggregated, returns paginated results ordered by batch id in descending order.
#[allow(clippy::cognitive_complexity)]
pub async fn prove_times(
    Query(params): Query<UnifiedQuery>,
    State(state): State<ApiState>,
) -> Result<Json<ProveTimesResponse>, ErrorResponse> {
    let query_mode = validate_unified_query(&params, MAX_TABLE_LIMIT)?;

    match query_mode {
        QueryMode::Aggregated => {
            // Aggregated mode - use time range parameters
            validate_time_range(&params.common.time_range)?;
            let has_time_range = has_time_range_params(&params.common.time_range);
            validate_range_exclusivity(has_time_range, false)?;

            let time_range = resolve_time_range_enum(&params.common.time_range);
            let bucket = prove_bucket_size(&time_range);
            let batches = match state.client.get_prove_times(time_range, Some(bucket)).await {
                Ok(rows) => rows,
                Err(e) => return Err(query_error("prove times", e)),
            };
            tracing::info!(count = batches.len(), "Returning aggregated prove times");
            Ok(Json(ProveTimesResponse { batches }))
        }
        QueryMode::Regular { limit } => {
            // Regular paginated mode
            validate_time_range(&params.common.time_range)?;
            let has_time_range = has_time_range_params(&params.common.time_range);
            let has_slot_range = params.starting_after.is_some() || params.ending_before.is_some();
            validate_range_exclusivity(has_time_range, has_slot_range)?;

            let since = resolve_time_range_since(&params.common.time_range);
            let batches = match state
                .client
                .get_prove_times_paginated(
                    since,
                    limit,
                    params.starting_after,
                    params.ending_before,
                )
                .await
            {
                Ok(rows) => rows,
                Err(e) => return Err(query_error("prove times", e)),
            };
            tracing::info!(count = batches.len(), "Returning paginated prove times");
            Ok(Json(ProveTimesResponse { batches }))
        }
    }
}

#[utoipa::path(
    get,
    path = "/verify-times",
    params(
        UnifiedQuery
    ),
    responses(
        (status = 200, description = "Verify times (regular or aggregated)", body = VerifyTimesResponse),
        (status = 500, description = "Database error", body = ErrorResponse)
    ),
    tag = "taikoscope"
)]
/// Get batch verification time metrics.
///
/// Use ?aggregated for aggregated data with automatic bucketing based on time range.
/// Without ?aggregated, returns paginated results ordered by batch id in descending order.
#[allow(clippy::cognitive_complexity)]
pub async fn verify_times(
    Query(params): Query<UnifiedQuery>,
    State(state): State<ApiState>,
) -> Result<Json<VerifyTimesResponse>, ErrorResponse> {
    let query_mode = validate_unified_query(&params, MAX_TABLE_LIMIT)?;

    match query_mode {
        QueryMode::Aggregated => {
            validate_time_range(&params.common.time_range)?;
            let has_time_range = has_time_range_params(&params.common.time_range);
            validate_range_exclusivity(has_time_range, false)?;

            let time_range = resolve_time_range_enum(&params.common.time_range);
            let bucket = verify_bucket_size(&time_range);
            let batches = match state.client.get_verify_times(time_range, Some(bucket)).await {
                Ok(rows) => rows,
                Err(e) => return Err(query_error("verify times", e)),
            };
            tracing::info!(count = batches.len(), "Returning aggregated verify times");
            Ok(Json(VerifyTimesResponse { batches }))
        }
        QueryMode::Regular { limit } => {
            validate_time_range(&params.common.time_range)?;
            let has_time_range = has_time_range_params(&params.common.time_range);
            let has_slot_range = params.starting_after.is_some() || params.ending_before.is_some();
            validate_range_exclusivity(has_time_range, has_slot_range)?;

            let since = resolve_time_range_since(&params.common.time_range);
            let batches = match state
                .client
                .get_verify_times_paginated(
                    since,
                    limit,
                    params.starting_after,
                    params.ending_before,
                )
                .await
            {
                Ok(rows) => rows,
                Err(e) => return Err(query_error("verify times", e)),
            };
            tracing::info!(count = batches.len(), "Returning paginated verify times");
            Ok(Json(VerifyTimesResponse { batches }))
        }
    }
}

#[utoipa::path(
    get,
    path = "/l1-block-times",
    params(
        RangeQuery
    ),
    responses(
        (status = 200, description = "L1 block times", body = L1BlockTimesResponse),
        (status = 500, description = "Database error", body = ErrorResponse)
    ),
    tag = "taikoscope"
)]
/// Get L1 block timing information for the specified time range
pub async fn l1_block_times(
    Query(params): Query<RangeQuery>,
    State(state): State<ApiState>,
) -> Result<Json<L1BlockTimesResponse>, ErrorResponse> {
    // Validate time range parameters
    validate_time_range(&params.time_range)?;

    // Check for range exclusivity
    let has_time_range = has_time_range_params(&params.time_range);
    validate_range_exclusivity(has_time_range, false)?;

    let time_range = resolve_time_range_enum(&params.time_range);
    let blocks = match state.client.get_l1_block_times(time_range).await {
        Ok(rows) => rows,
        Err(e) => return Err(query_error("L1 block times", e)),
    };
    tracing::info!(count = blocks.len(), "Returning L1 block times");
    Ok(Json(L1BlockTimesResponse { blocks }))
}

#[utoipa::path(
    get,
    path = "/sequencer-distribution",
    params(
        RangeQuery
    ),
    responses(
        (status = 200, description = "Sequencer distribution", body = SequencerDistributionResponse),
        (status = 500, description = "Database error", body = ErrorResponse)
    ),
    tag = "taikoscope"
)]
/// Get the distribution of blocks, batches, and TPS across different sequencers
pub async fn sequencer_distribution(
    Query(params): Query<RangeQuery>,
    State(state): State<ApiState>,
) -> Result<Json<SequencerDistributionResponse>, ErrorResponse> {
    // Validate time range parameters
    validate_time_range(&params.time_range)?;

    // Check for range exclusivity
    let has_time_range = has_time_range_params(&params.time_range);
    validate_range_exclusivity(has_time_range, false)?;

    // Determine the exact start and end timestamps for the range
    let (since, until) = resolve_time_range_bounds(&params.time_range);
    // Fetch distribution within the specified window
    let rows = state
        .client
        .get_sequencer_distribution_range(since, until)
        .await
        .map_err(|e| query_error("sequencer distribution", e))?;
    let sequencers: Vec<SequencerDistributionItem> = rows
        .into_iter()
        .map(|r| {
            let tps = (r.max_ts > r.min_ts && r.tx_sum > 0)
                .then(|| r.tx_sum as f64 / (r.max_ts - r.min_ts) as f64);
            SequencerDistributionItem {
                address: format_address(r.sequencer),
                blocks: r.blocks,
                batches: r.batches,
                tps,
            }
        })
        .collect();
    tracing::info!(count = sequencers.len(), "Returning sequencer distribution");
    Ok(Json(SequencerDistributionResponse { sequencers }))
}

// Legacy type aliases for backward compatibility
type SequencerBlocksQuery = CommonQuery;

#[utoipa::path(
    get,
    path = "/sequencer-blocks",
    params(
        SequencerBlocksQuery
    ),
    responses(
        (status = 200, description = "Sequencer blocks", body = SequencerBlocksResponse),
        (status = 500, description = "Database error", body = ErrorResponse)
    ),
    tag = "taikoscope"
)]
/// Get the list of blocks produced by each sequencer
pub async fn sequencer_blocks(
    Query(params): Query<SequencerBlocksQuery>,
    State(state): State<ApiState>,
) -> Result<Json<SequencerBlocksResponse>, ErrorResponse> {
    // Validate time range parameters
    validate_time_range(&params.time_range)?;

    // Check for range exclusivity
    let has_time_range = has_time_range_params(&params.time_range);
    validate_range_exclusivity(has_time_range, false)?;

    let since = resolve_time_range_since(&params.time_range);
    let rows = state
        .client
        .get_sequencer_blocks_grouped_since(since)
        .await
        .map_err(|e| query_error("sequencer blocks", e))?;

    let filter = params.address.as_ref().and_then(|addr| parse_address(addr).ok());

    let sequencers: Vec<SequencerBlocksItem> = rows
        .into_iter()
        .filter_map(|r| {
            if let Some(addr) = filter &&
                r.sequencer != addr
            {
                return None;
            }
            Some(SequencerBlocksItem { address: format_address(r.sequencer), blocks: r.blocks })
        })
        .collect();
    tracing::info!(count = sequencers.len(), "Returning sequencer blocks");
    Ok(Json(SequencerBlocksResponse { sequencers }))
}

#[utoipa::path(
    get,
    path = "/l1-data-cost",
    params(
        PaginatedQuery
    ),
    responses(
        (status = 200, description = "L1 data posting cost", body = L1DataCostResponse),
        (status = 500, description = "Database error", body = ErrorResponse)
    ),
    tag = "taikoscope"
)]
/// Get L1 data posting cost information for the specified time range.
///
/// Results are ordered by L1 block number in descending order.
pub async fn l1_data_cost(
    Query(params): Query<PaginatedQuery>,
    State(state): State<ApiState>,
) -> Result<Json<L1DataCostResponse>, ErrorResponse> {
    validate_time_range(&params.common.time_range)?;
    let limit = validate_pagination(
        params.starting_after.as_ref(),
        params.ending_before.as_ref(),
        params.limit.as_ref(),
        MAX_TABLE_LIMIT,
    )?;
    let has_time_range = has_time_range_params(&params.common.time_range);
    let has_slot_range = params.starting_after.is_some() || params.ending_before.is_some();
    validate_range_exclusivity(has_time_range, has_slot_range)?;

    let since = resolve_time_range_since(&params.common.time_range);
    let rows = match state
        .client
        .get_l1_data_costs_paginated(since, limit, params.starting_after, params.ending_before)
        .await
    {
        Ok(r) => r,
        Err(e) => return Err(query_error("L1 data cost", e)),
    };
    let rows: Vec<L1DataCostRow> = rows
        .into_iter()
        .map(|r| L1DataCostRow { l1_block_number: r.l1_block_number, cost: wei_to_gwei(r.cost) })
        .collect();
    tracing::info!(count = rows.len(), "Returning L1 data cost");
    Ok(Json(L1DataCostResponse { blocks: rows }))
}

#[utoipa::path(
    get,
    path = "/prove-cost",
    params(
        PaginatedQuery
    ),
    responses(
        (status = 200, description = "Prover cost", body = ProveCostResponse),
        (status = 500, description = "Database error", body = ErrorResponse)
    ),
    tag = "taikoscope"
)]
/// Get prover cost information for the specified time range.
///
/// Results are ordered by batch id in descending order.
pub async fn prove_cost(
    Query(params): Query<PaginatedQuery>,
    State(state): State<ApiState>,
) -> Result<Json<ProveCostResponse>, ErrorResponse> {
    validate_time_range(&params.common.time_range)?;
    let limit = validate_pagination(
        params.starting_after.as_ref(),
        params.ending_before.as_ref(),
        params.limit.as_ref(),
        MAX_TABLE_LIMIT,
    )?;
    let has_time_range = has_time_range_params(&params.common.time_range);
    let has_slot_range = params.starting_after.is_some() || params.ending_before.is_some();
    validate_range_exclusivity(has_time_range, has_slot_range)?;

    let since = resolve_time_range_since(&params.common.time_range);
    let rows = match state
        .client
        .get_prove_costs_paginated(since, limit, params.starting_after, params.ending_before)
        .await
    {
        Ok(r) => r,
        Err(e) => return Err(query_error("prove cost", e)),
    };
    let rows: Vec<ProveCostRow> = rows
        .into_iter()
        .map(|r| ProveCostRow {
            l1_block_number: r.l1_block_number,
            batch_id: r.batch_id,
            cost: wei_to_gwei(r.cost),
        })
        .collect();
    tracing::info!(count = rows.len(), "Returning prove cost");
    Ok(Json(ProveCostResponse { batches: rows }))
}

// removed: block_profits endpoint (unused by dashboard)

#[utoipa::path(
    get,
    path = "/eth-price",
    responses(
        (status = 200, description = "Current ETH price", body = EthPriceResponse),
        (status = 503, description = "Price fetch error", body = ErrorResponse)
    ),
    tag = "taikoscope"
)]
/// Get the current ETH price in USD
pub async fn eth_price(
    State(state): State<ApiState>,
) -> Result<Json<EthPriceResponse>, ErrorResponse> {
    match state.eth_price().await {
        Ok(price) => Ok(Json(EthPriceResponse { price })),
        Err(e) => {
            tracing::error!(error = %e, "Failed to fetch ETH price");
            Err(ErrorResponse::new(
                "price-error",
                "Failed to fetch ETH price",
                StatusCode::SERVICE_UNAVAILABLE,
                e.to_string(),
            ))
        }
    }
}

// Removed legacy l2_fees and l2_fee_components endpoints (use l2_fees_components)

#[utoipa::path(
    get,
    path = "/l2-fees-components",
    params(
        RangeQuery
    ),
    responses(
        (status = 200, description = "Combined L2 fees and batch components", body = L2FeesComponentsResponse),
        (status = 500, description = "Database error", body = ErrorResponse)
    ),
    tag = "taikoscope"
)]
/// Get combined L2 fees summary and detailed batch components for all sequencers
pub async fn l2_fees_components(
    Query(params): Query<RangeQuery>,
    State(state): State<ApiState>,
) -> Result<Json<L2FeesComponentsResponse>, ErrorResponse> {
    validate_time_range(&params.time_range)?;

    let has_time_range = has_time_range_params(&params.time_range);
    validate_range_exclusivity(has_time_range, false)?;

    let time_range = resolve_time_range_enum(&params.time_range);

    let (sequencer_fees, batch_components) = state
        .client
        .get_l2_fees_and_components(None, time_range)
        .await
        .map_err(|e| query_error("L2 fees and components", e))?;

    // Calculate aggregated totals from sequencer fees
    let priority_fee = sequencer_fees.iter().map(|s| s.priority_fee).sum::<u128>();
    let base_fee = sequencer_fees.iter().map(|s| s.base_fee).sum::<u128>();
    let l1_data_cost = sequencer_fees.iter().map(|s| s.l1_data_cost).sum::<u128>();
    let prove_cost = sequencer_fees.iter().map(|s| s.prove_cost).sum::<u128>();

    // Convert sequencer fees to gwei
    let sequencers: Vec<SequencerFeeRow> = sequencer_fees
        .into_iter()
        .map(|s| SequencerFeeRow {
            address: format_address(s.sequencer),
            priority_fee: wei_to_gwei(s.priority_fee),
            base_fee: wei_to_gwei(s.base_fee),
            l1_data_cost: wei_to_gwei(s.l1_data_cost),
            prove_cost: wei_to_gwei(s.prove_cost),
        })
        .collect();

    // Convert batch components to gwei
    let batches: Vec<BatchFeeComponentRow> = batch_components
        .into_iter()
        .map(|r| BatchFeeComponentRow {
            batch_id: r.batch_id,
            l1_block_number: r.l1_block_number,
            l1_tx_hash: B256::from(r.l1_tx_hash).to_string(),
            sequencer: format_address(r.sequencer),
            priority_fee: wei_to_gwei(r.priority_fee),
            base_fee: wei_to_gwei(r.base_fee),
            l1_data_cost: wei_to_gwei_opt(r.l1_data_cost),
            prove_cost: wei_to_gwei_opt(r.prove_cost),
        })
        .collect();

    Ok(Json(L2FeesComponentsResponse {
        priority_fee: (priority_fee > 0).then_some(wei_to_gwei(priority_fee)),
        base_fee: (base_fee > 0).then_some(wei_to_gwei(base_fee)),
        l1_data_cost: wei_to_gwei(l1_data_cost),
        prove_cost: wei_to_gwei(prove_cost),
        sequencers,
        batches,
    }))
}
