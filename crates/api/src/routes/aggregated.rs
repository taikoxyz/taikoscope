//! Aggregated data endpoints with complex processing

use crate::{
    helpers::{format_address, parse_optional_address, query_error, wei_to_gwei},
    state::ApiState,
    validation::{
        CommonQuery, has_time_range_params, resolve_time_range_enum, resolve_time_range_since,
        validate_range_exclusivity, validate_time_range,
    },
};
use api_types::*;
use axum::{
    Json,
    extract::{Query, State},
};

// Legacy type aliases for backward compatibility
type RangeQuery = CommonQuery;

#[utoipa::path(
    get,
    path = "/prove-costs",
    params(
        RangeQuery
    ),
    responses(
        (status = 200, description = "Aggregated prover costs", body = ProposerCostsResponse),
        (status = 500, description = "Database error", body = ErrorResponse)
    ),
    tag = "taikoscope"
)]
/// Get aggregated prover costs grouped by proposer
pub async fn prove_costs(
    Query(params): Query<RangeQuery>,
    State(state): State<ApiState>,
) -> Result<Json<ProposerCostsResponse>, ErrorResponse> {
    validate_time_range(&params.time_range)?;

    let has_time_range = has_time_range_params(&params.time_range);
    validate_range_exclusivity(has_time_range, false)?;

    let time_range = resolve_time_range_enum(&params.time_range);

    let rows = state
        .client
        .get_prove_costs_by_proposer(time_range)
        .await
        .map_err(|e| query_error("prover costs", e))?;

    let proposers: Vec<ProposerCostItem> = rows
        .into_iter()
        .map(|(addr, cost)| ProposerCostItem {
            address: format_address(addr),
            cost: wei_to_gwei(cost),
        })
        .collect();

    tracing::info!(count = proposers.len(), "Returning prover costs");
    Ok(Json(ProposerCostsResponse { proposers }))
}

#[utoipa::path(
    get,
    path = "/dashboard-data",
    params(
        RangeQuery
    ),
    responses(
        (status = 200, description = "Aggregated dashboard data", body = DashboardDataResponse),
        (status = 500, description = "Database error", body = ErrorResponse)
    ),
    tag = "taikoscope"
)]
/// Get comprehensive dashboard data including metrics, block info, and operational statistics
pub async fn dashboard_data(
    Query(params): Query<RangeQuery>,
    State(state): State<ApiState>,
) -> Result<Json<DashboardDataResponse>, ErrorResponse> {
    validate_time_range(&params.time_range)?;

    let has_time_range = has_time_range_params(&params.time_range);
    validate_range_exclusivity(has_time_range, false)?;

    let time_range = resolve_time_range_enum(&params.time_range);
    let since = resolve_time_range_since(&params.time_range);
    let address = parse_optional_address(params.address.as_ref()).ok().flatten();

    let (
        l2_block_cadence,
        batch_posting_cadence,
        avg_prove_time,
        avg_verify_time,
        avg_tps,
        preconf,
        reorgs,
        slashings,
        forced_inclusions,
        failed_proposals,
        l2_head_block,
        l1_head_block,
    ) = tokio::try_join!(
        state.client.get_l2_block_cadence(address, time_range),
        state.client.get_batch_posting_cadence(time_range),
        state.client.get_avg_prove_time(time_range),
        state.client.get_avg_verify_time(time_range),
        state.client.get_avg_l2_tps(address, time_range),
        state.client.get_last_preconf_data(),
        state.client.get_l2_reorgs_since(since),
        state.client.get_slashing_events_since(since),
        state.client.get_forced_inclusions_since(since),
        state.client.get_failed_proposals_since(since),
        state.client.get_last_l2_block_number(),
        state.client.get_last_l1_block_number()
    )
    .map_err(|e| {
        tracing::error!(error = %e, "Failed to get dashboard data");
        ErrorResponse::database_error()
    })?;

    let preconf_data = preconf.map(|d| PreconfDataResponse {
        candidates: d.candidates.into_iter().map(format_address).collect(),
        current_operator: d.current_operator.map(format_address),
        next_operator: d.next_operator.map(format_address),
    });

    tracing::info!(
        l2_head_block,
        l1_head_block,
        reorgs = reorgs.len(),
        slashings = slashings.len(),
        forced_inclusions = forced_inclusions.len(),
        failed_proposals = failed_proposals.len(),
        "Returning dashboard data"
    );

    Ok(Json(DashboardDataResponse {
        l2_block_cadence_ms: l2_block_cadence,
        batch_posting_cadence_ms: batch_posting_cadence,
        avg_prove_time_ms: avg_prove_time,
        avg_verify_time_ms: avg_verify_time,
        avg_tps,
        preconf_data,
        l2_reorgs: reorgs.len(),
        slashings: slashings.len(),
        forced_inclusions: forced_inclusions.len(),
        failed_proposals: failed_proposals.len(),
        l2_head_block,
        l1_head_block,
    }))
}
