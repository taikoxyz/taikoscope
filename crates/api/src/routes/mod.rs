//! API route definitions

pub mod aggregated;
pub mod core;
pub mod table;

use crate::{ApiDoc, state::ApiState};
use axum::{Router, routing::get};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use aggregated::{dashboard_data, prove_costs};
use core::*;
use table::*;

/// Build the router with all API endpoints.
pub fn router(state: ApiState) -> Router {
    let api_routes = Router::new()
        .route("/l2-head-block", get(l2_head_block))
        .route("/l1-head-block", get(l1_head_block))
        .route("/preconf-data", get(preconf_data))
        .route("/reorgs", get(reorgs))
        .route("/slashings", get(slashings))
        .route("/forced-inclusions", get(forced_inclusions))
        .route("/failed-proposals", get(failed_proposals))
        .route("/batch-posting-times", get(batch_posting_times))
        .route("/blobs-per-batch", get(blobs_per_batch))
        .route("/prove-times", get(prove_times))
        .route("/verify-times", get(verify_times))
        .route("/l1-block-times", get(l1_block_times))
        .route("/l2-block-times", get(l2_block_times))
        .route("/l2-gas-used", get(l2_gas_used))
        .route("/l2-tps", get(l2_tps))
        .route("/sequencer-distribution", get(sequencer_distribution))
        .route("/sequencer-blocks", get(sequencer_blocks))
        .route("/block-transactions", get(block_transactions))
        // Removed legacy /l2-fees and /l2-fee-components endpoints (use /l2-fees-components
        // instead)
        .route("/l2-fees-components", get(l2_fees_components))
        .route("/dashboard-data", get(dashboard_data))
        .route("/l1-data-cost", get(l1_data_cost))
        .route("/prove-costs", get(prove_costs))
        .route("/prove-cost", get(prove_cost))
        .route("/eth-price", get(eth_price));

    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-doc/openapi.json", ApiDoc::openapi()))
        .merge(api_routes)
        .with_state(state)
}
