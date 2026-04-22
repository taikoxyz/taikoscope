//! Preconfirmation data processing functionality

use clickhouse::ClickhouseWriter;
use extractor::Extractor;
use tracing::{error, info, warn};

/// Process preconfirmation data for L1 headers
pub async fn process_preconf_data(
    extractor: &Extractor,
    clickhouse_writer: &Option<ClickhouseWriter>,
    header: &primitives::headers::L1Header,
    enable_db_writes: bool,
) {
    let writer = match clickhouse_writer {
        Some(w) => w,
        None => {
            // When database writes disabled, we still want to validate the preconf data logic
            if enable_db_writes {
                return;
            }
            info!(
                block_number = header.number,
                "🧪 DRY-RUN: Validating preconf data processing without database writes"
            );
            // Continue validation but skip database writes
            return process_preconf_data_dry_run(extractor, header).await;
        }
    };

    // Get current operator for epoch
    let opt_current_operator = match extractor.get_operator_for_current_epoch().await {
        Ok(op) => {
            info!(
                block = header.number,
                operator = ?op,
                "Current operator for epoch"
            );
            Some(op)
        }
        Err(e) => {
            error!(block = header.number, err = %e, "get_operator_for_current_epoch failed");
            None
        }
    };

    // Get next operator for epoch
    let opt_next_operator = match extractor.get_operator_for_next_epoch().await {
        Ok(op) => {
            info!(
                block = header.number,
                operator = ?op,
                "Next operator for epoch"
            );
            Some(op)
        }
        Err(e) => {
            error!(block = header.number, err = %e, "get_operator_for_next_epoch failed");
            None
        }
    };

    // Insert preconf data if we have at least one operator
    if opt_current_operator.is_some() || opt_next_operator.is_some() {
        if let Err(e) =
            writer.insert_preconf_data(header.slot, opt_current_operator, opt_next_operator).await
        {
            error!(slot = header.slot, err = %e, "Failed to insert preconf data");
        } else {
            info!(slot = header.slot, "Inserted preconf data for slot");
        }
    } else {
        info!(
            slot = header.slot,
            "Skipping preconf data insertion due to errors fetching operator data"
        );
    }
}

/// Process preconfirmation data in dry-run mode
pub async fn process_preconf_data_dry_run(
    extractor: &Extractor,
    header: &primitives::headers::L1Header,
) {
    // Get current operator for epoch (for validation)
    let opt_current_operator = match extractor.get_operator_for_current_epoch().await {
        Ok(op) => {
            info!(
                block = header.number,
                operator = ?op,
                "🧪 DRY-RUN: Current operator for epoch"
            );
            Some(op)
        }
        Err(e) => {
            warn!(block = header.number, err = %e, "🧪 DRY-RUN: get_operator_for_current_epoch failed");
            None
        }
    };

    // Get next operator for epoch (for validation)
    let opt_next_operator = match extractor.get_operator_for_next_epoch().await {
        Ok(op) => {
            info!(
                block = header.number,
                operator = ?op,
                "🧪 DRY-RUN: Next operator for epoch"
            );
            Some(op)
        }
        Err(e) => {
            warn!(block = header.number, err = %e, "🧪 DRY-RUN: get_operator_for_next_epoch failed");
            None
        }
    };

    // Simulate database insertion
    if opt_current_operator.is_some() || opt_next_operator.is_some() {
        info!(
            slot = header.slot,
            has_current_op = opt_current_operator.is_some(),
            has_next_op = opt_next_operator.is_some(),
            "🧪 DRY-RUN: Would insert preconf data"
        );
    } else {
        info!(
            slot = header.slot,
            "🧪 DRY-RUN: Would skip preconf data insertion due to missing operators"
        );
    }
}
