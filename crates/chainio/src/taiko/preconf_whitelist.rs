//! Taiko preconf whitelist contract
use IPreconfWhitelist::{IPreconfWhitelistErrors, IPreconfWhitelistInstance};
use alloy::contract::{Error as ContractError, Result as ContractResult};
use alloy_primitives::Address;
use alloy_sol_macro::sol;
use alloy_sol_types::{Error as SolError, SolInterface};

use crate::DefaultProvider;

/// A UNIX timestamp in seconds.
pub type Timestamp = u64;

/// A wrapper over a `IPreconfWhitelist` contract that exposes various utility methods.
#[derive(Debug, Clone)]
pub struct TaikoPreconfWhitelist(IPreconfWhitelistInstance<DefaultProvider>);

impl TaikoPreconfWhitelist {
    /// Create a new `TaikoPreconfWhitelist` instance over an existing WS-based provider.
    pub const fn new_readonly(address: Address, provider: DefaultProvider) -> Self {
        Self(IPreconfWhitelistInstance::new(address, provider))
    }

    /// Get the operator for the current epoch.
    pub async fn get_operator_for_current_epoch(&self) -> ContractResult<Address> {
        match self.0.getOperatorForCurrentEpoch().call().await {
            Ok(result) => Ok(result),
            Err(err) => {
                let decoded_error = try_parse_contract_error::<IPreconfWhitelistErrors>(err)?;
                Err(SolError::custom(format!("{decoded_error:?}")).into())
            }
        }
    }

    /// Get the operator for the next epoch.
    pub async fn get_operator_for_next_epoch(&self) -> ContractResult<Address> {
        match self.0.getOperatorForNextEpoch().call().await {
            Ok(result) => Ok(result),
            Err(err) => {
                let decoded_error = try_parse_contract_error::<IPreconfWhitelistErrors>(err)?;
                Err(SolError::custom(format!("{decoded_error:?}")).into())
            }
        }
    }

    /// Check if an address is active in the whitelist for a given epoch.
    ///
    /// Note: "active" in the contract just means that the operator is able to be selected as the
    /// sequencer, not that it is currently the sequencer.
    pub async fn is_whitelisted(
        &self,
        address: Address,
        epoch_timestamp: Timestamp,
    ) -> ContractResult<bool> {
        match self.0.isOperatorActive(address, epoch_timestamp as u32).call().await {
            Ok(result) => Ok(result),
            Err(err) => {
                // Patch: handle the old whitelist contract implementation
                if err.to_string().contains("execution reverted") {
                    self.0.isOperator(address).call().await
                } else {
                    let decoded_error = try_parse_contract_error::<IPreconfWhitelistErrors>(err)?;
                    Err(SolError::custom(format!("{decoded_error:?}")).into())
                }
            }
        }
    }
}

sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    #[derive(Debug)]
    interface IPreconfWhitelist {
        error InvalidOperatorIndex();
        error InvalidOperatorCount();
        error InvalidOperatorAddress();
        error OperatorAlreadyExists();
        error OperatorNotAvailableYet();

        /// @notice Adds a new operator to the whitelist.
        /// @param _operatorAddress The address of the operator to be added.
        /// @dev Only callable by the owner or an authorized address.
        function addOperator(address _operatorAddress) external;

        /// @notice Removes an operator from the whitelist.
        /// @param _operatorId The ID of the operator to be removed.
        /// @dev Only callable by the owner or an authorized address.
        /// @dev Reverts if the operator ID does not exist.
        function removeOperator(uint256 _operatorId) external;

        /// @notice Retrieves the address of the operator for the current epoch.
        /// @dev Uses the beacon block root of the first block in the last epoch as the source
        ///      of randomness.
        /// @return The address of the operator.
        function getOperatorForCurrentEpoch() external view returns (address operator);

        /// @notice Retrieves the address of the operator for the next epoch.
        /// @dev Uses the beacon block root of the first block in the current epoch as the source
        ///      of randomness.
        /// @return The address of the operator.
        function getOperatorForNextEpoch() external view returns (address operator);

        /// @notice Check if an address is an active operator in the whitelist.
        /// @param operator The address to check.
        /// @param epochTimestamp The timestamp of the epoch to check.
        /// @return True if the address is an active operator in the given epoch.
        function isOperatorActive(address operator, uint32 epochTimestamp) external view returns (bool);

        /// @notice The OLD whitelist method for checking if an address is whitelisted.
        /// @dev This is kept for backwards compatibility.
        function isOperator(address operator) external view returns (bool);
    }
}

/// Try to parse a contract error as a specific interface error.
pub fn try_parse_contract_error<I: SolInterface>(error: ContractError) -> Result<I, ContractError> {
    error.as_decoded_interface_error::<I>().ok_or(error)
}
