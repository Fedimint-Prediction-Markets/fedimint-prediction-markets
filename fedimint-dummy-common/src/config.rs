use fedimint_core::core::ModuleKind;
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::{plugin_types_trait_impl_config, Amount};
use serde::{Deserialize, Serialize};

use crate::{PredictionMarketsCommonGen, ContractAmount, Outcome};

/// Parameters necessary to generate this module's configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictionMarketsGenParams {
    pub local: PredictionMarketsGenParamsLocal,
    pub consensus: PredictionMarketsGenParamsConsensus,
}

/// Local parameters for config generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictionMarketsGenParamsLocal {}

/// Consensus parameters for config generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictionMarketsGenParamsConsensus {
    pub new_market_fee: Amount,
    pub new_order_fee: Amount,
    pub max_contract_value: Amount,
    pub max_order_quantity: ContractAmount,
    pub max_market_outcomes: Outcome
}

impl Default for PredictionMarketsGenParams {
    fn default() -> Self {
        Self {
            local: PredictionMarketsGenParamsLocal {},
            consensus: PredictionMarketsGenParamsConsensus {
                new_market_fee: Amount::from_sats(100),
                new_order_fee: Amount::from_sats(1),
                max_contract_value: Amount::from_sats(100_000_000),
                max_order_quantity: ContractAmount(1000000),
                max_market_outcomes: 100
            },
        }
    }
}

/// Contains all the configuration for the server
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PredictionMarketsConfig {
    pub local: PredictionMarketsConfigLocal,
    pub private: PredictionMarketsConfigPrivate,
    pub consensus: PredictionMarketsConfigConsensus,
}

/// Contains all the configuration for the client
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Encodable, Decodable, Hash)]
pub struct PredictionMarketsClientConfig {
    pub new_market_fee: Amount,
    pub new_order_fee: Amount,
    pub max_contract_value: Amount,
    pub max_order_quantity: ContractAmount,
    pub max_market_outcomes: Outcome
}

/// Locally unencrypted config unique to each member
#[derive(Clone, Debug, Serialize, Deserialize, Decodable, Encodable)]
pub struct PredictionMarketsConfigLocal {
    pub examples: String,
}

/// Will be the same for every federation member
#[derive(Clone, Debug, Serialize, Deserialize, Decodable, Encodable)]
pub struct PredictionMarketsConfigConsensus {
    pub new_market_fee: Amount,
    pub new_order_fee: Amount,
    pub max_contract_value: Amount,
    pub max_order_quantity: ContractAmount,
    pub max_market_outcomes: Outcome
}

/// Will be encrypted and not shared such as private key material
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PredictionMarketsConfigPrivate {
    pub example: String,
}

// Wire together the configs for this module
plugin_types_trait_impl_config!(
    PredictionMarketsCommonGen,
    PredictionMarketsGenParams,
    PredictionMarketsGenParamsLocal,
    PredictionMarketsGenParamsConsensus,
    PredictionMarketsConfig,
    PredictionMarketsConfigLocal,
    PredictionMarketsConfigPrivate,
    PredictionMarketsConfigConsensus,
    PredictionMarketsClientConfig
);
