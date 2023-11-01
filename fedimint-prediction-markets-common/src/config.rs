use fedimint_core::core::ModuleKind;
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::{plugin_types_trait_impl_config, Amount};
use serde::{Deserialize, Serialize};

use crate::{ContractOfOutcomeAmount, Outcome, PredictionMarketsCommonGen};

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
    // market
    pub new_market_fee: Amount,
    pub max_contract_price: Amount,
    pub max_market_outcomes: Outcome,

    // orders
    pub new_order_fee: Amount,
    pub max_order_quantity: ContractOfOutcomeAmount,

    // data creation
    pub timestamp_interval_seconds: u64,
}

impl Default for PredictionMarketsGenParams {
    fn default() -> Self {
        Self {
            local: PredictionMarketsGenParamsLocal {},
            consensus: PredictionMarketsGenParamsConsensus {
                new_market_fee: Amount::from_sats(100),
                max_contract_price: Amount::from_sats(100_000_000),
                max_market_outcomes: 100,

                new_order_fee: Amount::from_sats(1),
                max_order_quantity: ContractOfOutcomeAmount(1000000),

                timestamp_interval_seconds: 15,
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
    pub max_contract_price: Amount,
    pub max_market_outcomes: Outcome,

    pub new_order_fee: Amount,
    pub max_order_quantity: ContractOfOutcomeAmount,
}

/// Locally unencrypted config unique to each member
#[derive(Clone, Debug, Serialize, Deserialize, Decodable, Encodable)]
pub struct PredictionMarketsConfigLocal {
    pub peer_count: u16,
}

/// Will be the same for every federation member
#[derive(Clone, Debug, Serialize, Deserialize, Decodable, Encodable)]
pub struct PredictionMarketsConfigConsensus {
    // market
    pub new_market_fee: Amount,
    pub max_contract_price: Amount,
    pub max_market_outcomes: Outcome,

    // orders
    pub new_order_fee: Amount,
    pub max_order_quantity: ContractOfOutcomeAmount,

    // data creation
    pub timestamp_interval_seconds: u64,
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
