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
    pub general_consensus: GeneralConsensus,
}

impl Default for PredictionMarketsGenParams {
    fn default() -> Self {
        Self {
            local: PredictionMarketsGenParamsLocal {},
            consensus: PredictionMarketsGenParamsConsensus {
                general_consensus: GeneralConsensus {
                    // fees
                    new_market_fee: Amount::from_sats(100),
                    new_order_fee: Amount::from_sats(1),
                    consumer_order_bitcoin_balance_fee: Amount::from_msats(100),
                    payout_proposal_fee: Amount::from_sats(1),

                    // markets
                    max_contract_price: Amount::from_sats(100_000_000),
                    max_market_outcomes: 30,
                    max_outcome_control_keys: 25,

                    // orders
                    max_order_quantity: ContractOfOutcomeAmount(1000000),

                    // timestamp creation
                    timestamp_interval_seconds: 15,

                    // match data
                    candlestick_intervals_seconds: vec![
                        15,
                        60,
                        60 * 5,
                        60 * 15,
                        60 * 60,
                        60 * 60 * 4,
                        60 * 60 * 24,
                    ],
                },
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
    pub general_consensus: GeneralConsensus,
}

/// Locally unencrypted config unique to each member
#[derive(Clone, Debug, Serialize, Deserialize, Decodable, Encodable)]
pub struct PredictionMarketsConfigLocal {
    pub peer_count: u16,
}

/// Will be the same for every federation member
#[derive(Clone, Debug, Serialize, Deserialize, Decodable, Encodable)]
pub struct PredictionMarketsConfigConsensus {
    pub general_consensus: GeneralConsensus,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Encodable, Decodable, Hash)]
pub struct GeneralConsensus {
    // fees
    pub new_market_fee: Amount,
    pub new_order_fee: Amount,
    pub consumer_order_bitcoin_balance_fee: Amount,
    pub payout_proposal_fee: Amount,

    // markets
    pub max_contract_price: Amount,
    pub max_market_outcomes: Outcome,
    pub max_outcome_control_keys: u16,

    // orders
    pub max_order_quantity: ContractOfOutcomeAmount,

    // timestamp creation
    pub timestamp_interval_seconds: u64,

    // match data
    pub candlestick_intervals_seconds: Vec<u64>,
}
