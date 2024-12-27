use fedimint_core::core::ModuleKind;
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::{plugin_types_trait_impl_config, Amount};
use prediction_market_event::information::Information;
use serde::{Deserialize, Serialize};

use crate::{ContractOfOutcomeAmount, Outcome, PredictionMarketsCommonInit, Seconds};

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
    pub gc: GeneralConsensus,
}

impl Default for PredictionMarketsGenParams {
    fn default() -> Self {
        Self {
            local: PredictionMarketsGenParamsLocal {},
            consensus: PredictionMarketsGenParamsConsensus {
                gc: GeneralConsensus {
                    // fees
                    new_market_fee: Amount::from_msats(0),
                    new_order_fee: Amount::from_msats(0),
                    consume_order_bitcoin_balance_fee: Amount::from_msats(0),

                    // markets
                    accepted_event_information_variant_ids: Information::ALL_VARIANT_IDS
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                    max_contract_price: Amount::from_sats(100_000_000),
                    max_market_outcomes: 50,
                    max_payout_control_keys: 25,

                    // orders
                    max_order_quantity: ContractOfOutcomeAmount(1000000),
                    max_sell_order_sources: 50,

                    // timestamp creation
                    timestamp_interval: 15,

                    // match data
                    candlestick_intervals: vec![
                        60 * 60 * 24,
                        60 * 60 * 4,
                        60 * 60,
                        60 * 15,
                        60 * 5,
                        60,
                        15,
                    ],
                    max_candlesticks_kept_per_market_outcome_interval: 500,

                    // order book data
                    order_book_precision: 100,
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
    pub gc: GeneralConsensus,
}

/// Locally unencrypted config unique to each member
#[derive(Clone, Debug, Serialize, Deserialize, Decodable, Encodable)]
pub struct PredictionMarketsConfigLocal {
    pub peer_count: u16,
}

/// Will be the same for every federation member
#[derive(Clone, Debug, Serialize, Deserialize, Decodable, Encodable)]
pub struct PredictionMarketsConfigConsensus {
    pub gc: GeneralConsensus,
}

/// Will be encrypted and not shared such as private key material
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PredictionMarketsConfigPrivate {
    // If private config is empty, fedimintd does not run
    pub example: String,
}

// Wire together the configs for this module
plugin_types_trait_impl_config!(
    PredictionMarketsCommonInit,
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
    pub consume_order_bitcoin_balance_fee: Amount,

    // markets
    pub accepted_event_information_variant_ids: Vec<String>,
    pub max_contract_price: Amount,
    pub max_market_outcomes: Outcome,
    pub max_payout_control_keys: u16,

    // orders
    pub max_order_quantity: ContractOfOutcomeAmount,
    pub max_sell_order_sources: u16,

    // timestamp creation
    pub timestamp_interval: Seconds,

    // match data
    pub candlestick_intervals: Vec<Seconds>,
    pub max_candlesticks_kept_per_market_outcome_interval: u64,

    // order book data
    pub order_book_precision: u64,
}
