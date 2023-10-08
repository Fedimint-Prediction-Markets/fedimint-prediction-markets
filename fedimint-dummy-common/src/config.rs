use fedimint_core::core::ModuleKind;
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::{plugin_types_trait_impl_config, Amount};
use serde::{Deserialize, Serialize};

use crate::OddsMarketsCommonGen;

/// Parameters necessary to generate this module's configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OddsMarketsGenParams {
    pub local: OddsMarketsGenParamsLocal,
    pub consensus: OddsMarketsGenParamsConsensus,
}

/// Local parameters for config generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OddsMarketsGenParamsLocal {}

/// Consensus parameters for config generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OddsMarketsGenParamsConsensus {
    pub new_market_fee: Amount,
    pub max_contract_value: Amount,
}

impl Default for OddsMarketsGenParams {
    fn default() -> Self {
        Self {
            local: OddsMarketsGenParamsLocal {},
            consensus: OddsMarketsGenParamsConsensus {
                new_market_fee: Amount::from_sats(1),
                max_contract_value: Amount::from_sats(100_000_000),
            },
        }
    }
}

/// Contains all the configuration for the server
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OddsMarketsConfig {
    pub local: OddsMarketsConfigLocal,
    pub private: OddsMarketsConfigPrivate,
    pub consensus: OddsMarketsConfigConsensus,
}

/// Contains all the configuration for the client
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Encodable, Decodable, Hash)]
pub struct OddsMarketsClientConfig {
    pub new_market_fee: Amount,
}

/// Locally unencrypted config unique to each member
#[derive(Clone, Debug, Serialize, Deserialize, Decodable, Encodable)]
pub struct OddsMarketsConfigLocal {
    pub examples: String,
}

/// Will be the same for every federation member
#[derive(Clone, Debug, Serialize, Deserialize, Decodable, Encodable)]
pub struct OddsMarketsConfigConsensus {
    pub new_market_fee: Amount,
    pub max_contract_value: Amount,
}

/// Will be encrypted and not shared such as private key material
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OddsMarketsConfigPrivate {
    pub example: String,
}

// Wire together the configs for this module
plugin_types_trait_impl_config!(
    OddsMarketsCommonGen,
    OddsMarketsGenParams,
    OddsMarketsGenParamsLocal,
    OddsMarketsGenParamsConsensus,
    OddsMarketsConfig,
    OddsMarketsConfigLocal,
    OddsMarketsConfigPrivate,
    OddsMarketsConfigConsensus,
    OddsMarketsClientConfig
);
