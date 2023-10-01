use std::fmt;
use std::hash::Hash;

use bitcoin_hashes::sha256;
use config::OddsMarketsClientConfig;
use fedimint_core::core::{Decoder, ModuleInstanceId, ModuleKind};
use fedimint_core::db::DatabaseValue;
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::module::{CommonModuleInit, ModuleCommon, ModuleConsensusVersion};
use fedimint_core::{plugin_types_trait_impl_common, Amount, OutPoint};
use secp256k1::schnorr::Signature;
use secp256k1::{Message, Secp256k1, XOnlyPublicKey};
use serde::{Deserialize, Serialize};
use thiserror::Error;

// Common contains types shared by both the client and server

// The client and server configuration
pub mod config;

/// Unique name for this module
pub const KIND: ModuleKind = ModuleKind::from_static_str("odds-markets");

/// Modules are non-compatible with older versions
pub const CONSENSUS_VERSION: ModuleConsensusVersion = ModuleConsensusVersion(0);

/// Non-transaction items that will be submitted to consensus
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, Encodable, Decodable)]
pub enum OddsMarketsConsensusItem {}

/// Input for a fedimint transaction
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize, Encodable, Decodable)]
pub enum OddsMarketsInput {
    ConsumeOrderFreeBalance(),
    CancelOrder(),
}

/// Output for a fedimint transaction
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize, Encodable, Decodable)]
pub enum OddsMarketsOutput {
    NewMarket(Market),
    PayoutMarket(Payout, Signature),
    NewOrder(),
}

/// Information needed by a client to update output funds
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize, Encodable, Decodable)]
pub enum OddsMarketsOutputOutcome {
    NewMarket,
    EndMarket,
    NewOrder(),
}

/// Errors that might be returned by the server
// TODO: Move to server lib?
#[derive(Debug, Clone, Eq, PartialEq, Hash, Error)]
pub enum OddsMarketsError {
    #[error("Not enough funds")]
    NotEnoughFunds,

    #[error("New market does not pass server validation")]
    FailedNewMarketValidation,

    #[error("The market does not exist")]
    MarketDoesNotExist,

    #[error("The payout failed validation")]
    FailedPayoutValidation,

    #[error("A payout already exists for this market")]
    PayoutAlreadyExists,
}

/// Contains the types defined above
pub struct OddsMarketsModuleTypes;

// Wire together the types for this module
plugin_types_trait_impl_common!(
    OddsMarketsModuleTypes,
    OddsMarketsClientConfig,
    OddsMarketsInput,
    OddsMarketsOutput,
    OddsMarketsOutputOutcome,
    OddsMarketsConsensusItem
);

#[derive(Debug)]
pub struct OddsMarketsCommonGen;

impl CommonModuleInit for OddsMarketsCommonGen {
    const CONSENSUS_VERSION: ModuleConsensusVersion = CONSENSUS_VERSION;
    const KIND: ModuleKind = KIND;

    type ClientConfig = OddsMarketsClientConfig;

    fn decoder() -> Decoder {
        OddsMarketsModuleTypes::decoder_builder().build()
    }
}

impl fmt::Display for OddsMarketsClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OddsMarketsClientConfig")
    }
}
impl fmt::Display for OddsMarketsInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OddsMarketsInput")
    }
}

impl fmt::Display for OddsMarketsOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OddsMarketsOutput")
    }
}

impl fmt::Display for OddsMarketsOutputOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OddsMarketsOutputOutcome")
    }
}

impl fmt::Display for OddsMarketsConsensusItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OddsMarketsConsensusItem")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct Market {
    pub contract_value: Amount,
    pub outcome_control: XOnlyPublicKey,
    pub description: MarketDescription,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct MarketDescription {
    pub title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct Payout {
    pub market: OutPoint,
    pub positive_contract_payout: Amount,
}

impl Payout {
    pub fn verify_schnorr(
        &self,
        pubkey: &XOnlyPublicKey,
        sig: &Signature,
    ) -> Result<(), secp256k1::Error> {
        let secp256k1 = Secp256k1::new();
        let msg = Message::from_hashed_data::<sha256::Hash>(self.to_bytes().as_slice());

        secp256k1.verify_schnorr(sig, &msg, pubkey)
    }
}
