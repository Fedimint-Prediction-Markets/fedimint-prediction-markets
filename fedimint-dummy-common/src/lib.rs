use std::fmt;
use std::hash::Hash;

use bitcoin_hashes::sha256;
use config::PredictionMarketsClientConfig;
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
pub const KIND: ModuleKind = ModuleKind::from_static_str("prediction-markets");

/// Modules are non-compatible with older versions
pub const CONSENSUS_VERSION: ModuleConsensusVersion = ModuleConsensusVersion(0);

/// Non-transaction items that will be submitted to consensus
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, Encodable, Decodable)]
pub enum PredictionMarketsConsensusItem {}

/// Input for a fedimint transaction
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize, Encodable, Decodable)]
pub enum PredictionMarketsInput {
    ConsumeOrderFreeBalance(),
    CancelOrder {
        order: OutPoint
    },
}

/// Output for a fedimint transaction
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize, Encodable, Decodable)]
pub enum PredictionMarketsOutput {
    NewMarket(Market),
    NewOrder {
        owner: XOnlyPublicKey,
        market_outpoint: OutPoint,
        outcome: u8,
        side: Side,
        price: Amount,
        quantity: u64,
    },
    PayoutMarket(Payout, Signature),
}

/// Information needed by a client to update output funds
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize, Encodable, Decodable)]
pub enum PredictionMarketsOutputOutcome {
    NewMarket,
    NewOrder,
    PayoutMarket,
}

/// Errors that might be returned by the server
// TODO: Move to server lib?
#[derive(Debug, Clone, Eq, PartialEq, Hash, Error)]
pub enum PredictionMarketsError {
    #[error("Not enough funds")]
    NotEnoughFunds,

    #[error("New market does not pass server validation")]
    FailedNewMarketValidation,

    #[error("The market does not exist")]
    MarketDoesNotExist,

    #[error("Order validation failed")]
    FailedNewOrderValidation,

    #[error("The payout failed validation")]
    FailedPayoutValidation,

    #[error("A payout already exists for this market")]
    PayoutAlreadyExists,
}

/// Contains the types defined above
pub struct PredictionMarketsModuleTypes;

// Wire together the types for this module
plugin_types_trait_impl_common!(
    PredictionMarketsModuleTypes,
    PredictionMarketsClientConfig,
    PredictionMarketsInput,
    PredictionMarketsOutput,
    PredictionMarketsOutputOutcome,
    PredictionMarketsConsensusItem
);

#[derive(Debug)]
pub struct PredictionMarketsCommonGen;

impl CommonModuleInit for PredictionMarketsCommonGen {
    const CONSENSUS_VERSION: ModuleConsensusVersion = CONSENSUS_VERSION;
    const KIND: ModuleKind = KIND;

    type ClientConfig = PredictionMarketsClientConfig;

    fn decoder() -> Decoder {
        PredictionMarketsModuleTypes::decoder_builder().build()
    }
}

impl fmt::Display for PredictionMarketsClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PredictionMarketsClientConfig")
    }
}
impl fmt::Display for PredictionMarketsInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PredictionMarketsInput")
    }
}

impl fmt::Display for PredictionMarketsOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PredictionMarketsOutput")
    }
}

impl fmt::Display for PredictionMarketsOutputOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PredictionMarketsOutputOutcome")
    }
}

impl fmt::Display for PredictionMarketsConsensusItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PredictionMarketsConsensusItem")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct Market {
    pub contract_price: Amount,
    pub outcomes: u8,

    pub outcome_control: XOnlyPublicKey,
    pub description: MarketDescription,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct MarketDescription {
    pub title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct Order {
    // static
    pub owner: XOnlyPublicKey,
    pub outcome: u8,
    pub side: Side,
    pub price: Amount,
    pub time_priority: u64,

    // mutated
    pub quantity_remaining: u64,
    pub quantity_balance: u64,
    pub btc_balance: Amount,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct Payout {
    pub market: OutPoint,
    pub outcome_payouts: Vec<Amount>,
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
