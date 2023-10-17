use std::fmt;
use std::hash::Hash;
use std::ops::{Add, Sub};

use config::PredictionMarketsClientConfig;
use fedimint_core::core::{Decoder, ModuleInstanceId, ModuleKind};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::module::{CommonModuleInit, ModuleCommon, ModuleConsensusVersion};
use fedimint_core::{plugin_types_trait_impl_common, Amount, OutPoint};
use secp256k1::XOnlyPublicKey;
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
    NewSellOrder {
        owner: XOnlyPublicKey,
        market: OutPoint,
        outcome: OutcomeSize,
        price: Amount,
        sources: Vec<ContractSource>,
    },
    ConsumeOrderFreeBalance {
        order: XOnlyPublicKey,
    },
    CancelOrder {
        order: XOnlyPublicKey,
    },
    PayoutMarket {
        market: OutPoint,
        payout: Payout,
    },
}

/// Output for a fedimint transaction
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize, Encodable, Decodable)]
pub enum PredictionMarketsOutput {
    NewMarket {
        contract_price: Amount,
        outcomes: OutcomeSize,
        outcome_control: XOnlyPublicKey,
        description: MarketDescription,
    },
    NewBuyOrder {
        owner: XOnlyPublicKey,
        market: OutPoint,
        outcome: OutcomeSize,
        price: Amount,
        quantity: ContractAmount,
    },
}

/// Information needed by a client to update output funds
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize, Encodable, Decodable)]
pub enum PredictionMarketsOutputOutcome {
    NewMarket,
    NewOrder,
}

/// Errors that might be returned by the server
// TODO: Move to server lib?
#[derive(Debug, Clone, Eq, PartialEq, Hash, Error)]
pub enum PredictionMarketsError {
    // general
    #[error("Not enough funds")]
    NotEnoughFunds,

    // markets
    #[error("New market does not pass server validation")]
    MarketValidationFailed,
    #[error("Market does not exist")]
    MarketDoesNotExist,

    // orders
    #[error("New order does not pass server validation")]
    OrderValidationFailed,
    #[error("Order does not exist")]
    OrderDoesNotExist,
    #[error("Order with owner XOnlyPublicKey already exists. Each XOnlyPublicKey can only control 1 order.")]
    OrderAlreadyExists,

    // payouts
    #[error("Payout validation failed")]
    PayoutValidationFailed,
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

/// Markets are identified by the OutPoint they were created in.
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct Market {
    pub contract_price: Amount,
    pub outcomes: OutcomeSize,

    pub outcome_control: XOnlyPublicKey,
    pub description: MarketDescription,

    pub payout: Option<Payout>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct MarketDescription {
    pub title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct Payout {
    pub outcome_payouts: Vec<Amount>,
}

/// Orders are identified by the [XOnlyPublicKey] that controls them. Each [XOnlyPublicKey]
/// can only control a single order.
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct Order {
    // static
    pub outcome: OutcomeSize,
    pub side: Side,
    pub price: Amount,
    pub original_quantity: ContractAmount,
    pub time_priority: TimePriority,

    // mutated
    pub quantity_waiting_for_match: ContractAmount,
    pub contract_balance: ContractAmount,
    pub btc_balance: Amount,
}

pub type OutcomeSize = u8;

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    Encodable,
    Decodable,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
)]
pub struct ContractAmount(pub u64);
impl ContractAmount {
    pub const ZERO: ContractAmount = ContractAmount(0);
}

impl Add for ContractAmount {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(
            self.0
                .checked_add(rhs.0)
                .expect("PredictionMarkets: ContractAmount: addition overflow"),
        )
    }
}

impl Sub for ContractAmount {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(
            self.0
                .checked_sub(rhs.0)
                .expect("PredictionMarkets: ContractAmount: subtraction overflow"),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct TimePriority(pub u64);

/// new sells use this to specify where to source quantity
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct ContractSource {
    pub order: XOnlyPublicKey,
    pub amount: ContractAmount,
}
