use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::ops::{Add, Sub};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Error;
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
pub enum PredictionMarketsConsensusItem {
    TimestampProposal(UnixTimestamp),
}

/// Input for a fedimint transaction
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize, Encodable, Decodable)]
pub enum PredictionMarketsInput {
    NewSellOrder {
        owner: XOnlyPublicKey,
        market: OutPoint,
        outcome: Outcome,
        price: Amount,
        sources: Vec<ContractOfOutcomeSource>,
    },
    ConsumeOrderBitcoinBalance {
        order: XOnlyPublicKey,
        amount: Amount,
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
        outcomes: Outcome,
        outcome_control: XOnlyPublicKey,
        description: MarketDescription,
    },
    NewBuyOrder {
        owner: XOnlyPublicKey,
        market: OutPoint,
        outcome: Outcome,
        price: Amount,
        quantity: ContractOfOutcomeAmount,
    },
}

/// Information needed by a client to update output funds
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize, Encodable, Decodable)]
pub enum PredictionMarketsOutputOutcome {
    NewMarket,
    NewBuyOrder,
}

/// Errors that might be returned by the server
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
    #[error("The market has already finished. A payout has occured")]
    MarketFinished,

    // orders
    #[error("New order does not pass server validation")]
    OrderValidationFailed,
    #[error("Order does not exist")]
    OrderDoesNotExist,
    #[error("Order with owner XOnlyPublicKey already exists. Each XOnlyPublicKey can only control 1 order.")]
    OrderAlreadyExists,
    #[error("Order's quantity waiting for match is already 0")]
    OrderAlreadyFinished,

    // payouts
    #[error("Payout validation failed")]
    PayoutValidationFailed,
    #[error("A payout already exists for market")]
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
    // static
    pub contract_price: Amount,
    pub outcomes: Outcome,
    pub outcome_control: XOnlyPublicKey,
    pub description: MarketDescription,

    // mutated
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

/// On the server side, Orders are identified by the [XOnlyPublicKey] that controls them. Each [XOnlyPublicKey]
/// can only control a single order.
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct Order {
    // static
    pub market: OutPoint,
    pub outcome: Outcome,
    pub side: Side,
    pub price: Amount,
    pub original_quantity: ContractOfOutcomeAmount,
    pub time_ordering: TimeOrdering,

    // mutated
    pub quantity_waiting_for_match: ContractOfOutcomeAmount,
    pub contract_of_outcome_balance: ContractOfOutcomeAmount,
    pub bitcoin_balance: Amount,
}

/// Same as the ChildID used from the order root secret to derive order owner
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
pub struct OrderIDClientSide(pub u64);

/// The id of outcomes starts from 0 like an array.
pub type Outcome = u8;

/// Side of order
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn opposite(&self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

impl TryFrom<&str> for Side {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "buy" => Ok(Self::Buy),
            "sell" => Ok(Self::Sell),
            _ => Err(Error::msg("could not parse side")),
        }
    }
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
pub struct ContractOfOutcomeAmount(pub u64);
impl ContractOfOutcomeAmount {
    pub const ZERO: ContractOfOutcomeAmount = ContractOfOutcomeAmount(0);
}

impl Add for ContractOfOutcomeAmount {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(
            self.0
                .checked_add(rhs.0)
                .expect("PredictionMarkets: ContractOfOutcomeAmount: addition overflow"),
        )
    }
}

impl Sub for ContractOfOutcomeAmount {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(
            self.0
                .checked_sub(rhs.0)
                .expect("PredictionMarkets: ContractOfOutcomeAmount: subtraction overflow"),
        )
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct TimeOrdering(pub u64);

/// new sells use this to specify where to source quantity
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct ContractOfOutcomeSource {
    pub order: XOnlyPublicKey,
    pub quantity: ContractOfOutcomeAmount,
}

/// Used to represent negative prices.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct SignedAmount {
    pub amount: Amount,
    pub negative: bool,
}

impl SignedAmount {
    pub fn is_negative(&self) -> bool {
        self.negative && self.amount != Amount::ZERO
    }
}

impl From<Amount> for SignedAmount {
    fn from(value: Amount) -> Self {
        SignedAmount {
            amount: value,
            negative: false,
        }
    }
}

impl TryFrom<SignedAmount> for Amount {
    type Error = anyhow::Error;

    fn try_from(value: SignedAmount) -> Result<Self, Self::Error> {
        if value.is_negative() {
            Err(Error::msg(
                "SignedAmount is negative. Amount cannot represent a negative.",
            ))
        } else {
            Ok(value.amount)
        }
    }
}

impl PartialOrd for SignedAmount {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SignedAmount {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.negative, other.negative) {
            (true, true) => other.amount.msats.cmp(&self.amount.msats),
            (true, false) => {
                if self.amount == Amount::ZERO && other.amount == Amount::ZERO {
                    Ordering::Equal
                } else {
                    Ordering::Less
                }
            }
            (false, true) => {
                if self.amount == Amount::ZERO && other.amount == Amount::ZERO {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (false, false) => self.amount.msats.cmp(&other.amount.msats),
        }
    }
}

impl Add for SignedAmount {
    type Output = SignedAmount;

    fn add(self, rhs: Self) -> Self::Output {
        if self.negative ^ rhs.negative {
            if self.amount < rhs.amount {
                SignedAmount {
                    amount: rhs.amount - self.amount,
                    negative: rhs.negative,
                }
            } else {
                SignedAmount {
                    amount: self.amount - rhs.amount,
                    negative: self.negative,
                }
            }
        } else {
            SignedAmount {
                amount: self.amount + rhs.amount,
                negative: self.negative,
            }
        }
    }
}

impl Sub for SignedAmount {
    type Output = SignedAmount;

    fn sub(self, mut rhs: Self) -> Self::Output {
        rhs.negative = !rhs.negative;

        self + rhs
    }
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
pub struct UnixTimestamp {
    pub seconds: u64,
}

impl UnixTimestamp {
    pub fn now() -> Self {
        UnixTimestamp {
            seconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("failed to get system unix timestamp")
                .as_secs(),
        }
    }

    pub fn round_down(&self, seconds: u64) -> Self {
        UnixTimestamp {
            seconds: self.seconds - self.seconds % seconds,
        }
    }

    pub fn divisible(&self, seconds: u64) -> bool {
        self.seconds % seconds == 0
    }

    pub fn duration_till(&self) -> Duration {
        Duration::from_secs(self.seconds)
            .checked_sub(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("failed to get system unix timestamp"),
            )
            .unwrap_or(Duration::ZERO)
    }
}
