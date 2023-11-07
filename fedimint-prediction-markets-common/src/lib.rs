use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::hash::Hash;
use std::ops::{Add, Mul, Sub};
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
        sources: BTreeMap<XOnlyPublicKey, ContractOfOutcomeAmount>,
    },
    ConsumeOrderBitcoinBalance {
        order: XOnlyPublicKey,
        amount: Amount,
    },
    CancelOrder {
        order: XOnlyPublicKey,
    },
    PayoutProposal {
        market: OutPoint,
        outcome_control: XOnlyPublicKey,
        outcome_payouts: Vec<Amount>,
    },
}

/// Output for a fedimint transaction
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize, Encodable, Decodable)]
pub enum PredictionMarketsOutput {
    NewMarket {
        contract_price: Amount,
        outcomes: Outcome,
        outcome_control_weights: BTreeMap<XOnlyPublicKey, Weight>,
        weight_required: WeightRequired,
        information: MarketInformation,
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
    pub outcome_controls_weights: BTreeMap<XOnlyPublicKey, Weight>,
    pub weight_required: WeightRequired,
    pub information: MarketInformation,

    pub created_consensus_timestamp: UnixTimestamp,

    // mutated
    pub open_contracts: ContractAmount,
    pub payout: Option<Payout>,
}

impl Market {
    pub fn validate_market_params(
        consensus_max_contract_price: &Amount,
        consensus_max_market_outcomes: &Outcome,
        consensus_max_outcome_control_keys: &u16,
        contract_price: &Amount,
        outcomes: &Outcome,
        outcome_control_weights: &BTreeMap<XOnlyPublicKey, Weight>,
        information: &MarketInformation,
    ) -> Result<(), ()> {
        // verify market params
        if contract_price == &Amount::ZERO
            || contract_price > consensus_max_contract_price
            || outcomes < &2
            || outcomes > consensus_max_market_outcomes
            || outcome_control_weights.len() > usize::from(*consensus_max_outcome_control_keys)
        {
            return Err(());
        }

        for (_, weight) in outcome_control_weights.iter() {
            if weight == &0 {
                return Err(());
            }
        }

        if let Err(_) = information.validate(outcomes) {
            return Err(());
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct MarketInformation {
    pub title: String,
    pub description: String,

    pub outcome_titles: Vec<String>,
    pub expected_payout_time: UnixTimestamp,
}

impl MarketInformation {
    // sane default size limits
    const MAX_TITLE_LENGTH: usize = 150;
    const MAX_DESCRIPTION_LENGTH: usize = 500;
    const MAX_OUTCOME_TITLE_LENGTH: usize = 64;

    pub fn validate(&self, outcomes: &Outcome) -> Result<(), ()> {
        if self.title.len() > Self::MAX_TITLE_LENGTH
            || self.description.len() > Self::MAX_DESCRIPTION_LENGTH
            || self.outcome_titles.len() != usize::from(outcomes.to_owned())
        {
            return Err(());
        }
        for outcome_title in &self.outcome_titles {
            if outcome_title.len() > Self::MAX_OUTCOME_TITLE_LENGTH {
                return Err(());
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct Payout {
    pub outcome_payouts: Vec<Amount>,
    pub occurred_consensus_timestamp: UnixTimestamp,
}

impl Payout {
    pub fn validate_payout_params(
        market: &Market,
        outcome_payouts: &Vec<Amount>,
    ) -> Result<(), ()> {
        let mut total_payout = Amount::ZERO;
        for outcome_payout in outcome_payouts {
            if outcome_payout > &market.contract_price {
                return Err(());
            }

            total_payout += outcome_payout.to_owned();
        }

        if total_payout != market.contract_price
            || outcome_payouts.len() != usize::from(market.outcomes)
        {
            return Err(());
        }

        Ok(())
    }
}

/// On the server side, Orders are identified by the [XOnlyPublicKey] that controls them. Each [XOnlyPublicKey]
/// can only control a single order.
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct Order {
    /// ----- static -----
    pub market: OutPoint,
    pub outcome: Outcome,
    pub side: Side,
    pub price: Amount,
    pub original_quantity: ContractOfOutcomeAmount,
    // increments on each new order. used for price-time priority
    pub time_ordering: TimeOrdering,
    pub created_consensus_timestamp: UnixTimestamp,

    // ----- mutated -----

    // active market quantity
    pub quantity_waiting_for_match: ContractOfOutcomeAmount,

    // during a payout, the contract prices is payed out to orders accoring to this balance.
    // payouts empty this balance
    pub contract_of_outcome_balance: ContractOfOutcomeAmount,

    // spendable by ConsumeOrderBitcoinBalance input
    pub bitcoin_balance: Amount,

    // cost incurred by order matches
    // buys (for posistive prices) add to cost
    // sells subtract from cost
    pub bitcoin_cost: SignedAmount,
}

impl Order {
    /// returns true on pass of verification
    pub fn validate_order_params(
        market: &Market,
        consensus_max_order_quantity: &ContractOfOutcomeAmount,
        outcome: &Outcome,
        price: &Amount,
        quantity: &ContractOfOutcomeAmount,
    ) -> Result<(), ()> {
        if outcome >= &market.outcomes
            || price == &Amount::ZERO
            || price >= &market.contract_price
            || quantity == &ContractOfOutcomeAmount::ZERO
            || quantity > consensus_max_order_quantity
        {
            Err(())
        } else {
            Ok(())
        }
    }
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
pub struct OrderIdClientSide(pub u64);

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
pub struct ContractAmount(pub u64);
impl ContractAmount {
    pub const ZERO: Self = ContractAmount(0);
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

/// Used to represent negative prices.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct SignedAmount {
    pub amount: Amount,
    pub negative: bool,
}

impl SignedAmount {
    pub const ZERO: Self = Self {
        amount: Amount::ZERO,
        negative: false,
    };

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

impl Mul<u64> for SignedAmount {
    type Output = SignedAmount;

    fn mul(self, rhs: u64) -> Self::Output {
        Self {
            amount: self.amount * rhs,
            negative: self.negative,
        }
    }
}

pub type Seconds = u64;

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
    pub seconds: Seconds,
}

impl UnixTimestamp {
    pub const ZERO: Self = Self { seconds: 0 };

    pub fn now() -> Self {
        UnixTimestamp {
            seconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("failed to get system unix timestamp")
                .as_secs(),
        }
    }

    pub fn round_down(&self, seconds: Seconds) -> Self {
        UnixTimestamp {
            seconds: self.seconds - self.seconds % seconds,
        }
    }

    pub fn divisible(&self, seconds: Seconds) -> bool {
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetOutcomeControlMarketsParams {
    pub outcome_control: XOnlyPublicKey,
    pub markets_created_after_and_including: UnixTimestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetOutcomeControlMarketsResult {
    pub markets: Vec<OutPoint>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetMarketOutcomeCandlesticksParams {
    pub market: OutPoint,
    pub outcome: Outcome,
    pub candlestick_interval: Seconds,
    pub candlestick_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetMarketOutcomeCandlesticksResult {
    pub candlesticks: Vec<(UnixTimestamp, Candlestick)>,
}

pub type Weight = u8;
pub type WeightRequired = u32;

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct Candlestick {
    pub open: Amount,
    pub close: Amount,
    pub high: Amount,
    pub low: Amount,

    // swaps produce 2 volume, creation/deletion produce 1 volume
    pub volume: ContractOfOutcomeAmount,
}
