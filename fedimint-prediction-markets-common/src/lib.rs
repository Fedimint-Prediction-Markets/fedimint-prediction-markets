use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::hash::Hash;
use std::ops::{Add, AddAssign, Mul, Sub, SubAssign};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::bail;
use config::{GeneralConsensus, PredictionMarketsClientConfig};
use fedimint_core::core::{Decoder, ModuleInstanceId, ModuleKind};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::module::{CommonModuleInit, ModuleCommon, ModuleConsensusVersion};
use fedimint_core::{plugin_types_trait_impl_common, Amount, OutPoint};
use prediction_market_event::Event;
pub use prediction_market_event::Outcome;
use secp256k1::PublicKey;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// Common contains types shared by both the client and server

// The client and server configuration
pub mod config;

// api params and results
pub mod api;

/// Unique name for this module
pub const KIND: ModuleKind = ModuleKind::from_static_str("prediction-markets");

/// Modules are non-compatible with older versions
pub const MODULE_CONSENSUS_VERSION: ModuleConsensusVersion =
    ModuleConsensusVersion { major: 0, minor: 0 };

/// Non-transaction items that will be submitted to consensus
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, Encodable, Decodable)]
pub enum PredictionMarketsConsensusItem {
    TimestampProposal(UnixTimestamp),
}

/// Input for a fedimint transaction
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize, Encodable, Decodable)]
pub enum PredictionMarketsInput {
    NewSellOrder {
        owner: PublicKey,
        market: OutPoint,
        outcome: Outcome,
        price: Amount,
        sources: BTreeMap<PublicKey, ContractOfOutcomeAmount>,
    },
    ConsumeOrderBitcoinBalance {
        order: PublicKey,
        amount: Amount,
    },
    CancelOrder {
        order: PublicKey,
    },
}

/// Output for a fedimint transaction
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize, Encodable, Decodable)]
pub enum PredictionMarketsOutput {
    NewMarket {
        event_json: EventJson,
        contract_price: Amount,
        payout_control_weight_map: BTreeMap<NostrPublicKeyHex, Weight>,
        weight_required_for_payout: WeightRequiredForPayout,
    },
    NewBuyOrder {
        owner: PublicKey,
        market: OutPoint,
        outcome: Outcome,
        price: Amount,
        quantity: ContractOfOutcomeAmount,
    },
    PayoutMarket {
        market: OutPoint,
        event_payout_attestations_json: Vec<NostrEventJson>,
    },
}

/// Information needed by a client to update output funds
#[derive(Debug, Clone, Eq, PartialEq, Hash, Deserialize, Serialize, Encodable, Decodable)]
pub enum PredictionMarketsOutputOutcome {
    NewMarket,
    NewBuyOrder,
}

/// Errors that might be returned by the server
#[derive(Debug, Clone, Eq, PartialEq, Hash, Error, Encodable, Decodable)]
pub enum PredictionMarketsInputError {
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
    #[error("Order with owner PublicKey already exists. Each PublicKey can only control 1 order.")]
    OrderAlreadyExists,
    #[error("Order's quantity waiting for match is already 0")]
    OrderAlreadyFinished,

    // payouts
    #[error("Payout validation failed")]
    PayoutValidationFailed,
    #[error("A payout already exists for market")]
    PayoutAlreadyExists,

    // other
    #[error("Other: {0}")]
    Other(String),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Error, Encodable, Decodable)]
pub enum PredictionMarketsOutputError {
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
    #[error("Order with owner PublicKey already exists. Each PublicKey can only control 1 order.")]
    OrderAlreadyExists,
    #[error("Order's quantity waiting for match is already 0")]
    OrderAlreadyFinished,

    // payouts
    #[error("Payout validation failed")]
    PayoutValidationFailed,
    #[error("A payout already exists for market")]
    PayoutAlreadyExists,

    // other
    #[error("Other: {0}")]
    Other(String),
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
    PredictionMarketsConsensusItem,
    PredictionMarketsInputError,
    PredictionMarketsOutputError
);

#[derive(Debug)]
pub struct PredictionMarketsCommonInit;

impl CommonModuleInit for PredictionMarketsCommonInit {
    const CONSENSUS_VERSION: ModuleConsensusVersion = MODULE_CONSENSUS_VERSION;
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

/// Markets are identified by the [PredictionMarketsOutput::NewMarket]
/// [OutPoint] they were created in.
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct Market(pub MarketStatic, pub MarketDynamic);

impl Market {
    pub fn validate_market_params(
        gc: &GeneralConsensus,
        event: &Event,
        contract_price: &Amount,
        payout_control_weight_map: &BTreeMap<NostrPublicKeyHex, Weight>,
        weight_required_for_payout: &WeightRequiredForPayout,
    ) -> Result<(), ()> {
        // validate event
        let accepted_information_variant_ids = gc
            .accepted_event_information_variant_ids
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<&str>>();
        if let Err(_) = event.validate(accepted_information_variant_ids.as_slice()) {
            return Err(());
        }
        if event.outcome_count > gc.max_market_outcomes {
            return Err(());
        }

        // validate contract price
        if contract_price == &Amount::ZERO || contract_price > &gc.max_contract_price {
            return Err(());
        }
        if contract_price.msats % u64::from(event.units_to_payout) != 0 {
            return Err(());
        }

        // validate payout_control_weight_map
        if payout_control_weight_map.len() == 0
            || payout_control_weight_map.len() > usize::from(gc.max_payout_control_keys)
        {
            return Err(());
        }

        for (payout_control, weight) in payout_control_weight_map.iter() {
            if !prediction_market_event::nostr_event_types::NostrPublicKeyHex::is_valid_format(
                &payout_control,
            ) {
                return Err(());
            }

            if weight < &1 {
                return Err(());
            }
        }

        // validate weight required for payout
        if weight_required_for_payout < &1 {
            return Err(());
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct MarketStatic {
    // set by market creator
    pub event_json: EventJson,
    pub contract_price: Amount,
    pub payout_control_weight_map: BTreeMap<NostrPublicKeyHex, Weight>,
    pub weight_required_for_payout: WeightRequiredForPayout,

    // set by guardians
    pub created_consensus_timestamp: UnixTimestamp,
}

impl MarketStatic {
    pub fn event(&self) -> Result<Event, prediction_market_event::Error> {
        Event::try_from_json_str(&self.event_json)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct MarketDynamic {
    pub open_contracts: ContractAmount,
    pub payout: Option<Payout>,
}

pub type Weight = u16;
pub type WeightRequiredForPayout = u64;

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct Payout {
    pub amount_per_outcome: Vec<Amount>,
    pub occurred_consensus_timestamp: UnixTimestamp,
}

/// On the server side, Orders are identified by the [PublicKey] that
/// controls them. Each [PublicKey] can only control a single order.
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

    // ----- mutated (for operation) -----

    // active market quantity
    pub quantity_waiting_for_match: ContractOfOutcomeAmount,

    // fulfilled buys add to this balance
    // sells use this balance for funding
    // during payout, the payout amount is found by multiplying this by the order's outcome's
    // payout amount. payouts set this to zero
    pub contract_of_outcome_balance: ContractOfOutcomeAmount,

    // spendable by ConsumeOrderBitcoinBalance input
    pub bitcoin_balance: Amount,

    // ----- mutated (for user information only) -----

    // how many contract of outcome were either bought or sold by this order
    pub quantity_fulfilled: ContractOfOutcomeAmount,

    // buys (for positive prices) subtract from this
    // sells add to this
    pub bitcoin_acquired_from_order_matches: SignedAmount,

    // payouts add to this
    pub bitcoin_acquired_from_payout: Amount,
}

impl Order {
    pub fn validate_order_params(
        gc: &GeneralConsensus,
        market_outcome_count: &Outcome,
        market_contract_price: &Amount,
        outcome: &Outcome,
        price: &Amount,
        quantity: &ContractOfOutcomeAmount,
    ) -> Result<(), ()> {
        if outcome >= &market_outcome_count
            || price == &Amount::ZERO
            || price >= &market_contract_price
            || quantity == &ContractOfOutcomeAmount::ZERO
            || quantity > &gc.max_order_quantity
        {
            Err(())
        } else {
            Ok(())
        }
    }
}

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

impl FromStr for Side {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "buy" => Ok(Self::Buy),
            "sell" => Ok(Self::Sell),
            _ => bail!("could not parse side"),
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
#[serde(transparent)]
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

impl AddAssign for ContractAmount {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
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

impl SubAssign for ContractAmount {
    fn sub_assign(&mut self, rhs: Self) {
        *self = *self - rhs;
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
#[serde(transparent)]
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

impl AddAssign for ContractOfOutcomeAmount {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
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

impl SubAssign for ContractOfOutcomeAmount {
    fn sub_assign(&mut self, rhs: Self) {
        *self = *self - rhs;
    }
}

impl FromStr for ContractOfOutcomeAmount {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(u64::from_str(s)?))
    }
}

pub type TimeOrdering = u64;

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
            bail!("SignedAmount is negative. Amount cannot represent a negative.",)
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
            (true, true) => other.amount.cmp(&self.amount),
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
            (false, false) => self.amount.cmp(&other.amount),
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

impl AddAssign for SignedAmount {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}

impl Sub for SignedAmount {
    type Output = SignedAmount;

    fn sub(self, mut rhs: Self) -> Self::Output {
        rhs.negative = !rhs.negative;

        self + rhs
    }
}

impl SubAssign for SignedAmount {
    fn sub_assign(&mut self, rhs: Self) {
        *self = *self - rhs;
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

impl Display for SignedAmount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prefix = match self.is_negative() {
            true => "-",
            false => "",
        };

        write!(f, "{}{}", prefix, self.amount)
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
#[serde(transparent)]
pub struct UnixTimestamp(pub Seconds);

impl UnixTimestamp {
    pub const ZERO: Self = Self(0);

    pub fn now() -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        return UnixTimestamp(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("failed to get system unix timestamp")
                .as_secs(),
        );
        #[cfg(target_arch = "wasm32")]
        return UnixTimestamp((js_sys::Date::now() / 1000f64) as Seconds);
    }

    pub fn round_down(&self, seconds: Seconds) -> Self {
        UnixTimestamp(self.0 - self.0 % seconds)
    }

    pub fn divisible(&self, seconds: Seconds) -> bool {
        self.0 % seconds == 0
    }
}

impl FromStr for UnixTimestamp {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Seconds::from_str(s)?))
    }
}

pub type EventJson = String;
pub type EventHashHex = String;
pub type NostrPublicKeyHex = String;
pub type NostrEventJson = String;

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct Candlestick {
    pub open: Amount,
    pub close: Amount,
    pub high: Amount,
    pub low: Amount,

    // swaps produce 2 volume, creation/deletion produce 1 volume
    pub volume: ContractOfOutcomeAmount,
}
