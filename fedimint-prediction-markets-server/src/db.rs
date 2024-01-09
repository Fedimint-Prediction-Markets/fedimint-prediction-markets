use fedimint_core::encoding::{Decodable, Encodable};

use fedimint_core::{impl_db_lookup, impl_db_record, Amount, OutPoint, PeerId};

#[allow(unused_imports)]
use fedimint_prediction_markets_common::{
    Candlestick, ContractAmount, Market, Order, Outcome, Payout, Seconds, Side, TimeOrdering,
    UnixTimestamp, ContractOfOutcomeAmount
};

use secp256k1::XOnlyPublicKey;
use serde::Serialize;
use strum_macros::EnumIter;

#[allow(unused_imports)]
use crate::{
    PredictionMarketsOutput, PredictionMarketsOutput::NewBuyOrder,
    PredictionMarketsOutput::NewMarket, PredictionMarketsOutputOutcome,
};

/// Namespaces DB keys for this module
#[repr(u8)]
#[derive(Clone, EnumIter, Debug)]
pub enum DbKeyPrefix {
    /// ----- 00-1f reserved for general information storage -----

    /// [PredictionMarketsOutput] [OutPoint] to [PredictionMarketsOutputOutcome]
    Outcome = 0x00,

    /// [NewMarket] [OutPoint] to [Market]
    Market = 0x01,

    /// Owner's [XOnlyPublicKey] to [Order]
    Order = 0x02,

    /// Payout Control's [XOnlyPublicKey] to [Amount]
    PayoutControlBalance = 0x03,

    /// ----- 20-3f reserved for market operation -----

    /// Used to produce time priority for new orders
    ///
    /// Market's [OutPoint] to [TimeOrdering]
    NextOrderTimeOrdering = 0x20,

    /// Used for payouts
    ///
    /// (Market's [OutPoint], Order's [OutPoint]) to ()
    OrdersByMarket = 0x21,

    /// Used to implement orderbook. Only holds active orders.
    ///
    /// Amount is (contract_price - price of order) for buys
    /// Amount is (price of order) for sells
    ///
    /// (Market's [OutPoint], [Outcome], [Side], [Amount], [TimeOrdering]) to (Order's [XOnlyPublicKey])
    OrderPriceTimePriority = 0x22,

    /// These keys are used to implement threshold payouts.
    ///
    /// (Market's [OutPoint], [XOnlyPublicKey]) to [Vec<Amount>]
    MarketPayoutControlProposal = 0x23,
    /// (Market's [OutPoint], [Vec<Amount>], [XOnlyPublicKey]) to ()
    MarketOutcomePayoutsProposals = 0x24,

    /// (Market's [OutPoint], [Outcome], candlestick interval [Seconds], Candle's [UnixTimestamp]) to [Candlestick]
    MarketOutcomeCandlesticks = 0x25,
    /// (Market's [OutPoint], [Outcome], candlestick interval [Seconds]) to (Candle's [UnixTimestamp], [ContractOfOutcomeAmount]) 
    MarketOutcomeNewestCandlestickVolume = 0x26,

    /// ----- 40-4f reserved for api lookup indexes -----

    /// Indexes payout control keys to the markets they belong to
    /// Used by client for data recovery in case of data loss
    ///
    /// ([XOnlyPublicKey], [UnixTimestamp], Market's [OutPoint]) to ()
    PayoutControlMarkets = 0x40,

    /// ----- 60-6f reserved for consensus items -----

    /// Stores timestamps proposed by peers.
    /// Used to create consensus timestamps.
    ///
    /// [PeerId] to [UnixTimestamp]
    PeersProposedTimestamp = 0x60,
}

impl std::fmt::Display for DbKeyPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Outcome
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct OutcomeKey(pub OutPoint);

#[derive(Debug, Encodable, Decodable)]
pub struct OutcomePrefixAll;

impl_db_record!(
    key = OutcomeKey,
    value = PredictionMarketsOutputOutcome,
    db_prefix = DbKeyPrefix::Outcome,
);

impl_db_lookup!(key = OutcomeKey, query_prefix = OutcomePrefixAll);

/// Market
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct MarketKey(pub OutPoint);

#[derive(Debug, Encodable, Decodable)]
pub struct MarketPrefixAll;

impl_db_record!(
    key = MarketKey,
    value = Market,
    db_prefix = DbKeyPrefix::Market,
);

impl_db_lookup!(key = MarketKey, query_prefix = MarketPrefixAll);

/// Order
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct OrderKey(pub XOnlyPublicKey);

#[derive(Debug, Encodable, Decodable)]
pub struct OrderPrefixAll;

impl_db_record!(
    key = OrderKey,
    value = Order,
    db_prefix = DbKeyPrefix::Order,
);

impl_db_lookup!(key = OrderKey, query_prefix = OrderPrefixAll,);

/// NextOrderTimePriority
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct NextOrderTimeOrderingKey {
    pub market: OutPoint,
}

#[derive(Debug, Encodable, Decodable)]
pub struct NextOrderTimeOrderingPrefixAll;

impl_db_record!(
    key = NextOrderTimeOrderingKey,
    value = TimeOrdering,
    db_prefix = DbKeyPrefix::NextOrderTimeOrdering,
);

impl_db_lookup!(
    key = NextOrderTimeOrderingKey,
    query_prefix = NextOrderTimeOrderingPrefixAll
);

/// OrdersByMarket
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct OrdersByMarketKey {
    pub market: OutPoint,
    pub order: XOnlyPublicKey,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OrdersByMarketPrefix1 {
    pub market: OutPoint,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OrdersByMarketPrefixAll;

impl_db_record!(
    key = OrdersByMarketKey,
    value = (),
    db_prefix = DbKeyPrefix::OrdersByMarket,
);

impl_db_lookup!(
    key = OrdersByMarketKey,
    query_prefix = OrdersByMarketPrefix1,
    query_prefix = OrdersByMarketPrefixAll
);

/// OrderPriceTimePriority
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct OrderPriceTimePriorityKey {
    pub market: OutPoint,
    pub outcome: Outcome,
    pub side: Side,
    pub price_priority: Amount,
    pub time_priority: TimeOrdering,
}

impl OrderPriceTimePriorityKey {
    pub fn from_market_and_order(market: &Market, order: &Order) -> Self {
        let price_priority = match order.side {
            Side::Buy => market.contract_price - order.price,
            Side::Sell => order.price,
        };

        OrderPriceTimePriorityKey {
            market: order.market,
            outcome: order.outcome,
            side: order.side,
            price_priority,
            time_priority: order.time_ordering,
        }
    }
}

#[derive(Debug, Encodable, Decodable)]
pub struct OrderPriceTimePriorityPrefix3 {
    pub market: OutPoint,
    pub outcome: Outcome,
    pub side: Side,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OrderPriceTimePriorityPrefixAll;

impl_db_record!(
    key = OrderPriceTimePriorityKey,
    value = XOnlyPublicKey,
    db_prefix = DbKeyPrefix::OrderPriceTimePriority,
);

impl_db_lookup!(
    key = OrderPriceTimePriorityKey,
    query_prefix = OrderPriceTimePriorityPrefix3,
    query_prefix = OrderPriceTimePriorityPrefixAll
);

/// PayoutControlMarkets
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct PayoutControlMarketsKey {
    pub payout_control: XOnlyPublicKey,
    pub market_created: UnixTimestamp,
    pub market: OutPoint,
}

#[derive(Debug, Encodable, Decodable)]
pub struct PayoutControlMarketsPrefixAll;

#[derive(Debug, Encodable, Decodable)]
pub struct PayoutControlMarketsPrefix1 {
    pub payout_control: XOnlyPublicKey,
}

#[derive(Debug, Encodable, Decodable)]
pub struct PayoutControlMarketsPrefix2 {
    pub payout_control: XOnlyPublicKey,
    pub market_created: UnixTimestamp,
}

impl_db_record!(
    key = PayoutControlMarketsKey,
    value = (),
    db_prefix = DbKeyPrefix::PayoutControlMarkets,
);

impl_db_lookup!(
    key = PayoutControlMarketsKey,
    query_prefix = PayoutControlMarketsPrefixAll,
    query_prefix = PayoutControlMarketsPrefix1,
    query_prefix = PayoutControlMarketsPrefix2
);

/// PeersProposedTimestamp
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct PeersProposedTimestampKey {
    pub peer_id: PeerId,
}

#[derive(Debug, Encodable, Decodable)]
pub struct PeersProposedTimestampPrefixAll;

impl_db_record!(
    key = PeersProposedTimestampKey,
    value = UnixTimestamp,
    db_prefix = DbKeyPrefix::PeersProposedTimestamp,
);

impl_db_lookup!(
    key = PeersProposedTimestampKey,
    query_prefix = PeersProposedTimestampPrefixAll
);

// MarketPayoutControlProposal
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct MarketPayoutControlProposalKey {
    pub market: OutPoint,
    pub payout_control: XOnlyPublicKey,
}

#[derive(Debug, Encodable, Decodable)]
pub struct MarketPayoutControlProposalPrefixAll;

#[derive(Debug, Encodable, Decodable)]
pub struct MarketPayoutControlProposalPrefix1 {
    pub market: OutPoint,
}

impl_db_record!(
    key = MarketPayoutControlProposalKey,
    value = Vec<Amount>,
    db_prefix = DbKeyPrefix::MarketPayoutControlProposal,
);

impl_db_lookup!(
    key = MarketPayoutControlProposalKey,
    query_prefix = MarketPayoutControlProposalPrefixAll,
    query_prefix = MarketPayoutControlProposalPrefix1
);

// MarketOutcomePayoutsProposals
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct MarketOutcomePayoutsProposalsKey {
    pub market: OutPoint,
    pub outcome_payouts: Vec<Amount>,
    pub payout_control: XOnlyPublicKey,
}

#[derive(Debug, Encodable, Decodable)]
pub struct MarketOutcomePayoutsProposalsPrefixAll;

#[derive(Debug, Encodable, Decodable)]
pub struct MarketOutcomePayoutsProposalsPrefix2 {
    pub market: OutPoint,
    pub outcome_payouts: Vec<Amount>,
}

impl_db_record!(
    key = MarketOutcomePayoutsProposalsKey,
    value = (),
    db_prefix = DbKeyPrefix::MarketOutcomePayoutsProposals,
);

impl_db_lookup!(
    key = MarketOutcomePayoutsProposalsKey,
    query_prefix = MarketOutcomePayoutsProposalsPrefixAll,
    query_prefix = MarketOutcomePayoutsProposalsPrefix2
);

// MarketOutcomeCandlesticks
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct MarketOutcomeCandlesticksKey {
    pub market: OutPoint,
    pub outcome: Outcome,
    pub candlestick_interval: Seconds,
    pub candlestick_timestamp: UnixTimestamp,
}

#[derive(Debug, Encodable, Decodable)]
pub struct MarketOutcomeCandlesticksPrefixAll;

#[derive(Debug, Encodable, Decodable)]
pub struct MarketOutcomeCandlesticksPrefix3 {
    pub market: OutPoint,
    pub outcome: Outcome,
    pub candlestick_interval: Seconds,
}

impl_db_record!(
    key = MarketOutcomeCandlesticksKey,
    value = Candlestick,
    db_prefix = DbKeyPrefix::MarketOutcomeCandlesticks,
);

impl_db_lookup!(
    key = MarketOutcomeCandlesticksKey,
    query_prefix = MarketOutcomeCandlesticksPrefixAll,
    query_prefix = MarketOutcomeCandlesticksPrefix3,
);

// MarketOutcomeNewestCandlestickVolume
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct MarketOutcomeNewestCandlestickVolumeKey {
    pub market: OutPoint,
    pub outcome: Outcome,
    pub candlestick_interval: Seconds,
}

#[derive(Debug, Encodable, Decodable)]
pub struct MarketOutcomeNewestCandlestickVolumePrefixAll;

impl_db_record!(
    key = MarketOutcomeNewestCandlestickVolumeKey,
    value = (UnixTimestamp, ContractOfOutcomeAmount),
    db_prefix = DbKeyPrefix::MarketOutcomeNewestCandlestickVolume,
    notify_on_modify = true
);

impl_db_lookup!(
    key = MarketOutcomeNewestCandlestickVolumeKey,
    query_prefix = MarketOutcomeNewestCandlestickVolumePrefixAll
);

// PayoutControlBalance
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct PayoutControlBalanceKey {
    pub payout_control: XOnlyPublicKey,
}

#[derive(Debug, Encodable, Decodable)]
pub struct PayoutControlBalancePrefixAll;

impl_db_record!(
    key = PayoutControlBalanceKey,
    value = Amount,
    db_prefix = DbKeyPrefix::PayoutControlBalance,
);

impl_db_lookup!(
    key = PayoutControlBalanceKey,
    query_prefix = PayoutControlBalancePrefixAll
);

// template
// #[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
// pub struct Key {
//     pub market: OutPoint,
// }

// #[derive(Debug, Encodable, Decodable)]
// pub struct Prefix;

// impl_db_record!(
//     key = Key,
//     value = FILL,
//     db_prefix = DbKeyPrefix::FILL,
// );

// impl_db_lookup!(
//     key = Key,
//     query_prefix = Prefix
// );
