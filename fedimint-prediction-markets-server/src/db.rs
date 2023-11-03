use fedimint_core::encoding::{Decodable, Encodable};

use fedimint_core::{impl_db_lookup, impl_db_record, Amount, OutPoint, PeerId};

use fedimint_prediction_markets_common::UnixTimestamp;
#[allow(unused_imports)]
use fedimint_prediction_markets_common::{Market, Order, Outcome, Payout, Side, TimeOrdering};

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
    /// ----- 00-1f reserved for struct storage -----

    /// [PredictionMarketsOutput] [OutPoint] to [PredictionMarketsOutputOutcome]
    Outcome = 0x00,

    /// [NewMarket] [OutPoint] to [Market]
    Market = 0x01,

    /// Owner's [XOnlyPublicKey] to [Order]
    Order = 0x02,

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
    MarketOutcomeControlProposal = 0x23,
    /// (Market's [OutPoint], [Vec<Amount>], [XOnlyPublicKey]) to ()
    MarketOutcomePayoutsProposals = 0x24,

    /// ----- 40-4f reserved for api lookup indexes -----

    /// Indexes outcome control keys to the markets they belong to
    /// Used by client for data recovery in case of data loss
    ///
    /// ([XOnlyPublicKey], [UnixTimestamp], Market's [OutPoint]) to ()
    OutcomeControlMarkets = 0x40,

    /// ----- 50-5f reserved for consensus items -----

    /// Stores timestamps proposed by peers.
    /// Used to create consensus timestamps.
    ///
    /// [PeerId] to [UnixTimestamp]
    PeersProposedTimestamp = 0x51,
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

/// OutcomeControlMarkets
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct OutcomeControlMarketsKey {
    pub outcome_control: XOnlyPublicKey,
    pub market_created: UnixTimestamp,
    pub market: OutPoint,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OutcomeControlMarketsPrefixAll;

#[derive(Debug, Encodable, Decodable)]
pub struct OutcomeControlMarketsPrefix1 {
    pub outcome_control: XOnlyPublicKey,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OutcomeControlMarketsPrefix2 {
    pub outcome_control: XOnlyPublicKey,
    pub market_created: UnixTimestamp,
}

impl_db_record!(
    key = OutcomeControlMarketsKey,
    value = (),
    db_prefix = DbKeyPrefix::OutcomeControlMarkets,
);

impl_db_lookup!(
    key = OutcomeControlMarketsKey,
    query_prefix = OutcomeControlMarketsPrefixAll,
    query_prefix = OutcomeControlMarketsPrefix1,
    query_prefix = OutcomeControlMarketsPrefix2
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

// MarketOutcomeControlProposal
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct MarketOutcomeControlProposalKey {
    pub market: OutPoint,
    pub outcome_control: XOnlyPublicKey,
}

#[derive(Debug, Encodable, Decodable)]
pub struct MarketOutcomeControlProposalPrefixAll;

#[derive(Debug, Encodable, Decodable)]
pub struct MarketOutcomeControlProposalPrefix1 {
    pub market: OutPoint
}

impl_db_record!(
    key = MarketOutcomeControlProposalKey,
    value = Vec<Amount>,
    db_prefix = DbKeyPrefix::MarketOutcomeControlProposal,
);

impl_db_lookup!(
    key = MarketOutcomeControlProposalKey,
    query_prefix = MarketOutcomeControlProposalPrefixAll,
    query_prefix = MarketOutcomeControlProposalPrefix1
);

// MarketOutcomePayoutsProposals
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct MarketOutcomePayoutsProposalsKey {
    pub market: OutPoint,
    pub outcome_payouts: Vec<Amount>,
    pub outcome_control: XOnlyPublicKey,
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
