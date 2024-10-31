use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::{impl_db_lookup, impl_db_record, OutPoint};
use fedimint_prediction_markets_common::{
    Market, NostrPublicKeyHex, Order, Outcome, Side, TimeOrdering, UnixTimestamp,
};

use crate::OrderId;

#[repr(u8)]
#[derive(Clone, Debug)]
pub enum DbKeyPrefix {
    /// Cache for markets
    ///
    /// Market's [OutPoint] to [Market]
    Market = 0x00,

    /// Cache for orders
    ///
    /// [OrderId] to [Order]
    Order = 0x01,

    /// Orders by market outcome
    ///
    /// (Market's [OutPoint], [Outcome], [OrderId], [Side]) to ()
    OrdersByMarketOutcomeSide = 0x21,

    /// Orders with some kind of balance.
    ///
    /// (Market's [OutPoint], [Outcome], [OrderId], [Side]) to ()
    NonZeroOrdersByMarketOutcomeSide = 0x22,

    /// (Market's [OutPoint], [Outcome], [Side], Price priority [u64],
    /// [TimeOrdering]) to ([OrderId])
    OrderPriceTimePriority = 0x23,

    /// (Market's [OutPoint]) to (Saved to db [UnixTimestamp])
    ClientSavedMarkets = 0x41,

    /// (Name [String]) to (Payout control [NostrPublicKeyHex])
    ClientNamedPayoutControls = 0x42,
}

// Market
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct MarketKey(pub OutPoint);

#[derive(Debug, Encodable, Decodable)]
pub struct MarketPrefixAll;

impl_db_record!(
    key = MarketKey,
    value = Market,
    db_prefix = DbKeyPrefix::Market,
);

impl_db_lookup!(key = MarketKey, query_prefix = MarketPrefixAll);

// Order
#[derive(Debug, Encodable, Decodable, PartialEq, Eq, Clone)]
pub enum OrderIdSlot {
    Reserved,
    Order(Order),
}

impl OrderIdSlot {
    pub fn to_order(self) -> Option<Order> {
        match self {
            Self::Reserved => None,
            Self::Order(order) => Some(order),
        }
    }
}

#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct OrderKey(pub OrderId);

#[derive(Debug, Encodable, Decodable)]
pub struct OrderPrefixAll;

impl_db_record!(
    key = OrderKey,
    value = OrderIdSlot,
    db_prefix = DbKeyPrefix::Order,
    notify_on_modify = true
);

impl_db_lookup!(key = OrderKey, query_prefix = OrderPrefixAll);

// OrdersByMarketOutcome
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct OrdersByMarketOutcomeKey {
    pub market: OutPoint,
    pub outcome: Outcome,
    pub side: Side,
    pub order: OrderId,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OrdersByMarketOutcomePrefixAll;

#[derive(Debug, Encodable, Decodable)]
pub struct OrdersByMarketOutcomePrefix1 {
    pub market: OutPoint,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OrdersByMarketOutcomePrefix2 {
    pub market: OutPoint,
    pub outcome: Outcome,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OrdersByMarketOutcomePrefix3 {
    pub market: OutPoint,
    pub outcome: Outcome,
    pub side: Side,
}

impl_db_record!(
    key = OrdersByMarketOutcomeKey,
    value = (),
    db_prefix = DbKeyPrefix::OrdersByMarketOutcomeSide,
);

impl_db_lookup!(
    key = OrdersByMarketOutcomeKey,
    query_prefix = OrdersByMarketOutcomePrefixAll,
    query_prefix = OrdersByMarketOutcomePrefix1,
    query_prefix = OrdersByMarketOutcomePrefix2,
    query_prefix = OrdersByMarketOutcomePrefix3
);

// NonZeroOrdersByMarketOutcome
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct NonZeroOrdersByMarketOutcomeKey {
    pub market: OutPoint,
    pub outcome: Outcome,
    pub side: Side,
    pub order: OrderId,
}

#[derive(Debug, Encodable, Decodable)]
pub struct NonZeroOrdersByMarketOutcomePrefixAll;

#[derive(Debug, Encodable, Decodable)]
pub struct NonZeroOrdersByMarketOutcomePrefix1 {
    pub market: OutPoint,
}

#[derive(Debug, Encodable, Decodable)]
pub struct NonZeroOrdersByMarketOutcomePrefix2 {
    pub market: OutPoint,
    pub outcome: Outcome,
}

#[derive(Debug, Encodable, Decodable)]
pub struct NonZeroOrdersByMarketOutcomePrefix3 {
    pub market: OutPoint,
    pub outcome: Outcome,
    pub side: Side,
}

impl_db_record!(
    key = NonZeroOrdersByMarketOutcomeKey,
    value = (),
    db_prefix = DbKeyPrefix::NonZeroOrdersByMarketOutcomeSide,
);

impl_db_lookup!(
    key = NonZeroOrdersByMarketOutcomeKey,
    query_prefix = NonZeroOrdersByMarketOutcomePrefixAll,
    query_prefix = NonZeroOrdersByMarketOutcomePrefix1,
    query_prefix = NonZeroOrdersByMarketOutcomePrefix2,
    query_prefix = NonZeroOrdersByMarketOutcomePrefix3
);

// ClientSavedMarkets
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct ClientSavedMarketsKey {
    pub market: OutPoint,
}

#[derive(Debug, Encodable, Decodable)]
pub struct ClientSavedMarketsPrefixAll;

impl_db_record!(
    key = ClientSavedMarketsKey,
    value = UnixTimestamp,
    db_prefix = DbKeyPrefix::ClientSavedMarkets,
);

impl_db_lookup!(
    key = ClientSavedMarketsKey,
    query_prefix = ClientSavedMarketsPrefixAll
);

// ClientSavedPayoutControls
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct ClientNamedPayoutControlsKey {
    pub name: String,
}

#[derive(Debug, Encodable, Decodable)]
pub struct ClientNamedPayoutControlsPrefixAll;

impl_db_record!(
    key = ClientNamedPayoutControlsKey,
    value = NostrPublicKeyHex,
    db_prefix = DbKeyPrefix::ClientNamedPayoutControls,
);

impl_db_lookup!(
    key = ClientNamedPayoutControlsKey,
    query_prefix = ClientNamedPayoutControlsPrefixAll
);

/// OrderPriceTimePriority
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct OrderPriceTimePriorityKey {
    pub market: OutPoint,
    pub outcome: Outcome,
    pub side: Side,
    pub price_priority: u64,
    pub time_priority: TimeOrdering,
}

impl OrderPriceTimePriorityKey {
    pub fn from_order(order: &Order) -> Self {
        let price_priority = match order.side {
            Side::Buy => u64::MAX - order.price.msats,
            Side::Sell => order.price.msats,
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
    value = OrderId,
    db_prefix = DbKeyPrefix::OrderPriceTimePriority,
);

impl_db_lookup!(
    key = OrderPriceTimePriorityKey,
    query_prefix = OrderPriceTimePriorityPrefix3,
    query_prefix = OrderPriceTimePriorityPrefixAll
);

// template
// #[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
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
