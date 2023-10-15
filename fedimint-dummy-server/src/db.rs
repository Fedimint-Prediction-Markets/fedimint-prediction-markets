use fedimint_core::encoding::{Decodable, Encodable};

use fedimint_core::{impl_db_lookup, impl_db_record, Amount, OutPoint};

#[allow(unused_imports)]
use fedimint_dummy_common::{Market, Order, OutcomeSize, Payout, Side, TimePriority};

use serde::Serialize;
use strum_macros::EnumIter;

#[allow(unused_imports)]
use crate::{
    PredictionMarketsOutput, PredictionMarketsOutput::NewMarket, PredictionMarketsOutput::NewOrder,
    PredictionMarketsOutputOutcome,
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

    /// [NewOrder] [OutPoint] to [Order]
    Order = 0x02,

    /// ----- 20-3f reserved for market operation -----

    /// Used to produce time priority for new orders
    ///
    /// Market's [OutPoint] to [TimePriority]
    NextOrderTimePriority = 0x20,

    /// Used for payouts
    ///
    /// (Market's [OutPoint], Order's [OutPoint]) to ()
    OrdersByMarket = 0x21,

    /// Used to implement orderbook. Only holds active orders.
    ///
    /// Amount is (contract_price - price of order) for buys
    /// Amount is (price of order) for sells
    ///
    /// (Market's [OutPoint], [OutcomeSize], [Side], [Amount], [TimePriority]) to (Order's [OutPoint])
    OrderPriceTimePriority = 0x22,
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

impl_db_lookup!(
    key = OutcomeKey,
    query_prefix = OutcomePrefixAll
);

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
pub struct OrderKey(pub OutPoint);

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
pub struct NextOrderTimePriorityKey {
    pub market: OutPoint,
}

#[derive(Debug, Encodable, Decodable)]
pub struct NextOrderTimePriorityPrefixAll;

impl_db_record!(
    key = NextOrderTimePriorityKey,
    value = TimePriority,
    db_prefix = DbKeyPrefix::NextOrderTimePriority,
);

impl_db_lookup!(
    key = NextOrderTimePriorityKey,
    query_prefix = NextOrderTimePriorityPrefixAll
);

/// OrdersByMarket
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct OrdersByMarketKey {
    pub market: OutPoint,
    pub order: OutPoint,
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
    pub outcome: OutcomeSize,
    pub side: Side,
    pub price_priority: Amount,
    pub time_priority: TimePriority
}

#[derive(Debug, Encodable, Decodable)]
pub struct OrderPriceTimePriorityPrefix3 {
    pub market: OutPoint,
    pub outcome: OutcomeSize,
    pub side: Side,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OrderPriceTimePriorityPrefixAll;

impl_db_record!(
    key = OrderPriceTimePriorityKey,
    value = OutPoint,
    db_prefix = DbKeyPrefix::OrderPriceTimePriority,
);

impl_db_lookup!(
    key = OrderPriceTimePriorityKey,
    query_prefix = OrderPriceTimePriorityPrefix3,
    query_prefix = OrderPriceTimePriorityPrefixAll
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
