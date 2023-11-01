use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::{impl_db_lookup, impl_db_record, OutPoint};
use fedimint_prediction_markets_common::{Market, Order, OrderIdClientSide, Outcome};

#[repr(u8)]
#[derive(Clone, Debug)]
pub enum DbKeyPrefix {
    /// ----- 00-1f reserved for struct storage -----

    /// Cache for markets
    ///
    /// Market's [OutPoint] to [Market]
    Market = 0x00,

    /// Cache for orders
    ///
    /// [OrderIdClientSide] to [Order]
    Order = 0x01,

    /// ----- 20-3f reserved for lookup indexes -----

    /// Markets that our outcome control key has some portion of control over.
    ///
    /// Market's [OutPoint]  to ()
    OutcomeControlMarkets = 0x20,

    /// Index for orders by market outcome
    ///
    /// (Market's [OutPoint], [Outcome], [OrderIdClientSide]) to ()
    OrdersByMarketOutcome = 0x21,

    /// Index for orders with some kind of balance.
    ///
    /// (Market's [OutPoint], [Outcome], [OrderIdClientSide]) to ()
    NonZeroOrdersByMarketOutcome = 0x22,

    /// ----- 40-4f reserved for client operation -----

    /// Orders are added to this set when they are known to be out of
    /// date in local db
    ///
    /// ([OrderIdClientSide]) to ()
    OrderNeedsUpdate = 0x40,
}

// Market
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct MarketKey {
    pub market: OutPoint,
}

#[derive(Debug, Encodable, Decodable)]
pub struct MarketPrefixAll;

impl_db_record!(
    key = MarketKey,
    value = Market,
    db_prefix = DbKeyPrefix::Market,
);

impl_db_lookup!(key = MarketKey, query_prefix = MarketPrefixAll);

// Order
#[derive(Debug, Encodable, Decodable)]
pub enum OrderIdSlot {
    Reserved,
    Order(Order),
}

#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct OrderKey {
    pub id: OrderIdClientSide,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OrderPrefixAll;

impl_db_record!(
    key = OrderKey,
    value = OrderIdSlot,
    db_prefix = DbKeyPrefix::Order,
);

impl_db_lookup!(key = OrderKey, query_prefix = OrderPrefixAll);

// OutcomeControlMarkets
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct OutcomeControlMarketsKey {
    pub market: OutPoint,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OutcomeControlMarketsPrefixAll;

impl_db_record!(
    key = OutcomeControlMarketsKey,
    value = (),
    db_prefix = DbKeyPrefix::OutcomeControlMarkets,
);

impl_db_lookup!(
    key = OutcomeControlMarketsKey,
    query_prefix = OutcomeControlMarketsPrefixAll
);

// OrdersByMarketOutcome
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct OrdersByMarketOutcomeKey {
    pub market: OutPoint,
    pub outcome: Outcome,
    pub order: OrderIdClientSide,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OrdersByMarketPrefix1 {
    pub market: OutPoint,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OrdersByMarketPrefix2 {
    pub market: OutPoint,
    pub outcome: Outcome,
}

impl_db_record!(
    key = OrdersByMarketOutcomeKey,
    value = (),
    db_prefix = DbKeyPrefix::OrdersByMarketOutcome,
);

impl_db_lookup!(
    key = OrdersByMarketOutcomeKey,
    query_prefix = OrdersByMarketPrefix1,
    query_prefix = OrdersByMarketPrefix2
);

// NonZeroOrdersByMarketOutcome
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct NonZeroOrdersByMarketOutcomeKey {
    pub market: OutPoint,
    pub outcome: Outcome,
    pub order: OrderIdClientSide,
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

impl_db_record!(
    key = NonZeroOrdersByMarketOutcomeKey,
    value = (),
    db_prefix = DbKeyPrefix::NonZeroOrdersByMarketOutcome,
);

impl_db_lookup!(
    key = NonZeroOrdersByMarketOutcomeKey,
    query_prefix = NonZeroOrdersByMarketOutcomePrefixAll,
    query_prefix = NonZeroOrdersByMarketOutcomePrefix1,
    query_prefix = NonZeroOrdersByMarketOutcomePrefix2
);

// OrderNeedsUpdate
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct OrderNeedsUpdateKey {
    pub order: OrderIdClientSide,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OrderNeedsUpdatePrefixAll;

impl_db_record!(
    key = OrderNeedsUpdateKey,
    value = (),
    db_prefix = DbKeyPrefix::OrderNeedsUpdate,
);

impl_db_lookup!(
    key = OrderNeedsUpdateKey,
    query_prefix = OrderNeedsUpdatePrefixAll
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
