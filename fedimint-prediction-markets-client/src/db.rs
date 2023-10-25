use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::{impl_db_lookup, impl_db_record, OutPoint};
use fedimint_prediction_markets_common::{Order, Market, OrderIDClientSide, Outcome};

#[repr(u8)]
#[derive(Clone, Debug)]
pub enum DbKeyPrefix {
    /// ----- 00-1f reserved for struct storage -----
    
    /// Market's [OutPoint] to [Market]
    Market = 0x01,

    /// ChildId of order root secret to [Order]
    Order = 0x02,

    /// ----- 20-3f reserved for lookup indexes -----
    
    /// Markets that our outcome control key has some portion of control over to ()
    OutcomeControlMarkets = 0x20,

    /// (Market's [OutPoint], [OrderIDClientSide]) to ()
    OrdersByMarketOutcome = 0x21
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

impl_db_lookup!(
    key = MarketKey,
    query_prefix = MarketPrefixAll
);

// Order
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct OrderKey {
    pub id: OrderIDClientSide,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OrderPrefixAll;

impl_db_record!(
    key = OrderKey,
    value = Order,
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
    pub order: OrderIDClientSide
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
