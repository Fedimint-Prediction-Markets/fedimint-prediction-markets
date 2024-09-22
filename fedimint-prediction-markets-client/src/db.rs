use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::{impl_db_lookup, impl_db_record, OutPoint};
use fedimint_prediction_markets_common::{
    Market, NostrPublicKeyHex, Order, Outcome, UnixTimestamp,
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
    /// (Market's [OutPoint], [Outcome], [OrderId]) to ()
    OrdersByMarketOutcome = 0x21,

    /// Orders with some kind of balance.
    ///
    /// (Market's [OutPoint], [Outcome], [OrderId]) to ()
    NonZeroOrdersByMarketOutcome = 0x22,

    /// Order ids are added to this set when the order in local db is known
    /// to be out of sync with the order on server
    ///
    /// ([OrderId]) to ()
    OrderNeedsUpdate = 0x40,

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
#[derive(Debug, Encodable, Decodable)]
pub enum OrderIdSlot {
    Reserved,
    Order(Order),
}

#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct OrderKey(pub OrderId);

#[derive(Debug, Encodable, Decodable)]
pub struct OrderPrefixAll;

impl_db_record!(
    key = OrderKey,
    value = OrderIdSlot,
    db_prefix = DbKeyPrefix::Order,
);

impl_db_lookup!(key = OrderKey, query_prefix = OrderPrefixAll);

// OrdersByMarketOutcome
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct OrdersByMarketOutcomeKey {
    pub market: OutPoint,
    pub outcome: Outcome,
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

impl_db_record!(
    key = OrdersByMarketOutcomeKey,
    value = (),
    db_prefix = DbKeyPrefix::OrdersByMarketOutcome,
);

impl_db_lookup!(
    key = OrdersByMarketOutcomeKey,
    query_prefix = OrdersByMarketOutcomePrefixAll,
    query_prefix = OrdersByMarketOutcomePrefix1,
    query_prefix = OrdersByMarketOutcomePrefix2
);

// NonZeroOrdersByMarketOutcome
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash)]
pub struct NonZeroOrdersByMarketOutcomeKey {
    pub market: OutPoint,
    pub outcome: Outcome,
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
pub struct OrderNeedsUpdateKey(pub OrderId);

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
