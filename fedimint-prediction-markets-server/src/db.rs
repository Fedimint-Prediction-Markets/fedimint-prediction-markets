use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::{impl_db_lookup, impl_db_record, OutPoint, PeerId};
use fedimint_prediction_markets_common::{
    Candlestick, ContractOfOutcomeAmount, MarketDynamic, MarketStatic, NostrEventJson, Order,
    PredictionMarketsOutputOutcome, Seconds, Side, TimeOrdering, UnixTimestamp,
};
use prediction_market_event::Outcome;
use secp256k1::PublicKey;
use serde::Serialize;
use strum_macros::EnumIter;

use crate::MarketSpecificationsNeededForNewOrders;

/// Namespaces DB keys for this module
#[repr(u8)]
#[derive(Clone, EnumIter, Debug)]
pub enum DbKeyPrefix {
    /// [OutPoint] to [PredictionMarketsOutputOutcome]
    Outcome = 0x00,

    /// Market's [OutPoint] to [MarketStatic]
    MarketStatic = 0x01,

    /// Market's [OutPoint] to [MarketDynamic]
    MarketDynamic = 0x02,

    /// Owner's [PublicKey] to [Order]
    Order = 0x03,

    /// Information needed to process new orders
    ///
    /// Market's [OutPoint] to [MarketSpecificationsNeededForNewOrders]
    MarketSpecificationsNeededForNewOrders = 0x20,

    /// Used for payouts
    ///
    /// (Market's [OutPoint], Order's [OutPoint]) to ()
    OrdersByMarket = 0x21,

    /// Used to implement orderbook. Only holds orders with non-zero
    /// quantity_waiting_for_match.
    ///
    /// Amount is (contract_price - price of order) for buys
    /// Amount is (price of order) for sells
    ///
    /// (Market's [OutPoint], [Outcome], [Side], Price priority [u64],
    /// [TimeOrdering]) to (Order's [PublicKey])
    OrderPriceTimePriority = 0x22,

    /// (Market's [OutPoint]) to
    /// (Vec<[prediction_market_event::nostr::EventPayoutAttestation] as json>)
    EventPayoutAttestationsUsedToPermitPayout = 0x23,

    /// Used to implement candlestick data
    ///
    /// (Market's [OutPoint], [Outcome], candlestick interval [Seconds],
    /// Candle's [UnixTimestamp]) to [Candlestick]
    MarketOutcomeCandlesticks = 0x24,
    /// (Market's [OutPoint], [Outcome], candlestick interval [Seconds]) to
    /// (Candle's [UnixTimestamp], [ContractOfOutcomeAmount])
    MarketOutcomeNewestCandlestickVolume = 0x25,

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

/// MarketStatic
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct MarketStaticKey(pub OutPoint);

#[derive(Debug, Encodable, Decodable)]
pub struct MarketStaticPrefixAll;

impl_db_record!(
    key = MarketStaticKey,
    value = MarketStatic,
    db_prefix = DbKeyPrefix::MarketStatic,
);

impl_db_lookup!(key = MarketStaticKey, query_prefix = MarketStaticPrefixAll);

/// MarketDynamic
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct MarketDynamicKey(pub OutPoint);

#[derive(Debug, Encodable, Decodable)]
pub struct MarketDynamicPrefixAll;

impl_db_record!(
    key = MarketDynamicKey,
    value = MarketDynamic,
    db_prefix = DbKeyPrefix::MarketDynamic,
);

impl_db_lookup!(
    key = MarketDynamicKey,
    query_prefix = MarketDynamicPrefixAll
);

/// Order
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct OrderKey(pub PublicKey);

#[derive(Debug, Encodable, Decodable)]
pub struct OrderPrefixAll;

impl_db_record!(
    key = OrderKey,
    value = Order,
    db_prefix = DbKeyPrefix::Order,
);

impl_db_lookup!(key = OrderKey, query_prefix = OrderPrefixAll,);

/// MarketSpecificationsNeededForNewOrders
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct MarketSpecificationsNeededForNewOrdersKey(pub OutPoint);

#[derive(Debug, Encodable, Decodable)]
pub struct MarketSpecificationsNeededForNewOrdersPrefixAll;

impl_db_record!(
    key = MarketSpecificationsNeededForNewOrdersKey,
    value = MarketSpecificationsNeededForNewOrders,
    db_prefix = DbKeyPrefix::MarketSpecificationsNeededForNewOrders,
);

impl_db_lookup!(
    key = MarketSpecificationsNeededForNewOrdersKey,
    query_prefix = MarketSpecificationsNeededForNewOrdersPrefixAll
);

/// OrdersByMarket
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct OrdersByMarketKey {
    pub market: OutPoint,
    pub order: PublicKey,
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
    value = PublicKey,
    db_prefix = DbKeyPrefix::OrderPriceTimePriority,
);

impl_db_lookup!(
    key = OrderPriceTimePriorityKey,
    query_prefix = OrderPriceTimePriorityPrefix3,
    query_prefix = OrderPriceTimePriorityPrefixAll
);

/// EventPayoutAttestationsUsedToPermitPayout
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct EventPayoutAttestationsUsedToPermitPayoutKey(pub OutPoint);

#[derive(Debug, Encodable, Decodable)]
pub struct EventPayoutAttestationsUsedToPermitPayoutPrefixAll;

impl_db_record!(
    key = EventPayoutAttestationsUsedToPermitPayoutKey,
    value = Vec<NostrEventJson>,
    db_prefix = DbKeyPrefix::EventPayoutAttestationsUsedToPermitPayout,
);

impl_db_lookup!(
    key = EventPayoutAttestationsUsedToPermitPayoutKey,
    query_prefix = EventPayoutAttestationsUsedToPermitPayoutPrefixAll
);

/// MarketOutcomeCandlesticks
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

/// MarketOutcomeNewestCandlestickVolume
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
