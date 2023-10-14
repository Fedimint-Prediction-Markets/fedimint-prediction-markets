use fedimint_core::encoding::{Decodable, Encodable};

use fedimint_core::{impl_db_lookup, impl_db_record, OutPoint};
use fedimint_dummy_common::{Market, Order, Payout};

use secp256k1::schnorr::Signature;

use serde::Serialize;
use strum_macros::EnumIter;

use crate::{
    PredictionMarketsOutput, PredictionMarketsOutput::NewMarket, PredictionMarketsOutput::NewOrder,
    PredictionMarketsOutputOutcome,
};

/// Namespaces DB keys for this module
#[repr(u8)]
#[derive(Clone, EnumIter, Debug)]
pub enum DbKeyPrefix {
    /// [PredictionMarketsOutput] [OutPoint] to [PredictionMarketsOutputOutcome]
    Outcome = 0x01,

    /// [NewMarket] [OutPoint] to [Market]
    Market = 0x02,

    /// [NewOrder] [OutPoint] to [Order]
    Order = 0x03,

    NextOrderPriority = 0x05,
}

struct Test;

impl std::fmt::Display for DbKeyPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// OutputToOutcomeStatus
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct OutputToOutcomeStatusKey(pub OutPoint);

#[derive(Debug, Encodable, Decodable)]
pub struct OutputToOutcomeStatusPrefix;

impl_db_record!(
    key = OutputToOutcomeStatusKey,
    value = PredictionMarketsOutputOutcome,
    db_prefix = DbKeyPrefix::Outcome,
);

impl_db_lookup!(
    key = OutputToOutcomeStatusKey,
    query_prefix = OutputToOutcomeStatusPrefix
);

/// Market
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct MarketKey(pub OutPoint);

#[derive(Debug, Encodable, Decodable)]
pub struct MarketPrefix;

impl_db_record!(
    key = MarketKey,
    value = Market,
    db_prefix = DbKeyPrefix::Market,
);

impl_db_lookup!(key = MarketKey, query_prefix = MarketPrefix);

/// Order
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct OrderKey(pub OutPoint);

#[derive(Debug, Encodable, Decodable)]
pub struct OrderPrefix;

impl_db_record!(
    key = OrderKey,
    value = Order,
    db_prefix = DbKeyPrefix::Order,
);

impl_db_lookup!(key = OrderKey, query_prefix = OrderPrefix,);

// Payout information
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct OddsMarketsPayoutKey {
    pub market: OutPoint,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OddsMarketsPayoutPrefix;

impl_db_record!(
    key = OddsMarketsPayoutKey,
    value = (Payout, Signature),
    db_prefix = DbKeyPrefix::Payout,
);

impl_db_lookup!(
    key = OddsMarketsPayoutKey,
    query_prefix = OddsMarketsPayoutPrefix
);

// Next order priority
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct OddsMarketsNextOrderPriorityKey {
    pub market: OutPoint,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OddsMarketsNextOrderPriorityPrefix;

impl_db_record!(
    key = OddsMarketsNextOrderPriorityKey,
    value = u64,
    db_prefix = DbKeyPrefix::NextOrderPriority,
);

impl_db_lookup!(
    key = OddsMarketsNextOrderPriorityKey,
    query_prefix = OddsMarketsNextOrderPriorityPrefix
);
