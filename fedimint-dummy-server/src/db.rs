use fedimint_core::encoding::{Decodable, Encodable};

use fedimint_core::{impl_db_lookup, impl_db_record, OutPoint};
use fedimint_dummy_common::{Market, Order, Payout};

use secp256k1::schnorr::Signature;

use serde::Serialize;
use strum_macros::EnumIter;

use crate::PredictionMarketsOutputOutcome;

/// Namespaces DB keys for this module
#[repr(u8)]
#[derive(Clone, EnumIter, Debug)]
pub enum DbKeyPrefix {
    Outcome = 0x01,
    Market = 0x02,
    Order = 0x03,
    Payout = 0x04,
    NextOrderPriority = 0x05,
}

impl std::fmt::Display for DbKeyPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Outcome information
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct OddsMarketsOutcomeKey(pub OutPoint);

#[derive(Debug, Encodable, Decodable)]
pub struct OddsMarketsOutcomePrefix;

impl_db_record!(
    key = OddsMarketsOutcomeKey,
    value = PredictionMarketsOutputOutcome,
    db_prefix = DbKeyPrefix::Outcome,
);

impl_db_lookup!(
    key = OddsMarketsOutcomeKey,
    query_prefix = OddsMarketsOutcomePrefix
);

/// Market information
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct OddsMarketsMarketKey {
    pub market: OutPoint,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OddsMarketsMarketPrefix;

impl_db_record!(
    key = OddsMarketsMarketKey,
    value = Market,
    db_prefix = DbKeyPrefix::Market,
);

impl_db_lookup!(
    key = OddsMarketsMarketKey,
    query_prefix = OddsMarketsMarketPrefix
);

/// Order information
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct OddsMarketsOrderKey {
    pub market: OutPoint,
    pub order: OutPoint,
}

#[derive(Debug, Encodable, Decodable)]
pub struct OddsMarketsOrderPrefixAll;

#[derive(Debug, Encodable, Decodable)]
pub struct OddsMarketsOrderPrefixMarket {
    pub market: OutPoint,
}

impl_db_record!(
    key = OddsMarketsOrderKey,
    value = Order,
    db_prefix = DbKeyPrefix::Order,
);

impl_db_lookup!(
    key = OddsMarketsOrderKey,
    query_prefix = OddsMarketsOrderPrefixAll,
    query_prefix = OddsMarketsOrderPrefixMarket
);

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
