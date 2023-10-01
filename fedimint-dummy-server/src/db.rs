use fedimint_core::encoding::{Decodable, Encodable};

use fedimint_core::{impl_db_lookup, impl_db_record, OutPoint};
use fedimint_dummy_common::{Market, Payout};

use secp256k1::schnorr::Signature;

use serde::Serialize;
use strum_macros::EnumIter;

use crate::OddsMarketsOutputOutcome;

/// Namespaces DB keys for this module
#[repr(u8)]
#[derive(Clone, EnumIter, Debug)]
pub enum DbKeyPrefix {
    OutPoint = 0x01,
    Market = 0x02,
    Payout = 0x03,
}

impl std::fmt::Display for DbKeyPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Market information
#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct OddsMarketsOutPointKey(pub OutPoint);

#[derive(Debug, Encodable, Decodable)]
pub struct OddsMarketsOutPointPrefix;

impl_db_record!(
    key = OddsMarketsOutPointKey,
    value = OddsMarketsOutputOutcome,
    db_prefix = DbKeyPrefix::OutPoint,
);

impl_db_lookup!(
    key = OddsMarketsOutPointKey,
    query_prefix = OddsMarketsOutPointPrefix
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
