use fedimint_core::encoding::{Decodable, Encodable};
use secp256k1::XOnlyPublicKey;
use serde::{Deserialize, Serialize};
use fedimint_core::OutPoint;

use crate::{Candlestick, Outcome, Seconds, UnixTimestamp, ContractOfOutcomeAmount};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetPayoutControlMarketsParams {
    pub payout_control: XOnlyPublicKey,
    pub markets_created_after_and_including: UnixTimestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetPayoutControlMarketsResult {
    pub markets: Vec<OutPoint>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetMarketOutcomeCandlesticksParams {
    pub market: OutPoint,
    pub outcome: Outcome,
    pub candlestick_interval: Seconds,
    pub min_candlestick_timestamp: UnixTimestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetMarketOutcomeCandlesticksResult {
    pub candlesticks: Vec<(UnixTimestamp, Candlestick)>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct WaitMarketOutcomeCandlesticksParams {
    pub market: OutPoint,
    pub outcome: Outcome,
    pub candlestick_interval: Seconds,
    pub candlestick_timestamp: UnixTimestamp,
    pub candlestick_volume: ContractOfOutcomeAmount
}

#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct WaitMarketOutcomeCandlesticksResult {
    pub candlesticks: Vec<(UnixTimestamp, Candlestick)>,
}
