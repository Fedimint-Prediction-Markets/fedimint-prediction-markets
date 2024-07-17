use std::collections::BTreeMap;

use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::{Amount, OutPoint};
use secp256k1::PublicKey;
use serde::{Deserialize, Serialize};

use crate::{Candlestick, ContractOfOutcomeAmount, Market, Order, Outcome, Seconds, UnixTimestamp};

//
// Get Market
//

pub const GET_MARKET: &str = "get_market";
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetMarketParams {
    pub market: OutPoint,
}
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetMarketResult {
    pub market: Option<Market>,
}

//
// Get Order
//

pub const GET_ORDER: &str = "get_order";
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetOrderParams {
    pub order: PublicKey,
}
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetOrderResult {
    pub order: Option<Order>,
}

//
// Get Payout Control Markets
//

pub const GET_PAYOUT_CONTROL_MARKETS: &str = "get_payout_control_markets";
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetPayoutControlMarketsParams {
    pub payout_control: PublicKey,
    pub markets_created_after_and_including: UnixTimestamp,
}
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetPayoutControlMarketsResult {
    pub markets: Vec<OutPoint>,
}

//
// Get Market Payout Control Proposals
//

pub const GET_MARKET_PAYOUT_CONTROL_PROPOSALS: &str = "get_market_payout_control_proposals";
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetMarketPayoutControlProposalsParams {
    pub market: OutPoint,
}
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetMarketPayoutControlProposalsResult {
    pub payout_control_proposals: BTreeMap<PublicKey, Vec<Amount>>,
}

//
// Get Market Outcome Candlesticks
//

pub const GET_MARKET_OUTCOME_CANDLESTICKS: &str = "get_market_outcome_candlesticks";
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

//
// Wait Market Outcome Candlesticks
//

pub const WAIT_MARKET_OUTCOME_CANDLESTICKS: &str = "wait_market_outcome_candlesticks";
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct WaitMarketOutcomeCandlesticksParams {
    pub market: OutPoint,
    pub outcome: Outcome,
    pub candlestick_interval: Seconds,
    pub candlestick_timestamp: UnixTimestamp,
    pub candlestick_volume: ContractOfOutcomeAmount,
}
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct WaitMarketOutcomeCandlesticksResult {
    pub candlesticks: Vec<(UnixTimestamp, Candlestick)>,
}

//
// Get Payout Control Balances
//

pub const GET_PAYOUT_CONTROL_BALANCE: &str = "get_payout_control_balance";
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetPayoutControlBalanceParams {
    pub payout_control: PublicKey,
}
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetPayoutControlBalanceResult {
    pub balance: Amount,
}
