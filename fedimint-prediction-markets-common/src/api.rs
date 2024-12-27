use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::{Amount, OutPoint};
use secp256k1::PublicKey;
use serde::{Deserialize, Serialize};

use crate::{
    Candlestick, ContractOfOutcomeAmount, Market, MarketDynamic, NostrEventJson, Order, Outcome, Seconds, UnixTimestamp
};

//
// Get Market
//

pub const GET_MARKET_ENDPOINT: &str = "get_market";
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetMarketParams {
    pub market: OutPoint,
}
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetMarketResult {
    pub market: Option<Market>,
}

//
// Get Market Dynamic
//

pub const GET_MARKET_DYNAMIC_ENDPOINT: &str = "get_market_dynamic";
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetMarketDynamicParams {
    pub market: OutPoint,
}
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetMarketDynamicResult {
    pub market_dynamic: Option<MarketDynamic>,
}

//
// Get Event Payout Attestation Vec
//

pub const GET_EVENT_PAYOUT_ATTESTATIONS_USED_TO_PERMIT_PAYOUT_ENDPOINT: &str =
    "get_event_payout_attestations_used_to_permit_payout";
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetEventPayoutAttestationsUsedToPermitPayoutParams {
    pub market: OutPoint,
}
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetEventPayoutAttestationsUsedToPermitPayoutResult {
    pub event_payout_attestations: Option<Vec<NostrEventJson>>,
}

//
// Get Order
//

pub const GET_ORDER_ENDPOINT: &str = "get_order";
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetOrderParams {
    pub order: PublicKey,
}
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetOrderResult {
    pub order: Option<Order>,
}

//
// Wait Order Match
//
pub const WAIT_ORDER_MATCH_ENDPOINT: &str = "wait_order_match";
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct WaitOrderMatchParams {
    pub order: PublicKey,
    pub current_quantity_waiting_for_match: ContractOfOutcomeAmount,
}
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct WaitOrderMatchResult {
    pub order: Order,
}

//
// Get Market Outcome Candlesticks
//

pub const GET_MARKET_OUTCOME_CANDLESTICKS_ENDPOINT: &str = "get_market_outcome_candlesticks";
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
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

pub const WAIT_MARKET_OUTCOME_CANDLESTICKS_ENDPOINT: &str = "wait_market_outcome_candlesticks";
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
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
// Get Market Outcome Order Book
//

pub const GET_MARKET_OUTCOME_ORDER_BOOK_ENDPOINT: &str = "get_market_outcome_order_book";
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetMarketOutcomeOrderBookParams {
    pub market: OutPoint,
    pub outcome: Outcome,
}
#[derive(Debug, Clone, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct GetMarketOutcomeOrderBookResult {
    pub buys: Vec<(Amount, ContractOfOutcomeAmount)>,
    pub sells: Vec<(Amount, ContractOfOutcomeAmount)>,
}