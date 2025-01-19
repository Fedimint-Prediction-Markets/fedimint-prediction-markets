use std::collections::BTreeMap;
use std::time::Duration;

use async_stream::try_stream;
use fedimint_core::util::BoxStream;
use fedimint_core::{Amount, OutPoint};
use fedimint_prediction_markets_common::{
    ContractOfOutcomeAmount, NostrPublicKeyHex, PredictionMarketEventJson, Seconds, Side,
    UnixTimestamp, Weight, WeightRequiredForPayout,
};
use futures::StreamExt;
use prediction_market_event::Outcome;
use serde::Deserialize;
use serde_json::json;

use crate::order_filter::{OrderFilter, OrderPath};
use crate::{OrderId, PredictionMarketsClientModule};

pub async fn handle_rpc(
    prediction_markets: &PredictionMarketsClientModule,
    method: String,
    request: serde_json::Value,
) -> BoxStream<'_, anyhow::Result<serde_json::Value>> {
    Box::pin(try_stream! { match method.as_str() {
        "get_general_consensus" => {
            let res = prediction_markets.get_general_consensus();
            yield json!(res);
        }
        "new_market" => {
            let req = serde_json::from_value::<NewMarketRequest>(request)?;
            let res = prediction_markets.new_market(req.event_json, req.contract_price, req.payout_control_weight_map, req.weight_required_for_payout).await?;
            yield json!(res);
        }
        "get_market" => {
            let req = serde_json::from_value::<GetMarketRequest>(request)?;
            let res = prediction_markets.get_market(req.market, req.from_local_cache).await?;
            yield json!(res);
        }
        "payout_market" => {
            let req = serde_json::from_value::<PayoutMarketRequest>(request)?;
            let res = prediction_markets.payout_market(req.market, req.event_payout_attestations_json).await?;
            yield json!(res);
        }
        "get_event_payout_attestations_used_to_permit_payout" => {
            let req = serde_json::from_value::<GetEventPayoutAttestationsUsedToPermitPayoutRequest>(request)?;
            let res = prediction_markets.get_event_payout_attestations_used_to_permit_payout(req.market).await?;
            yield json!(res);
        }
        "new_order" => {
            let req = serde_json::from_value::<NewOrderRequest>(request)?;
            let res = prediction_markets.new_order(req.market, req.outcome, req.side, req.price, req.quantity).await?;
            yield json!(res);
        }
        "get_order" => {
            let req = serde_json::from_value::<GetOrderRequest>(request)?;
            let res = prediction_markets.get_order(req.order_id, req.from_local_cache).await?;
            yield json!(res);
        }
        "get_orders_from_db" => {
            let req = serde_json::from_value::<GetOrdersFromDbRequest>(request)?;
            let res = prediction_markets.get_orders_from_db(req.filter).await;
            yield json!(res);
        }
        "stream_order_from_db" => {
            let req = serde_json::from_value::<StreamOrderFromDbRequest>(request)?;
            let mut stream = prediction_markets.stream_order_from_db(req.id).await;
            while let Some(res) = stream.next().await {
                yield json!(res);
            }
        }
        "stream_order_ids" => {
            let req = serde_json::from_value::<StreamOrderIdsRequest>(request)?;
            let mut stream = prediction_markets.stream_order_ids(req.filter).await;
            while let Some(res) = stream.next().await {
                yield json!(res);
            }
        }
        "cancel_order" => {
            let req = serde_json::from_value::<CancelOrderRequest>(request)?;
            let res = prediction_markets.cancel_order(req.order_id).await?;
            yield json!(res);
        }
        "send_order_bitcoin_balance_to_primary_module" => {
            let res = prediction_markets.send_order_bitcoin_balance_to_primary_module().await?;
            yield json!(res);
        }
        "sync_payouts" => {
            let req = serde_json::from_value::<SyncPayoutsRequest>(request)?;
            let res = prediction_markets.sync_payouts(req.market_specifier).await?;
            yield json!(res);
        }
        "sync_matches" => {
            let req = serde_json::from_value::<SyncMatchesRequest>(request)?;
            let res = prediction_markets.sync_matches(req.order_path).await?;
            yield json!(res);
        }
        "start_watch_matches" => {
            let req = serde_json::from_value::<StartWatchMatchesRequest>(request)?;
            let res = prediction_markets.start_watch_matches(req.order_path).await?;
            yield json!(res);
        }
        "stop_watch_matches" => {
            let req = serde_json::from_value::<StopWatchMatchesRequest>(request)?;
            let res = prediction_markets.stop_watch_matches(req.id).await?;
            yield json!(res);
        }
        "resync_order_slots" => {
            let req = serde_json::from_value::<ResyncOrderSlotsRequest>(request)?;
            let res = prediction_markets.resync_order_slots(req.gap_size_to_check).await?;
            yield json!(res);
        }
        "get_candlesticks" => {
            let req = serde_json::from_value::<GetCandlesticksRequest>(request)?;
            let res = prediction_markets.get_candlesticks(req.market, req.outcome, req.candlestick_interval, req.min_candlestick_timestamp).await?;
            yield json!(res);
        }
        "wait_candlesticks" => {
            let req = serde_json::from_value::<WaitCandlesticksRequest>(request)?;
            let res = prediction_markets.wait_candlesticks(req.market, req.outcome, req.candlestick_interval, req.candlestick_timestamp, req.candlestick_volume).await?;
            yield json!(res);
        }
        "stream_candlesticks" => {
            let req = serde_json::from_value::<StreamCandlesticksRequest>(request)?;
            let mut stream = prediction_markets.stream_candlesticks(req.market, req.outcome, req.candlestick_interval, req.min_candlestick_timestamp, req.min_duration_between_requests).await;
            while let Some(res) = stream.next().await {
                yield json!(res);
            }
        }
        "get_order_book" => {
            let req = serde_json::from_value::<GetOrderBookRequest>(request)?;
            let res = prediction_markets.get_order_book(req.market, req.outcome).await?;
            yield json!(res);
        }
        "save_market" => {
            let req = serde_json::from_value::<SaveMarketRequest>(request)?;
            let res = prediction_markets.save_market(req.market).await;
            yield json!(res);
        }
        "unsave_market" => {
            let req = serde_json::from_value::<UnsaveMarketRequest>(request)?;
            let res = prediction_markets.unsave_market(req.market).await;
            yield json!(res);
        }
        "get_saved_markets" => {
            let res = prediction_markets.get_saved_markets().await;
            yield json!(res);        
        }
        "set_name_to_payout_control" => {
            let req = serde_json::from_value::<SetNameToPayoutControlRequest>(request)?;
            let res = prediction_markets.set_name_to_payout_control(req.name, req.payout_control).await;
            yield json!(res);
        }
        "get_name_to_payout_control" => {
            let req = serde_json::from_value::<GetNameToPayoutControlRequest>(request)?;
            let res = prediction_markets.get_name_to_payout_control(req.name).await;
            yield json!(res);
        }
        "get_name_to_payout_control_map" => {
            let res = prediction_markets.get_name_to_payout_control_map().await;
            yield json!(res);
        }
        _ => {
            Err(anyhow::format_err!("unknown method"))?;
            unreachable!();
        }
    }})
}

#[derive(Deserialize)]
pub struct NewMarketRequest {
    event_json: PredictionMarketEventJson,
    contract_price: Amount,
    payout_control_weight_map: BTreeMap<NostrPublicKeyHex, Weight>,
    weight_required_for_payout: WeightRequiredForPayout,
}

#[derive(Deserialize)]
pub struct GetMarketRequest {
    market: OutPoint,
    from_local_cache: bool,
}

#[derive(Deserialize)]
pub struct PayoutMarketRequest {
    market: OutPoint,
    event_payout_attestations_json: Vec<PredictionMarketEventJson>,
}

#[derive(Deserialize)]
pub struct GetEventPayoutAttestationsUsedToPermitPayoutRequest {
    market: OutPoint,
}

#[derive(Deserialize)]
pub struct NewOrderRequest {
    market: OutPoint,
    outcome: Outcome,
    side: Side,
    price: Amount,
    quantity: ContractOfOutcomeAmount,
}

#[derive(Deserialize)]
pub struct GetOrderRequest {
    order_id: OrderId,
    from_local_cache: bool,
}

#[derive(Deserialize)]
pub struct GetOrdersFromDbRequest {
    filter: OrderFilter,
}

#[derive(Deserialize)]
pub struct StreamOrderFromDbRequest {
    id: OrderId,
}

#[derive(Deserialize)]
pub struct StreamOrderIdsRequest {
    filter: OrderFilter,
}

#[derive(Deserialize)]
pub struct CancelOrderRequest {
    order_id: OrderId,
}

#[derive(Deserialize)]
pub struct SyncPayoutsRequest {
    market_specifier: Option<OutPoint>,
}

#[derive(Deserialize)]
pub struct SyncMatchesRequest {
    order_path: OrderPath,
}

#[derive(Deserialize)]
pub struct StartWatchMatchesRequest {
    order_path: OrderPath,
}

#[derive(Deserialize)]
pub struct StopWatchMatchesRequest {
    id: u64,
}

#[derive(Deserialize)]
pub struct ResyncOrderSlotsRequest {
    gap_size_to_check: usize,
}

#[derive(Deserialize)]
pub struct GetCandlesticksRequest {
    market: OutPoint,
    outcome: Outcome,
    candlestick_interval: Seconds,
    min_candlestick_timestamp: UnixTimestamp,
}

#[derive(Deserialize)]
pub struct WaitCandlesticksRequest {
    market: OutPoint,
    outcome: Outcome,
    candlestick_interval: Seconds,
    candlestick_timestamp: UnixTimestamp,
    candlestick_volume: ContractOfOutcomeAmount,
}

#[derive(Deserialize)]
pub struct StreamCandlesticksRequest {
    market: OutPoint,
    outcome: Outcome,
    candlestick_interval: Seconds,
    min_candlestick_timestamp: UnixTimestamp,
    min_duration_between_requests: Duration,
}

#[derive(Deserialize)]
pub struct GetOrderBookRequest {
    market: OutPoint,
    outcome: Outcome,
}

#[derive(Deserialize)]
pub struct SaveMarketRequest {
    market: OutPoint,
}

#[derive(Deserialize)]
pub struct UnsaveMarketRequest {
    market: OutPoint,
}

#[derive(Deserialize)]
pub struct SetNameToPayoutControlRequest {
    name: String,
    payout_control: Option<NostrPublicKeyHex>,
}

#[derive(Deserialize)]
pub struct GetNameToPayoutControlRequest {
    name: String,
}
