use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use fedimint_client::DynGlobalClientContext;
use fedimint_core::task::sleep;
use fedimint_core::OutPoint;
use fedimint_prediction_markets_common::api::{
    GetMarketParams, GetMarketResult, GetOrderParams, GetOrderResult,
};
use fedimint_prediction_markets_common::{Market, Order};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use secp256k1::PublicKey;

use crate::api::PredictionMarketsFederationApi;
use crate::{OrderId, PredictionMarketsClientContext};

pub const RETRY_DELAY: Duration = Duration::from_secs(5);

pub async fn await_order_from_federation(
    global_context: DynGlobalClientContext,
    order: PublicKey,
) -> Order {
    loop {
        let res = global_context
            .module_api()
            .get_order(GetOrderParams { order })
            .await;

        if let Ok(GetOrderResult { order: Some(order) }) = res {
            return order;
        }

        sleep(RETRY_DELAY).await;
    }
}

pub async fn await_orders_from_federation(
    context: PredictionMarketsClientContext,
    global_context: DynGlobalClientContext,
    orders: BTreeSet<OrderId>,
) -> BTreeMap<OrderId, Order> {
    orders
        .into_iter()
        .map(|order_id| {
            let global_context = global_context.clone();
            let order_owner = order_id
                .into_key_pair(context.root_secret.clone())
                .public_key();

            async move {
                (
                    order_id,
                    await_order_from_federation(global_context, order_owner).await,
                )
            }
        })
        .collect::<FuturesUnordered<_>>()
        .collect()
        .await
}

pub async fn await_market_from_federation(
    global_context: DynGlobalClientContext,
    market: OutPoint,
) -> Market {
    loop {
        let res = global_context
            .module_api()
            .get_market(GetMarketParams { market })
            .await;

        if let Ok(GetMarketResult {
            market: Some(market),
        }) = res
        {
            return market;
        }

        sleep(RETRY_DELAY).await;
    }
}
