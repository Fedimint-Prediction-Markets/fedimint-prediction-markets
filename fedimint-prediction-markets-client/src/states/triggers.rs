use std::{collections::BTreeMap, time::Duration};

use fedimint_client::DynGlobalClientContext;
use fedimint_core::{task::sleep, OutPoint};
use fedimint_prediction_markets_common::{api::{GetMarketParams, GetMarketResult, GetOrderParams, GetOrderResult}, Market, Order};
use futures::{stream::FuturesUnordered, StreamExt};
use secp256k1::PublicKey;

use crate::{api::PredictionMarketsFederationApi, OrderId};

pub const RETRY_DELAY: Duration = Duration::from_secs(5);

pub async fn await_order_from_federation(global_context: DynGlobalClientContext, order: PublicKey) -> Order {
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
    global_context: DynGlobalClientContext,
    orders: BTreeMap<OrderId, PublicKey>,
) -> BTreeMap<OrderId, Order> {
    orders
        .into_iter()
        .map(|(order_id, order_owner)| {
            let global_context = global_context.clone();

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

pub async fn await_market_from_federation(global_context: DynGlobalClientContext, market: OutPoint) -> Market {
    loop {
        let res = global_context
            .module_api()
            .get_market(GetMarketParams{ market })
            .await;

        if let Ok(GetMarketResult{ market: Some(market) }) = res {
            return market;
        }

        sleep(RETRY_DELAY).await;
    }
}