use async_stream::{stream, try_stream};
use fedimint_core::util::BoxStream;
use futures::StreamExt;

use crate::PredictionMarketsClientModule;

pub async fn handle_rpc(
    prediction_markets: &PredictionMarketsClientModule,
    method: String,
    request: serde_json::Value,
) -> BoxStream<'_, anyhow::Result<serde_json::Value>> {
    todo!()
}
