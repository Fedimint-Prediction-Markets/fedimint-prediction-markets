use fedimintd::fedimintd::Fedimintd;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Fedimintd::new()?
        .with_default_modules()
        .with_module(fedimint_prediction_markets_server::PredictionMarketsInit)
        .with_extra_module_inits_params(
            3,
            fedimint_prediction_markets_common::KIND,
            fedimint_prediction_markets_common::config::PredictionMarketsGenParams::default(),
        )
        .run()
        .await
}
