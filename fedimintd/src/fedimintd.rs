use fedimintd::fedimintd::Fedimintd;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Fedimintd::new()?
        .with_default_modules()
        .with_module(fedimint_prediction_markets_server::PredictionMarketsGen)
        .with_extra_module_inits_params(
            3,
            fedimint_prediction_markets_server::KIND,
            fedimint_prediction_markets_server::PredictionMarketsGenParams::default(),
        )
        .run()
        .await
}
