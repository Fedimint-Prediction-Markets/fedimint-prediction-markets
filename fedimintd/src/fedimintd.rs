use fedimint_core::fedimint_build_code_version_env;
use fedimintd::Fedimintd;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Fedimintd::new(fedimint_build_code_version_env!(), None)?
        .with_default_modules()
        .with_module_kind(fedimint_prediction_markets_server::PredictionMarketsInit)
        .with_module_instance(
            fedimint_prediction_markets_common::KIND,
            fedimint_prediction_markets_common::config::PredictionMarketsGenParams::default(),
        )
        .run()
        .await
}
