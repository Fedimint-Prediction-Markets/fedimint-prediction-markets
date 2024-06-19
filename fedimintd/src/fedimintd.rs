use fedimint_core::core::ModuleKind;
use fedimint_core::fedimint_build_code_version_env;
use fedimint_dummy_common::config::DummyGenParams;
use fedimint_dummy_server::DummyInit;
use fedimintd::Fedimintd;

const KIND: ModuleKind = ModuleKind::from_static_str("dummy");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Fedimintd::new(fedimint_build_code_version_env!())?
        .with_default_modules()
        .with_module_kind(fedimint_prediction_markets_server::PredictionMarketsInit)
        .with_module_instance(
            fedimint_prediction_markets_common::KIND,
            fedimint_prediction_markets_common::config::PredictionMarketsGenParams::default(),
        )
        .run()
        .await
}
