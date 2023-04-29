use fedimint_cli::FedimintCli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    FedimintCli::new()?
        .with_default_modules()
        .with_module(fedimint_starter_client::StarterClientGen)
        .run()
        .await;
    Ok(())
}
