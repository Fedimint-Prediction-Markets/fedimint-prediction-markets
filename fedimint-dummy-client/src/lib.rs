use std::ffi;
use std::sync::Arc;

use common::Market;
use fedimint_client::derivable_secret::DerivableSecret;
use fedimint_client::module::init::ClientModuleInit;
use fedimint_client::module::{ClientModule, IClientModule};
use fedimint_client::sm::{Context, ModuleNotifier, OperationId};
use fedimint_client::transaction::{ClientOutput, TransactionBuilder};
use fedimint_client::{Client, DynGlobalClientContext};
use fedimint_core::api::{DynGlobalApi, DynModuleApi};
use fedimint_core::config::FederationId;
use fedimint_core::core::{Decoder, IntoDynInstance, KeyPair};
use fedimint_core::db::{Database, ModuleDatabaseTransaction};
use fedimint_core::module::{
    ApiVersion, CommonModuleInit, ExtendsCommonModuleInit, ModuleCommon, MultiApiVersion,
    TransactionItemAmount,
};

use fedimint_core::{apply, async_trait_maybe_send, Amount, OutPoint};
pub use fedimint_dummy_common as common;
use fedimint_dummy_common::config::OddsMarketsClientConfig;
use fedimint_dummy_common::{
    OddsMarketsCommonGen, OddsMarketsInput, OddsMarketsModuleTypes, OddsMarketsOutput, KIND,
};

use secp256k1::{Secp256k1, XOnlyPublicKey};
use states::OddsMarketsStateMachine;

use crate::db::DummyClientFundsKeyV0;

pub mod api;
mod db;
mod states;

/// Exposed API calls for client apps
#[apply(async_trait_maybe_send!)]
pub trait OddsMarketsClientExt {
    /// Create new market
    async fn create_market(&self, market: Market) -> anyhow::Result<OutPoint>;

    /// Payout market
    async fn payout_market(
        &self,
        payout: XOnlyPublicKey,
        amount: Amount,
    ) -> anyhow::Result<OutPoint>;
}

#[apply(async_trait_maybe_send!)]
impl OddsMarketsClientExt for Client {
    async fn create_market(&self, market: Market) -> anyhow::Result<OutPoint> {
        let (_odds_markets, instance) = self.get_first_module::<OddsMarketsClientModule>(&KIND);
        let _dbtx = instance.db.begin_transaction().await;
        let op_id = OperationId(rand::random());

        let output = ClientOutput {
            output: OddsMarketsOutput::NewMarket(market),
            state_machines: Arc::new(move |_, _| Vec::<OddsMarketsStateMachine>::new()),
        };

        let tx = TransactionBuilder::new().with_output(output.into_dyn(instance.id));
        let out_point = |txid, _| OutPoint { txid, out_idx: 0 };
        let txid = self
            .finalize_and_submit_transaction(
                op_id,
                OddsMarketsCommonGen::KIND.as_str(),
                out_point,
                tx,
            )
            .await?;

        Ok(OutPoint { txid, out_idx: 0 })
    }
    async fn payout_market(
        &self,
        _payout: XOnlyPublicKey,
        _amount: Amount,
    ) -> anyhow::Result<OutPoint> {
        panic!()
    }
}

#[derive(Debug)]
pub struct OddsMarketsClientModule {
    cfg: OddsMarketsClientConfig,
    key: KeyPair,
    notifier: ModuleNotifier<DynGlobalClientContext, OddsMarketsStateMachine>,
}

/// Data needed by the state machine
#[derive(Debug, Clone)]
pub struct OddsMarketsClientContext {
    pub dummy_decoder: Decoder,
}

// TODO: Boiler-plate
impl Context for OddsMarketsClientContext {}

#[apply(async_trait_maybe_send!)]
impl ClientModule for OddsMarketsClientModule {
    type Common = OddsMarketsModuleTypes;
    type ModuleStateMachineContext = OddsMarketsClientContext;
    type States = OddsMarketsStateMachine;

    fn context(&self) -> Self::ModuleStateMachineContext {
        OddsMarketsClientContext {
            dummy_decoder: self.decoder(),
        }
    }

    fn input_amount(&self, input: &<Self::Common as ModuleCommon>::Input) -> TransactionItemAmount {
        let amount = Amount::ZERO;
        let fee = Amount::ZERO;

        match input {
            OddsMarketsInput::CancelOrder() => {}
            OddsMarketsInput::ConsumeOrderFreeBalance() => {}
        }

        TransactionItemAmount {
            amount: amount,
            fee: fee,
        }
    }

    fn output_amount(
        &self,
        output: &<Self::Common as ModuleCommon>::Output,
    ) -> TransactionItemAmount {
        let amount = Amount::ZERO;
        let mut fee = Amount::ZERO;

        match output {
            OddsMarketsOutput::NewMarket(_) => {
                fee = self.cfg.new_market_fee;
            }
            OddsMarketsOutput::NewOrder() => {}
            OddsMarketsOutput::PayoutMarket(_, _) => {}
        }

        TransactionItemAmount {
            amount: amount,
            fee: fee,
        }
    }

    async fn handle_cli_command(
        &self,
        _client: &Client,
        args: &[ffi::OsString],
    ) -> anyhow::Result<serde_json::Value> {
        if args.is_empty() {
            return Err(anyhow::format_err!(
                "Expected to be called with at least 1 arguments: <command> …"
            ));
        }

        let command = args[0].to_string_lossy();

        match command.as_ref() {
            command => Err(anyhow::format_err!(
                "Unknown command: {command}, supported commands: print-money"
            )),
        }
    }
}

async fn get_funds(dbtx: &mut ModuleDatabaseTransaction<'_>) -> Amount {
    let funds = dbtx.get_value(&DummyClientFundsKeyV0).await;
    funds.unwrap_or(Amount::ZERO)
}

#[derive(Debug, Clone)]
pub struct OddsMarketsClientGen;

// TODO: Boilerplate-code
impl ExtendsCommonModuleInit for OddsMarketsClientGen {
    type Common = OddsMarketsCommonGen;
}

/// Generates the client module
#[apply(async_trait_maybe_send!)]
impl ClientModuleInit for OddsMarketsClientGen {
    type Module = OddsMarketsClientModule;

    fn supported_api_versions(&self) -> MultiApiVersion {
        MultiApiVersion::try_from_iter([ApiVersion { major: 0, minor: 0 }])
            .expect("no version conflicts")
    }

    async fn init(
        &self,
        _federation_id: FederationId,
        cfg: OddsMarketsClientConfig,
        _db: Database,
        _api_version: ApiVersion,
        module_root_secret: DerivableSecret,
        notifier: ModuleNotifier<DynGlobalClientContext, <Self::Module as ClientModule>::States>,
        _api: DynGlobalApi,
        _module_api: DynModuleApi,
    ) -> anyhow::Result<Self::Module> {
        Ok(OddsMarketsClientModule {
            cfg,
            key: module_root_secret.to_secp_key(&Secp256k1::new()),
            notifier,
        })
    }
}
