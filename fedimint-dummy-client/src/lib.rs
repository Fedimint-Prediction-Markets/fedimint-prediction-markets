use std::ffi;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::bail;
use bitcoin::Denomination;
use common::{
    ContractAmount, Market, MarketDescription, Order, OrderIDClientSide, Outcome, Payout, Side,
};
use fedimint_client::derivable_secret::{ChildId, DerivableSecret};
use fedimint_client::module::init::ClientModuleInit;
use fedimint_client::module::{ClientModule, IClientModule};
use fedimint_client::sm::{Context, ModuleNotifier, OperationId};
use fedimint_client::transaction::{ClientInput, ClientOutput, TransactionBuilder};
use fedimint_client::{Client, DynGlobalClientContext};
use fedimint_core::api::{DynGlobalApi, DynModuleApi};
use fedimint_core::config::FederationId;
use fedimint_core::core::{Decoder, IntoDynInstance, KeyPair};
use fedimint_core::db::{Database, ModuleDatabaseTransaction};
use fedimint_core::module::{
    ApiVersion, CommonModuleInit, ExtendsCommonModuleInit, ModuleCommon, MultiApiVersion,
    TransactionItemAmount,
};

use fedimint_core::{apply, async_trait_maybe_send, outcome, Amount, OutPoint, TransactionId};
pub use fedimint_dummy_common as common;
use fedimint_dummy_common::config::PredictionMarketsClientConfig;
use fedimint_dummy_common::{
    PredictionMarketsCommonGen, PredictionMarketsInput, PredictionMarketsModuleTypes,
    PredictionMarketsOutput, KIND,
};

use futures::StreamExt;
use secp256k1::{Secp256k1, XOnlyPublicKey};
use states::PredictionMarketsStateMachine;

use crate::api::OddsMarketsFederationApi;

pub mod api;
mod db;
mod states;

/// Exposed API calls for client apps
#[apply(async_trait_maybe_send!)]
pub trait OddsMarketsClientExt {
    /// Create new market
    async fn new_market(
        &self,
        contract_price: Amount,
        outcomes: Outcome,
        description: MarketDescription,
    ) -> anyhow::Result<OutPoint>;

    /// Get Market
    async fn get_market(&self, market_out_point: OutPoint) -> anyhow::Result<Market>;

    /// Payout market
    async fn payout_market(&self, payout: Payout) -> anyhow::Result<OutPoint>;

    /// Create new order
    async fn new_order(
        &self,
        market: OutPoint,
        outcome: Outcome,
        side: Side,
        price: Amount,
        quantity: ContractAmount,
    ) -> anyhow::Result<OrderIDClientSide>;

    /// get order information from federation and update local db.
    async fn get_order_from_federation(
        &self,
        id: OrderIDClientSide,
    ) -> anyhow::Result<Option<Order>>;

    /// Get order present in our local db, may be out of date.
    async fn get_order_from_db(&self, id: OrderIDClientSide) -> anyhow::Result<Order>;

    /// Cancel order
    async fn cancel_order(&self, order: XOnlyPublicKey) -> anyhow::Result<()>;
}

#[apply(async_trait_maybe_send!)]
impl OddsMarketsClientExt for Client {
    async fn new_market(
        &self,
        contract_price: Amount,
        outcomes: Outcome,
        description: MarketDescription,
    ) -> anyhow::Result<OutPoint> {
        let (prediction_markets, instance) =
            self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let op_id = OperationId(rand::random());

        let key = prediction_markets
            .root_secret
            .child_key(MARKET_OUTCOME)
            .to_secp_key(&Secp256k1::new());
        let (outcome_control, _) = XOnlyPublicKey::from_keypair(&key);

        let output = ClientOutput {
            output: PredictionMarketsOutput::NewMarket {
                contract_price,
                outcomes,
                outcome_control,
                description,
            },
            state_machines: Arc::new(move |_, _| Vec::<PredictionMarketsStateMachine>::new()),
        };

        let tx = TransactionBuilder::new().with_output(output.into_dyn(instance.id));
        let outpoint = |txid, _| OutPoint { txid, out_idx: 0 };
        let txid = self
            .finalize_and_submit_transaction(
                op_id,
                PredictionMarketsCommonGen::KIND.as_str(),
                outpoint,
                tx,
            )
            .await?;

        let tx_subscription = self.transaction_updates(op_id).await;
        tx_subscription.await_tx_accepted(txid).await?;

        let outpoint = OutPoint { txid, out_idx: 0 };

        let mut dbtx = instance.db.begin_transaction().await;
        dbtx.insert_entry(&db::OutcomeControlMarketsKey { market: outpoint }, &())
            .await;
        dbtx.commit_tx().await;

        Ok(outpoint)
    }

    async fn get_market(&self, market_out_point: OutPoint) -> anyhow::Result<Market> {
        let (_, instance) = self.get_first_module::<PredictionMarketsClientModule>(&KIND);

        Ok(instance.api.get_market(market_out_point).await?)
    }

    async fn payout_market(&self, payout: Payout) -> anyhow::Result<OutPoint> {
        bail!("t")
    }

    async fn new_order(
        &self,
        market: OutPoint,
        outcome: Outcome,
        side: Side,
        price: Amount,
        quantity: ContractAmount,
    ) -> anyhow::Result<OrderIDClientSide> {
        let (prediction_markets, instance) =
            self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let op_id = OperationId(rand::random());

        let mut dbtx = instance.db.begin_transaction().await;

        let id = {
            let mut stream = dbtx
                .find_by_prefix_sorted_descending(&db::OrderPrefixAll)
                .await;
            match stream.next().await {
                Some((key, _)) => key.id.0 + 1,
                None => 0,
            }
        };

        let order_key = prediction_markets
            .root_secret
            .child_key(ORDER)
            .child_key(ChildId(id))
            .to_secp_key(&Secp256k1::new());
        let (owner, _) = XOnlyPublicKey::from_keypair(&order_key);

        let mut tx = TransactionBuilder::new();
        match side {
            Side::Buy => {
                let output = ClientOutput {
                    output: PredictionMarketsOutput::NewBuyOrder {
                        owner,
                        market,
                        outcome,
                        price,
                        quantity,
                    },
                    state_machines: Arc::new(move |_, _| {
                        Vec::<PredictionMarketsStateMachine>::new()
                    }),
                };

                tx = tx.with_output(output.into_dyn(instance.id));
            }
            Side::Sell => {}
        }

        let outpoint = |txid, _| OutPoint { txid, out_idx: 0 };
        let txid = self
            .finalize_and_submit_transaction(
                op_id,
                PredictionMarketsCommonGen::KIND.as_str(),
                outpoint,
                tx,
            )
            .await?;

        let tx_subscription = self.transaction_updates(op_id).await;
        tx_subscription.await_tx_accepted(txid).await?;

        self.get_order_from_federation(OrderIDClientSide(id))
            .await?;

        Ok(OrderIDClientSide(id))
    }

    async fn get_order_from_federation(
        &self,
        id: OrderIDClientSide,
    ) -> anyhow::Result<Option<Order>> {
        let (prediction_markets, instance) =
            self.get_first_module::<PredictionMarketsClientModule>(&KIND);

        let mut dbtx = instance.db.begin_transaction().await;

        let order_key = prediction_markets
            .root_secret
            .child_key(ORDER)
            .child_key(ChildId(id.0))
            .to_secp_key(&Secp256k1::new());
        let (owner, _) = XOnlyPublicKey::from_keypair(&order_key);

        let order_option = instance.api.get_order(owner).await?;
        if let Some(order) = order_option.as_ref() {
            dbtx.insert_new_entry(&db::OrderKey { id }, order).await;
        }

        dbtx.commit_tx().await;

        Ok(order_option)
    }

    async fn get_order_from_db(&self, id: OrderIDClientSide) -> anyhow::Result<Order> {
        let (_, instance) = self.get_first_module::<PredictionMarketsClientModule>(&KIND);

        let mut dbtx = instance.db.begin_transaction().await;

        Ok(dbtx.get_value(&db::OrderKey { id }).await.unwrap())
    }

    async fn cancel_order(&self, order: XOnlyPublicKey) -> anyhow::Result<()> {
        bail!("t")
    }
}

#[derive(Debug)]
pub struct PredictionMarketsClientModule {
    cfg: PredictionMarketsClientConfig,
    root_secret: DerivableSecret,
    notifier: ModuleNotifier<DynGlobalClientContext, PredictionMarketsStateMachine>,
}

impl PredictionMarketsClientModule {}

/// Data needed by the state machine
#[derive(Debug, Clone)]
pub struct PredictionMarketsClientContext {
    pub dummy_decoder: Decoder,
}

// TODO: Boiler-plate
impl Context for PredictionMarketsClientContext {}

#[apply(async_trait_maybe_send!)]
impl ClientModule for PredictionMarketsClientModule {
    type Common = PredictionMarketsModuleTypes;
    type ModuleStateMachineContext = PredictionMarketsClientContext;
    type States = PredictionMarketsStateMachine;

    fn context(&self) -> Self::ModuleStateMachineContext {
        PredictionMarketsClientContext {
            dummy_decoder: self.decoder(),
        }
    }

    fn input_amount(&self, input: &<Self::Common as ModuleCommon>::Input) -> TransactionItemAmount {
        let mut amount = Amount::ZERO;
        let mut fee = Amount::ZERO;

        match input {
            PredictionMarketsInput::PayoutMarket { market, payout } => {}
            PredictionMarketsInput::CancelOrder { order } => {}
            PredictionMarketsInput::ConsumeOrderFreeBalance {
                order,
                amount: amount_to_free,
            } => amount = amount_to_free.to_owned(),
            PredictionMarketsInput::NewSellOrder {
                owner,
                market,
                outcome,
                price,
                sources,
            } => {
                fee = self.cfg.new_order_fee;
            }
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
        let mut amount = Amount::ZERO;
        let mut fee = Amount::ZERO;

        match output {
            PredictionMarketsOutput::NewMarket {
                contract_price,
                outcomes,
                outcome_control,
                description,
            } => {
                fee = self.cfg.new_market_fee;
            }
            PredictionMarketsOutput::NewBuyOrder {
                owner,
                market,
                outcome,
                price,
                quantity,
            } => {
                amount = price.to_owned() * quantity.0;
                fee = self.cfg.new_order_fee;
            }
        }

        TransactionItemAmount {
            amount: amount,
            fee: fee,
        }
    }

    async fn handle_cli_command(
        &self,
        client: &Client,
        args: &[ffi::OsString],
    ) -> anyhow::Result<serde_json::Value> {
        if args.is_empty() {
            return Err(anyhow::format_err!(
                "Expected to be called with at least 1 arguments: <command> …"
            ));
        }

        let command = args[0].to_string_lossy();

        match command.as_ref() {
            "new-market" => {
                if args.len() != 3 {
                    return Err(anyhow::format_err!(
                        "`new-market` command expects 2 argument: <contract_price_msats> <outcomes>"
                    ));
                }

                let contract_price =
                    Amount::from_str_in(&args[1].to_string_lossy(), Denomination::MilliSatoshi)?;
                let outcomes: Outcome = args[2].to_string_lossy().parse()?;

                let market_out_point = client
                    .new_market(
                        contract_price,
                        outcomes,
                        MarketDescription {
                            title: "test".to_owned(),
                        },
                    )
                    .await?;

                Ok(serde_json::to_value(market_out_point)?)
            }

            "get-market" => {
                if args.len() != 2 {
                    return Err(anyhow::format_err!(
                        "`get-market` command expects 1 argument: <market_out_point>"
                    ));
                }

                let Ok(txid) = TransactionId::from_str(&args[1].to_string_lossy()) else {
                    bail!("Error getting transaction id");
                };

                let out_point = OutPoint { txid, out_idx: 0 };

                Ok(serde_json::to_value(client.get_market(out_point).await?)?)
            }

            "new-order" => {
                if args.len() != 6 {
                    return Err(anyhow::format_err!(
                        "`create-market` command expects 5 argument: <market_out_point> <outcome> <side> <price_msats> <quantity>"
                    ));
                }

                let Ok(txid) = TransactionId::from_str(&args[1].to_string_lossy()) else {
                    bail!("Error getting transaction id");
                };

                let out_point = OutPoint { txid, out_idx: 0 };

                let outcome: Outcome = args[2].to_string_lossy().parse()?;

                let side = Side::try_from(args[3].to_string_lossy().into_owned().as_str())?;

                let price =
                    Amount::from_str_in(&args[4].to_string_lossy(), Denomination::MilliSatoshi)?;

                let quantity = ContractAmount(args[5].to_string_lossy().parse()?);

                Ok(serde_json::to_value(
                    client.new_order(out_point, outcome, side, price, quantity).await?,
                )?)
            }

            "get-order" => {
                if args.len() != 2 {
                    return Err(anyhow::format_err!(
                        "`create-market` command expects 1 argument: <order_id>"
                    ));
                }

                let id = OrderIDClientSide(args[1].to_string_lossy().parse()?);

                Ok(serde_json::to_value(
                    client.get_order_from_federation(id).await?,
                )?)
            }

            command => Err(anyhow::format_err!(
                "Unknown command: {command}, supported commands: print-money"
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PredictionMarketsClientGen;

// TODO: Boilerplate-code
impl ExtendsCommonModuleInit for PredictionMarketsClientGen {
    type Common = PredictionMarketsCommonGen;
}

/// Generates the client module
#[apply(async_trait_maybe_send!)]
impl ClientModuleInit for PredictionMarketsClientGen {
    type Module = PredictionMarketsClientModule;

    fn supported_api_versions(&self) -> MultiApiVersion {
        MultiApiVersion::try_from_iter([ApiVersion { major: 0, minor: 0 }])
            .expect("no version conflicts")
    }

    async fn init(
        &self,
        _federation_id: FederationId,
        cfg: PredictionMarketsClientConfig,
        _db: Database,
        _api_version: ApiVersion,
        root_secret: DerivableSecret,
        notifier: ModuleNotifier<DynGlobalClientContext, <Self::Module as ClientModule>::States>,
        _api: DynGlobalApi,
        _module_api: DynModuleApi,
    ) -> anyhow::Result<Self::Module> {
        Ok(PredictionMarketsClientModule {
            cfg,
            root_secret,
            notifier,
        })
    }
}

const MARKET_OUTCOME: ChildId = ChildId(0);
const ORDER: ChildId = ChildId(1);
