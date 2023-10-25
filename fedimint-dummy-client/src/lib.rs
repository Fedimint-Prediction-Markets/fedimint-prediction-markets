use std::ffi;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::bail;
use bitcoin::Denomination;
use common::{
    ContractAmount, ContractSource, Market, MarketDescription, Order, OrderIDClientSide, Outcome,
    Payout, Side,
};
use fedimint_client::derivable_secret::{ChildId, DerivableSecret};
use fedimint_client::module::init::ClientModuleInit;
use fedimint_client::module::{ClientModule, IClientModule};
use fedimint_client::sm::{Context, ModuleNotifier, OperationId};
use fedimint_client::transaction::{ClientInput, ClientOutput, TransactionBuilder};
use fedimint_client::{Client, DynGlobalClientContext};
use fedimint_core::api::{DynGlobalApi, DynModuleApi};
use fedimint_core::config::FederationId;
use fedimint_core::core::{Decoder, IntoDynInstance};
use fedimint_core::db::Database;
use fedimint_core::module::{
    ApiVersion, CommonModuleInit, ExtendsCommonModuleInit, ModuleCommon, MultiApiVersion,
    TransactionItemAmount,
};

use fedimint_core::{apply, async_trait_maybe_send, Amount, OutPoint, TransactionId};
pub use fedimint_dummy_common as common;
use fedimint_dummy_common::config::PredictionMarketsClientConfig;
use fedimint_dummy_common::{
    PredictionMarketsCommonGen, PredictionMarketsInput, PredictionMarketsModuleTypes,
    PredictionMarketsOutput, KIND,
};

use futures::StreamExt;
use secp256k1::{KeyPair, Secp256k1, XOnlyPublicKey};
use states::PredictionMarketsStateMachine;

use crate::api::PredictionMarketsFederationApi;

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
    async fn get_market(&self, market: OutPoint) -> anyhow::Result<Market>;

    /// Payout market
    async fn payout_market(
        &self,
        market: OutPoint,
        outcome_payouts: Vec<Amount>,
    ) -> anyhow::Result<()>;

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
    async fn get_order_from_db(&self, id: OrderIDClientSide) -> Option<Order>;

    /// Cancel order
    async fn cancel_order(&self, id: OrderIDClientSide) -> anyhow::Result<()>;
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

        let key = prediction_markets.get_market_outcome_control_key_pair();
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

    async fn payout_market(
        &self,
        market_out_point: OutPoint,
        outcome_payouts: Vec<Amount>,
    ) -> anyhow::Result<()> {
        let (prediction_markets, instance) =
            self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let op_id = OperationId(rand::random());

        let outcome_control_key = prediction_markets.get_market_outcome_control_key_pair();

        let input = ClientInput {
            input: PredictionMarketsInput::PayoutMarket {
                market: market_out_point,
                payout: Payout { outcome_payouts },
            },
            state_machines: Arc::new(move |_, _| Vec::<PredictionMarketsStateMachine>::new()),
            keys: vec![outcome_control_key],
        };

        let tx = TransactionBuilder::new().with_input(input.into_dyn(instance.id));
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

        Ok(())
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
            .child_key(ORDER_FROM_ROOT)
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
            Side::Sell => {
                let mut sources = vec![];
                let mut keys = vec![];

                let mut sourced_quantity = ContractAmount::ZERO;
                let market_orders: Vec<_> = dbtx
                    .find_by_prefix(&db::OrdersByMarketPrefix2 { market, outcome })
                    .await
                    .map(|(key, _)| key.order)
                    .collect()
                    .await;
                for order_id in market_orders {
                    let mut order = self
                        .get_order_from_db(order_id)
                        .await
                        .expect("should always produce order");
                    if order.contract_balance == ContractAmount::ZERO {
                        continue;
                    }

                    let order_key = prediction_markets.order_id_to_key_pair(order_id);
                    let quantity_sourced_from_order =
                        order.contract_balance.min(quantity - sourced_quantity);

                    sources.push(ContractSource {
                        order: XOnlyPublicKey::from_keypair(&order_key).0,
                        amount: quantity_sourced_from_order,
                    });
                    keys.push(order_key);

                    sourced_quantity = sourced_quantity + quantity_sourced_from_order;

                    order.contract_balance = order.contract_balance - quantity_sourced_from_order;
                    dbtx.insert_entry(&db::OrderKey { id: order_id }, &order)
                        .await;

                    if quantity == sourced_quantity {
                        break;
                    }
                }

                if quantity != sourced_quantity {
                    bail!("Insufficient outcome quantity for new sell order");
                }

                let input = ClientInput {
                    input: PredictionMarketsInput::NewSellOrder {
                        owner,
                        market,
                        outcome,
                        price,
                        sources,
                    },
                    state_machines: Arc::new(move |_, _| {
                        Vec::<PredictionMarketsStateMachine>::new()
                    }),
                    keys,
                };

                tx = tx.with_input(input.into_dyn(instance.id));
            }
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

        dbtx.commit_tx().await;

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
            .child_key(ORDER_FROM_ROOT)
            .child_key(ChildId(id.0))
            .to_secp_key(&Secp256k1::new());
        let (owner, _) = XOnlyPublicKey::from_keypair(&order_key);

        let order_option = instance.api.get_order(owner).await?;
        if let Some(order) = order_option.as_ref() {
            dbtx.insert_entry(&db::OrderKey { id }, order).await;
            dbtx.insert_entry(
                &db::OrdersByMarketOutcomeKey {
                    market: order.market,
                    outcome: order.outcome,
                    order: id,
                },
                &(),
            )
            .await;
        }

        dbtx.commit_tx().await;

        Ok(order_option)
    }

    async fn get_order_from_db(&self, id: OrderIDClientSide) -> Option<Order> {
        let (_, instance) = self.get_first_module::<PredictionMarketsClientModule>(&KIND);

        let mut dbtx = instance.db.begin_transaction().await;

        dbtx.get_value(&db::OrderKey { id }).await
    }

    async fn cancel_order(&self, id: OrderIDClientSide) -> anyhow::Result<()> {
        let (prediction_markets, instance) =
            self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let op_id = OperationId(rand::random());

        let order_key = prediction_markets.order_id_to_key_pair(id);

        let input = ClientInput {
            input: PredictionMarketsInput::CancelOrder {
                order: XOnlyPublicKey::from_keypair(&order_key).0,
            },
            state_machines: Arc::new(move |_, _| Vec::<PredictionMarketsStateMachine>::new()),
            keys: vec![order_key],
        };

        let tx = TransactionBuilder::new().with_input(input.into_dyn(instance.id));
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

        Ok(())
    }
}

#[derive(Debug)]
pub struct PredictionMarketsClientModule {
    cfg: PredictionMarketsClientConfig,
    root_secret: DerivableSecret,
    notifier: ModuleNotifier<DynGlobalClientContext, PredictionMarketsStateMachine>,
}

impl PredictionMarketsClientModule {
    pub fn order_id_to_key_pair(&self, id: OrderIDClientSide) -> KeyPair {
        self.root_secret
            .child_key(ORDER_FROM_ROOT)
            .child_key(ChildId(id.0))
            .to_secp_key(&Secp256k1::new())
    }

    pub fn get_market_outcome_control_key_pair(&self) -> KeyPair {
        self.root_secret
            .child_key(MARKET_OUTCOME_CONTROL_FROM_ROOT)
            .to_secp_key(&Secp256k1::new())
    }
}

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
            PredictionMarketsInput::PayoutMarket {
                market: _,
                payout: _,
            } => {}
            PredictionMarketsInput::CancelOrder { order: _ } => {}
            PredictionMarketsInput::ConsumeOrderFreeBalance {
                order: _,
                amount: amount_to_free,
            } => amount = amount_to_free.to_owned(),
            PredictionMarketsInput::NewSellOrder {
                owner: _,
                market: _,
                outcome: _,
                price: _,
                sources: _,
            } => {
                fee = self.cfg.new_order_fee;
            }
        }

        TransactionItemAmount { amount, fee }
    }

    fn output_amount(
        &self,
        output: &<Self::Common as ModuleCommon>::Output,
    ) -> TransactionItemAmount {
        let mut amount = Amount::ZERO;
        let fee;

        match output {
            PredictionMarketsOutput::NewMarket {
                contract_price: _,
                outcomes: _,
                outcome_control: _,
                description: _,
            } => {
                fee = self.cfg.new_market_fee;
            }
            PredictionMarketsOutput::NewBuyOrder {
                owner: _,
                market: _,
                outcome: _,
                price,
                quantity,
            } => {
                amount = price.to_owned() * quantity.0;
                fee = self.cfg.new_order_fee;
            }
        }

        TransactionItemAmount { amount, fee }
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

            "payout-market" => {
                if args.len() < 4 {
                    return Err(anyhow::format_err!(
                        "`get-market` command expects at least 3 arguments: <market_out_point> <outcome_0_payout> <outcome_1_payout> ..."
                    ));
                }

                let Ok(txid) = TransactionId::from_str(&args[1].to_string_lossy()) else {
                    bail!("Error getting transaction id");
                };
                let market_out_point = OutPoint { txid, out_idx: 0 };

                let mut outcome_payouts: Vec<Amount> = vec![];

                for i in 2..usize::MAX {
                    let Some(arg) = args.get(i) else {
                        break;
                    };

                    outcome_payouts.push(Amount::from_str_in(
                        &arg.to_string_lossy(),
                        Denomination::MilliSatoshi,
                    )?);
                }

                Ok(serde_json::to_value(
                    client
                        .payout_market(market_out_point, outcome_payouts)
                        .await?,
                )?)
            }

            "new-order" => {
                if args.len() != 6 {
                    return Err(anyhow::format_err!(
                        "`new-order` command expects 5 argument: <market_out_point> <outcome> <side> <price_msats> <quantity>"
                    ));
                }

                let Ok(txid) = TransactionId::from_str(&args[1].to_string_lossy()) else {
                    bail!("Error getting transaction id");
                };

                let out_point = OutPoint { txid, out_idx: 0 };

                let outcome: Outcome = args[2].to_string_lossy().parse()?;

                let side = Side::try_from(args[3].to_string_lossy().as_ref())?;

                let price =
                    Amount::from_str_in(&args[4].to_string_lossy(), Denomination::MilliSatoshi)?;

                let quantity = ContractAmount(args[5].to_string_lossy().parse()?);

                Ok(serde_json::to_value(
                    client
                        .new_order(out_point, outcome, side, price, quantity)
                        .await?,
                )?)
            }

            "get-order" => {
                if args.len() != 2 {
                    return Err(anyhow::format_err!(
                        "`get-order` command expects 1 argument: <order_id>"
                    ));
                }

                let id = OrderIDClientSide(args[1].to_string_lossy().parse()?);

                Ok(serde_json::to_value(
                    client.get_order_from_federation(id).await?,
                )?)
            }

            "cancel-order" => {
                if args.len() != 2 {
                    return Err(anyhow::format_err!(
                        "`cancel-order` command expects 1 argument: <order_id>"
                    ));
                }

                let id = OrderIDClientSide(args[1].to_string_lossy().parse()?);

                Ok(serde_json::to_value(client.cancel_order(id).await?)?)
            }

            command => Err(anyhow::format_err!(
                "Unknown command: {command}, supported commands: new-market, get-market, payout-market, new-order, get-order, cancel-order"
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

const MARKET_OUTCOME_CONTROL_FROM_ROOT: ChildId = ChildId(0);
const ORDER_FROM_ROOT: ChildId = ChildId(1);
