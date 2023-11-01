use std::collections::BTreeMap;
use std::ffi;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::bail;
use bitcoin::Denomination;
use db::OrderIdSlot;
use fedimint_client::derivable_secret::{ChildId, DerivableSecret};
use fedimint_client::module::init::ClientModuleInit;
use fedimint_client::module::{ClientModule, IClientModule};
use fedimint_client::sm::{Context, ModuleNotifier, OperationId};
use fedimint_client::transaction::{ClientInput, ClientOutput, TransactionBuilder};
use fedimint_client::{Client, DynGlobalClientContext};
use fedimint_core::api::{DynGlobalApi, DynModuleApi};
use fedimint_core::config::FederationId;
use fedimint_core::core::{Decoder, IntoDynInstance};
use fedimint_core::db::{Database, ModuleDatabaseTransaction};
use fedimint_core::module::{
    ApiVersion, CommonModuleInit, ExtendsCommonModuleInit, ModuleCommon, MultiApiVersion,
    TransactionItemAmount,
};
use fedimint_prediction_markets_common::{
    ContractOfOutcomeAmount, ContractOfOutcomeSource, Market, MarketInformation, Order,
    OrderIdClientSide, Outcome, Payout, Side, UnixTimestamp,
};

use fedimint_core::{apply, async_trait_maybe_send, Amount, OutPoint, TransactionId};
use fedimint_prediction_markets_common::config::PredictionMarketsClientConfig;
use fedimint_prediction_markets_common::{
    PredictionMarketsCommonGen, PredictionMarketsInput, PredictionMarketsModuleTypes,
    PredictionMarketsOutput, KIND,
};

use futures::{future, StreamExt};
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
        description: MarketInformation,
    ) -> anyhow::Result<OutPoint>;

    /// Get Market
    async fn get_market(
        &self,
        market: OutPoint,
        from_local_cache: bool,
    ) -> anyhow::Result<Option<Market>>;

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
        quantity: ContractOfOutcomeAmount,
    ) -> anyhow::Result<OrderIdClientSide>;

    /// Get order
    async fn get_order(
        &self,
        id: OrderIdClientSide,
        from_local_cache: bool,
    ) -> anyhow::Result<Option<Order>>;

    /// Cancel order
    async fn cancel_order(&self, id: OrderIdClientSide) -> anyhow::Result<()>;

    /// Update all orders in db that could have possibly changed
    /// Optionally provide a market to update only orders belonging to that market
    async fn sync_orders(&self, market: Option<OutPoint>) -> anyhow::Result<()>;

    /// Get all orders in the db.
    async fn get_all_orders_from_db(&self) -> BTreeMap<OrderIdClientSide, Order>;
}

#[apply(async_trait_maybe_send!)]
impl OddsMarketsClientExt for Client {
    async fn new_market(
        &self,
        contract_price: Amount,
        outcomes: Outcome,
        description: MarketInformation,
    ) -> anyhow::Result<OutPoint> {
        let (prediction_markets, instance) =
            self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let operation_id = OperationId::new_random();

        let key = prediction_markets.get_market_outcome_control_key_pair();
        let (outcome_control, _) = XOnlyPublicKey::from_keypair(&key);

        let output = ClientOutput {
            output: PredictionMarketsOutput::NewMarket {
                contract_price,
                outcomes,
                outcome_control,
                description,
            },
            state_machines: Arc::new(move |tx_id, _| {
                vec![PredictionMarketsStateMachine::NewMarket {
                    operation_id,
                    tx_id,
                }]
            }),
        };

        let tx = TransactionBuilder::new().with_output(output.into_dyn(instance.id));
        let out_point = |txid, _| OutPoint { txid, out_idx: 0 };
        let tx_id = self
            .finalize_and_submit_transaction(
                operation_id,
                PredictionMarketsCommonGen::KIND.as_str(),
                out_point,
                tx,
            )
            .await?;

        let tx_subscription = self.transaction_updates(operation_id).await;
        tx_subscription.await_tx_accepted(tx_id).await?;

        Ok(OutPoint {
            txid: tx_id,
            out_idx: 0,
        })
    }

    async fn get_market(
        &self,
        market_out_point: OutPoint,
        from_local_cache: bool,
    ) -> anyhow::Result<Option<Market>> {
        let (_, instance) = self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let mut dbtx = instance.db.begin_transaction().await;

        match from_local_cache {
            true => Ok(dbtx
                .get_value(&db::MarketKey {
                    market: market_out_point,
                })
                .await),

            false => {
                let market_option = instance.api.get_market(market_out_point).await?;

                if let Some(market) = market_option.as_ref() {
                    dbtx.insert_entry(
                        &db::MarketKey {
                            market: market_out_point,
                        },
                        market,
                    )
                    .await;
                    dbtx.commit_tx().await;
                }

                Ok(market_option)
            }
        }
    }

    async fn payout_market(
        &self,
        market_out_point: OutPoint,
        outcome_payouts: Vec<Amount>,
    ) -> anyhow::Result<()> {
        let (prediction_markets, instance) =
            self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let op_id = OperationId::new_random();

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
        let out_point = |txid, _| OutPoint { txid, out_idx: 0 };
        let txid = self
            .finalize_and_submit_transaction(
                op_id,
                PredictionMarketsCommonGen::KIND.as_str(),
                out_point,
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
        quantity: ContractOfOutcomeAmount,
    ) -> anyhow::Result<OrderIdClientSide> {
        let (prediction_markets, instance) =
            self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let operation_id = OperationId::new_random();
        let mut dbtx = instance.db.begin_transaction().await;

        let order_id = {
            let mut stream = dbtx
                .find_by_prefix_sorted_descending(&db::OrderPrefixAll)
                .await;
            match stream.next().await {
                Some((mut key, _)) => {
                    key.id.0 += 1;
                    key.id
                }
                None => OrderIdClientSide(0),
            }
        };

        let order_key = prediction_markets.order_id_to_key_pair(order_id);
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
                    state_machines: Arc::new(move |tx_id, _| {
                        vec![PredictionMarketsStateMachine::NewOrder {
                            operation_id,
                            tx_id,
                            order: order_id,
                            sources: vec![],
                        }]
                    }),
                };

                tx = tx.with_output(output.into_dyn(instance.id));
            }
            Side::Sell => {
                let mut sources_for_input = vec![];
                let mut sources_for_state_machine = vec![];
                let mut sources_keys = vec![];

                let non_zero_market_outcome_orders: Vec<_> = dbtx
                    .find_by_prefix(&db::NonZeroOrdersByMarketOutcomePrefix2 { market, outcome })
                    .await
                    .map(|(key, _)| key.order)
                    .collect()
                    .await;

                let mut sourced_quantity = ContractOfOutcomeAmount::ZERO;
                for order_id in non_zero_market_outcome_orders {
                    let order = self
                        .get_order(order_id, true)
                        .await
                        .expect("should never fail")
                        .expect("should always be some");

                    if order.contract_of_outcome_balance == ContractOfOutcomeAmount::ZERO {
                        continue;
                    }

                    let quantity_sourced_from_order = order
                        .contract_of_outcome_balance
                        .min(quantity - sourced_quantity);
                    sources_for_input.push(ContractOfOutcomeSource {
                        order: XOnlyPublicKey::from_keypair(&order_key).0,
                        quantity: quantity_sourced_from_order,
                    });

                    sources_for_state_machine.push(order_id);

                    let order_key = prediction_markets.order_id_to_key_pair(order_id);
                    sources_keys.push(order_key);

                    sourced_quantity = sourced_quantity + quantity_sourced_from_order;
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
                        sources: sources_for_input,
                    },
                    state_machines: Arc::new(move |tx_id, _| {
                        vec![PredictionMarketsStateMachine::NewOrder {
                            operation_id,
                            tx_id,
                            order: order_id,
                            sources: sources_for_state_machine.to_owned(),
                        }]
                    }),
                    keys: sources_keys,
                };

                tx = tx.with_input(input.into_dyn(instance.id));
            }
        }

        PredictionMarketsClientModule::new_order(dbtx.get_isolated(), order_id).await;
        dbtx.commit_tx().await;

        let outpoint = |txid, _| OutPoint { txid, out_idx: 0 };
        let txid = self
            .finalize_and_submit_transaction(
                operation_id,
                PredictionMarketsCommonGen::KIND.as_str(),
                outpoint,
                tx,
            )
            .await?;

        let tx_subscription = self.transaction_updates(operation_id).await;
        tx_subscription.await_tx_accepted(txid).await?;

        Ok(order_id)
    }

    async fn get_order(
        &self,
        id: OrderIdClientSide,
        from_local_cache: bool,
    ) -> anyhow::Result<Option<Order>> {
        let (prediction_markets, instance) =
            self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let mut dbtx = instance.db.begin_transaction().await;

        let order_key = prediction_markets.order_id_to_key_pair(id);
        let (order_owner, _) = XOnlyPublicKey::from_keypair(&order_key);

        match from_local_cache {
            true => Ok(match dbtx.get_value(&db::OrderKey { id }).await {
                Some(d) => match d {
                    OrderIdSlot::Reserved => None,
                    OrderIdSlot::Order(order) => Some(order),
                },

                None => None,
            }),

            false => {
                let order_option = instance.api.get_order(order_owner).await?;

                if let Some(order) = order_option.as_ref() {
                    PredictionMarketsClientModule::save_order_to_db(
                        &mut dbtx.get_isolated(),
                        id,
                        order,
                    )
                    .await;

                    dbtx.commit_tx().await;
                }

                Ok(order_option)
            }
        }
    }

    async fn cancel_order(&self, id: OrderIdClientSide) -> anyhow::Result<()> {
        let (prediction_markets, instance) =
            self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let operation_id = OperationId::new_random();

        let order_key = prediction_markets.order_id_to_key_pair(id);

        let input = ClientInput {
            input: PredictionMarketsInput::CancelOrder {
                order: XOnlyPublicKey::from_keypair(&order_key).0,
            },
            state_machines: Arc::new(move |tx_id, _| {
                vec![PredictionMarketsStateMachine::CancelOrder {
                    operation_id,
                    tx_id,
                    order: id,
                }]
            }),
            keys: vec![order_key],
        };

        let tx = TransactionBuilder::new().with_input(input.into_dyn(instance.id));
        let outpoint = |txid, _| OutPoint { txid, out_idx: 0 };
        let txid = self
            .finalize_and_submit_transaction(
                operation_id,
                PredictionMarketsCommonGen::KIND.as_str(),
                outpoint,
                tx,
            )
            .await?;

        let tx_subscription = self.transaction_updates(operation_id).await;
        tx_subscription.await_tx_accepted(txid).await?;

        Ok(())
    }

    async fn sync_orders(&self, market: Option<OutPoint>) -> anyhow::Result<()> {
        let (_, instance) = self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let mut dbtx = instance.db.begin_transaction().await;

        let non_zero_orders: Vec<_> = match market {
            None => {
                dbtx.find_by_prefix(&db::NonZeroOrdersByMarketOutcomePrefixAll)
                    .await
                    .map(|(key, _)| key.order)
                    .collect()
                    .await
            }
            Some(market) => {
                dbtx.find_by_prefix(&db::NonZeroOrdersByMarketOutcomePrefix1 { market })
                    .await
                    .map(|(key, _)| key.order)
                    .collect()
                    .await
            }
        };
        let mut non_zero_quantity_for_match_orders = vec![];
        for order_id in non_zero_orders {
            let order = self
                .get_order(order_id, true)
                .await
                .expect("should never error because from local cache")
                .expect("should always produce order");

            if order.quantity_waiting_for_match == ContractOfOutcomeAmount::ZERO {
                continue;
            }

            non_zero_quantity_for_match_orders.push(order_id);
        }
        let get_order_futures = non_zero_quantity_for_match_orders
            .iter()
            .map(|id| async { self.get_order(id.to_owned(), false).await });
        let results = future::join_all(get_order_futures).await;
        for result in results {
            result?.expect("should always produce order");
        }

        let need_update_orders: Vec<_> = dbtx
            .find_by_prefix(&db::OrderNeedsUpdatePrefixAll)
            .await
            .map(|(key, _)| key.order)
            .collect()
            .await;
        let get_order_futures = need_update_orders
            .iter()
            .map(|id| async { self.get_order(id.to_owned(), false).await });
        let results = future::join_all(get_order_futures).await;
        for result in results {
            result?.expect("should always produce order");
        }

        Ok(())
    }

    async fn get_all_orders_from_db(&self) -> BTreeMap<OrderIdClientSide, Order> {
        let (_, instance) = self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let mut dbtx = instance.db.begin_transaction().await;

        let mut order_stream = dbtx.find_by_prefix(&db::OrderPrefixAll).await;
        let mut orders = BTreeMap::new();
        loop {
            let Some((key, value)) = order_stream.next().await else {
                break;
            };
            let OrderIdSlot::Order(order) = value else {
                continue;
            };
            orders.insert(key.id, order);
        }

        orders
    }
}

#[derive(Debug)]
pub struct PredictionMarketsClientModule {
    cfg: PredictionMarketsClientConfig,
    root_secret: DerivableSecret,
    _notifier: ModuleNotifier<DynGlobalClientContext, PredictionMarketsStateMachine>,
}

impl PredictionMarketsClientModule {
    const MARKET_OUTCOME_CONTROL_FROM_ROOT_SECRET: ChildId = ChildId(0);
    const ORDER_FROM_ROOT_SECRET: ChildId = ChildId(1);

    fn get_market_outcome_control_key_pair(&self) -> KeyPair {
        self.root_secret
            .child_key(Self::MARKET_OUTCOME_CONTROL_FROM_ROOT_SECRET)
            .to_secp_key(&Secp256k1::new())
    }

    fn order_id_to_key_pair(&self, id: OrderIdClientSide) -> KeyPair {
        self.root_secret
            .child_key(Self::ORDER_FROM_ROOT_SECRET)
            .child_key(ChildId(id.0))
            .to_secp_key(&Secp256k1::new())
    }

    async fn save_order_to_db(
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        id: OrderIdClientSide,
        order: &Order,
    ) {
        dbtx.insert_entry(&db::OrderKey { id }, &OrderIdSlot::Order(order.to_owned()))
            .await;

        dbtx.insert_entry(
            &db::OrdersByMarketOutcomeKey {
                market: order.market,
                outcome: order.outcome,
                order: id,
            },
            &(),
        )
        .await;

        if order.quantity_waiting_for_match != ContractOfOutcomeAmount::ZERO
            || order.contract_of_outcome_balance != ContractOfOutcomeAmount::ZERO
            || order.bitcoin_balance != Amount::ZERO
        {
            dbtx.insert_entry(
                &db::NonZeroOrdersByMarketOutcomeKey {
                    market: order.market,
                    outcome: order.outcome,
                    order: id,
                },
                &(),
            )
            .await;
        } else {
            dbtx.remove_entry(&db::NonZeroOrdersByMarketOutcomeKey {
                market: order.market,
                outcome: order.outcome,
                order: id,
            })
            .await;
        }

        dbtx.remove_entry(&db::OrderNeedsUpdateKey { order: id })
            .await;
    }

    async fn new_order(mut dbtx: ModuleDatabaseTransaction<'_>, order: OrderIdClientSide) {
        dbtx.insert_entry(&db::OrderKey { id: order }, &OrderIdSlot::Reserved)
            .await;
    }

    async fn new_order_accepted(
        mut dbtx: ModuleDatabaseTransaction<'_>,
        order: OrderIdClientSide,
        sources: Vec<OrderIdClientSide>,
    ) {
        dbtx.insert_entry(&db::OrderNeedsUpdateKey { order }, &())
            .await;
        for source in sources {
            dbtx.insert_entry(&db::OrderNeedsUpdateKey { order: source }, &())
                .await;
        }
    }

    async fn new_order_failed(mut dbtx: ModuleDatabaseTransaction<'_>, order: OrderIdClientSide) {
        dbtx.remove_entry(&db::OrderKey { id: order }).await;
    }

    async fn cancel_order_accepted(
        mut dbtx: ModuleDatabaseTransaction<'_>,
        order: OrderIdClientSide,
    ) {
        dbtx.insert_entry(&db::OrderNeedsUpdateKey { order }, &())
            .await;
    }
}

/// Data needed by the state machine
#[derive(Debug, Clone)]
pub struct PredictionMarketsClientContext {
    pub prediction_markets_decoder: Decoder,
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
            prediction_markets_decoder: self.decoder(),
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
            PredictionMarketsInput::ConsumeOrderBitcoinBalance {
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
                        MarketInformation {
                            title: "my market".to_owned(),
                            description: "this is my market".to_owned(),
                            outcome_titles: (0..outcomes).map(|i| {
                                let mut title = String::new();

                                title.push_str("Outcome ");
                                title.push_str(&i.to_string());

                                title
                            }).collect(),
                            expected_payout_time: UnixTimestamp::ZERO,
                        },
                    ).await?;

                   Ok(serde_json::to_value(market_out_point)?)
            }

            "get-market" => {
                if args.len() != 2 {
                    return Err(anyhow::format_err!(
                        "`get-market` command expects 1 argument: <market_txid>"
                    ));
                }

                let Ok(txid) = TransactionId::from_str(&args[1].to_string_lossy()) else {
                    bail!("Error getting transaction id");
                };

                let out_point = OutPoint { txid, out_idx: 0 };

                Ok(serde_json::to_value(client.get_market(out_point, false).await?)?)
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

                let quantity = ContractOfOutcomeAmount(args[5].to_string_lossy().parse()?);

                Ok(serde_json::to_value(
                    client
                        .new_order(out_point, outcome, side, price, quantity)
                        .await?,
                )?)
            }

            "list-orders" => {
                let orders = client.get_all_orders_from_db().await;

                Ok(serde_json::to_value(orders)?)
            }

            "get-order" => {
                if args.len() != 2 {
                    return Err(anyhow::format_err!(
                        "`get-order` command expects 1 argument: <order_id>"
                    ));
                }

                let id = OrderIdClientSide(args[1].to_string_lossy().parse()?);

                Ok(serde_json::to_value(
                    client.get_order(id, false).await?,
                )?)
            }

            "cancel-order" => {
                if args.len() != 2 {
                    return Err(anyhow::format_err!(
                        "`cancel-order` command expects 1 argument: <order_id>"
                    ));
                }

                let id = OrderIdClientSide(args[1].to_string_lossy().parse()?);

                Ok(serde_json::to_value(client.cancel_order(id).await?)?)
            }

            "sync-orders" => {
                if args.len() != 1 && args.len() != 2 {
                    return Err(anyhow::format_err!(
                        "`sync-order` command only accepts 1 argument: <market_out_point>"
                    ));
                }

                let mut market: Option<OutPoint> = None;
                if let Ok(tx_id) = TransactionId::from_str(&args[1].to_string_lossy()) {
                    market = Some(OutPoint{txid: tx_id, out_idx: 0});
                };

                Ok(serde_json::to_value(client.sync_orders(market).await?)?)
            }

            command => Err(anyhow::format_err!(
                "Unknown command: {command}, supported commands: new-market, get-market, payout-market, new-order, get-order, cancel-order, sync-orders"
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
            _notifier: notifier,
        })
    }
}
