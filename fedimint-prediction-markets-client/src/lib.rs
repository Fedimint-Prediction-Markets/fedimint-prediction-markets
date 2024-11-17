use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::iter;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail};
use async_stream::stream;
use db::OrderIdSlot;
use fedimint_api_client::api::DynModuleApi;
use fedimint_client::derivable_secret::{ChildId, DerivableSecret};
use fedimint_client::module::init::{ClientModuleInit, ClientModuleInitArgs};
use fedimint_client::module::recovery::NoModuleBackup;
use fedimint_client::module::{ClientContext, ClientModule, IClientModule};
use fedimint_client::sm::{Context, ModuleNotifier};
use fedimint_client::transaction::{ClientInput, ClientOutput, TransactionBuilder};
use fedimint_core::core::{Decoder, OperationId};
use fedimint_core::db::{
    Database, DatabaseTransaction, DatabaseVersion, IDatabaseTransactionOpsCoreTyped,
};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::module::{
    ApiVersion, CommonModuleInit, ModuleCommon, ModuleInit, MultiApiVersion,
};
use fedimint_core::task::{sleep_until, spawn};
use fedimint_core::util::BoxStream;
use fedimint_core::{apply, async_trait_maybe_send, Amount, OutPoint, TransactionId};
use fedimint_prediction_markets_common::api::{
    GetEventPayoutAttestationsUsedToPermitPayoutParams, GetMarketDynamicParams,
    GetMarketOutcomeCandlesticksParams, GetMarketOutcomeCandlesticksResult, GetMarketParams,
    GetOrderParams, WaitMarketOutcomeCandlesticksParams, WaitMarketOutcomeCandlesticksResult,
    WaitOrderMatchParams, WaitOrderMatchResult,
};
use fedimint_prediction_markets_common::config::{GeneralConsensus, PredictionMarketsClientConfig};
use fedimint_prediction_markets_common::{
    Candlestick, ContractOfOutcomeAmount, Market, NostrPublicKeyHex, Order, Outcome,
    PredictionMarketEventJson, PredictionMarketsCommonInit, PredictionMarketsInput,
    PredictionMarketsModuleTypes, PredictionMarketsOutput, Seconds, Side, UnixTimestamp, Weight,
    WeightRequiredForPayout,
};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use order_filter::{OrderFilter, OrderPath, OrderState};
use secp256k1::{KeyPair, PublicKey, Scalar, Secp256k1};
use serde::{Deserialize, Serialize};
use states::{
    CancelOrderState, ConsumeOrderBitcoinBalanceState, NewMarketState, NewOrderState,
    PayoutMarketState, PredictionMarketState, PredictionMarketsStateMachine,
};
use tokio::select;
use tokio::sync::broadcast;
use tokio::time::Instant;

use crate::api::PredictionMarketsFederationApi;

mod api;
#[cfg(feature = "cli")]
mod cli;
mod db;
mod states;

pub mod order_filter;
pub mod stop_signal;

#[derive(Debug)]
pub struct PredictionMarketsClientModule {
    cfg: PredictionMarketsClientConfig,
    root_secret: DerivableSecret,
    notifier: ModuleNotifier<PredictionMarketsStateMachine>,
    ctx: ClientContext<Self>,
    db: Database,
    module_api: DynModuleApi,

    new_order_broadcast: Arc<(broadcast::Sender<OrderId>, broadcast::Receiver<OrderId>)>,
}

/// Data needed by the state machine
#[derive(Debug, Clone)]
pub struct PredictionMarketsClientContext {
    pub prediction_markets_decoder: Decoder,
    pub new_order_broadcast_sender: broadcast::Sender<OrderId>,
    pub root_secret: DerivableSecret,
}

impl Context for PredictionMarketsClientContext {}

#[derive(Debug, Clone)]
pub struct PredictionMarketsClientInit;

impl ModuleInit for PredictionMarketsClientInit {
    type Common = PredictionMarketsCommonInit;
    const DATABASE_VERSION: DatabaseVersion = DatabaseVersion(0);

    async fn dump_database(
        &self,
        _dbtx: &mut DatabaseTransaction<'_>,
        _prefix_names: Vec<String>,
    ) -> Box<dyn Iterator<Item = (String, Box<dyn erased_serde::Serialize + Send>)> + '_> {
        Box::new(iter::empty())
    }
}

#[apply(async_trait_maybe_send!)]
impl ClientModuleInit for PredictionMarketsClientInit {
    type Module = PredictionMarketsClientModule;

    fn supported_api_versions(&self) -> MultiApiVersion {
        MultiApiVersion::try_from_iter([ApiVersion::new(0, 0)]).expect("no version conflicts")
    }

    async fn init(&self, args: &ClientModuleInitArgs<Self>) -> anyhow::Result<Self::Module> {
        let new_order_broadcast = Arc::new(broadcast::channel(100));

        Ok(PredictionMarketsClientModule {
            cfg: args.cfg().to_owned(),
            root_secret: args.module_root_secret().to_owned(),
            notifier: args.notifier().to_owned(),
            ctx: args.context(),
            db: args.db().to_owned(),
            module_api: args.module_api().to_owned(),

            new_order_broadcast,
        })
    }
}

#[apply(async_trait_maybe_send!)]
impl ClientModule for PredictionMarketsClientModule {
    type Init = PredictionMarketsClientInit;
    type Common = PredictionMarketsModuleTypes;
    type Backup = NoModuleBackup;
    type ModuleStateMachineContext = PredictionMarketsClientContext;
    type States = PredictionMarketsStateMachine;

    fn context(&self) -> Self::ModuleStateMachineContext {
        PredictionMarketsClientContext {
            prediction_markets_decoder: self.decoder(),
            new_order_broadcast_sender: self.new_order_broadcast.0.clone(),
            root_secret: self.root_secret.clone(),
        }
    }

    fn input_fee(&self, input: &<Self::Common as ModuleCommon>::Input) -> Option<Amount> {
        Some(match input {
            PredictionMarketsInput::CancelOrder { .. } => Amount::ZERO,
            PredictionMarketsInput::ConsumeOrderBitcoinBalance { .. } => {
                self.cfg.gc.consume_order_bitcoin_balance_fee
            }
            PredictionMarketsInput::NewSellOrder { .. } => self.cfg.gc.new_order_fee,
        })
    }

    fn output_fee(&self, output: &<Self::Common as ModuleCommon>::Output) -> Option<Amount> {
        Some(match output {
            PredictionMarketsOutput::NewMarket { .. } => self.cfg.gc.new_market_fee,
            PredictionMarketsOutput::NewBuyOrder { .. } => self.cfg.gc.new_order_fee,
            PredictionMarketsOutput::PayoutMarket { .. } => Amount::ZERO,
        })
    }

    #[cfg(feature = "cli")]
    async fn handle_cli_command(
        &self,
        args: &[std::ffi::OsString],
    ) -> anyhow::Result<serde_json::Value> {
        cli::handle_cli_command(self, args).await
    }

    fn supports_backup(&self) -> bool {
        false
    }
}

/// Public api
impl PredictionMarketsClientModule {
    pub fn get_general_consensus(&self) -> GeneralConsensus {
        self.cfg.gc.to_owned()
    }

    pub async fn new_market(
        &self,
        event_json: PredictionMarketEventJson,
        contract_price: Amount,
        payout_control_weight_map: BTreeMap<NostrPublicKeyHex, Weight>,
        weight_required_for_payout: WeightRequiredForPayout,
    ) -> anyhow::Result<OutPoint> {
        let operation_id = OperationId::new_random();

        let output = ClientOutput {
            output: PredictionMarketsOutput::NewMarket {
                event_json,
                contract_price,
                payout_control_weight_map,
                weight_required_for_payout,
            },
            amount: Amount::ZERO,
            state_machines: Arc::new(move |tx_id, _| {
                vec![PredictionMarketsStateMachine {
                    operation_id,
                    state: NewMarketState::Pending { tx_id }.into(),
                }]
            }),
        };

        let tx = TransactionBuilder::new().with_output(self.ctx.make_client_output(output));
        let out_point = |txid, _| OutPoint { txid, out_idx: 0 };
        let (tx_id, _) = self
            .ctx
            .finalize_and_submit_transaction(
                operation_id,
                PredictionMarketsCommonInit::KIND.as_str(),
                out_point,
                tx,
            )
            .await?;

        self.await_accepted(operation_id, tx_id).await?;
        self.await_state(operation_id, |s| {
            matches!(
                s,
                PredictionMarketState::NewMarket(NewMarketState::Complete)
            )
        })
        .await;

        Ok(OutPoint {
            txid: tx_id,
            out_idx: 0,
        })
    }

    pub async fn get_market(
        &self,
        market: OutPoint,
        from_local_cache: bool,
    ) -> anyhow::Result<Option<Market>> {
        let mut dbtx = self.db.begin_transaction().await;
        let market_out_point = market;

        match from_local_cache {
            true => Ok(dbtx.get_value(&db::MarketKey(market_out_point)).await),

            false => {
                if let Some(mut market) = dbtx.get_value(&db::MarketKey(market_out_point)).await {
                    // if in finished state in db, just return db version
                    if market.1.payout.is_some() {
                        return Ok(Some(market));
                    }

                    // if we have market but not finished, update market dynamic
                    let result = self
                        .module_api
                        .get_market_dynamic(GetMarketDynamicParams {
                            market: market_out_point,
                        })
                        .await?;
                    let Some(market_dynamic) = result.market_dynamic else {
                        bail!("server returned no market_dynamic when it should exist")
                    };
                    market.1 = market_dynamic;
                    dbtx.insert_entry(&db::MarketKey(market_out_point), &market)
                        .await;
                    dbtx.commit_tx_result().await?;

                    return Ok(Some(market));
                }

                let result = self
                    .module_api
                    .get_market(GetMarketParams {
                        market: market_out_point,
                    })
                    .await?;
                if let Some(market) = result.market.as_ref() {
                    dbtx.insert_entry(&db::MarketKey(market_out_point), market)
                        .await;
                    dbtx.commit_tx_result().await?;
                }

                Ok(result.market)
            }
        }
    }

    pub async fn payout_market(
        &self,
        market: OutPoint,
        event_payout_attestations_json: Vec<PredictionMarketEventJson>,
    ) -> anyhow::Result<()> {
        let operation_id = OperationId::new_random();

        let output = ClientOutput {
            output: PredictionMarketsOutput::PayoutMarket {
                market,
                event_payout_attestations_json,
            },
            amount: Amount::ZERO,
            state_machines: Arc::new(move |tx_id, _| {
                vec![PredictionMarketsStateMachine {
                    operation_id,
                    state: PayoutMarketState::Pending { tx_id }.into(),
                }]
            }),
        };

        let tx = TransactionBuilder::new().with_output(self.ctx.make_client_output(output));
        let out_point = |txid, _| OutPoint { txid, out_idx: 0 };
        let (tx_id, _) = self
            .ctx
            .finalize_and_submit_transaction(
                operation_id,
                PredictionMarketsCommonInit::KIND.as_str(),
                out_point,
                tx,
            )
            .await?;

        self.await_accepted(operation_id, tx_id).await?;
        self.await_state(operation_id, |s| {
            matches!(
                s,
                PredictionMarketState::PayoutMarket(PayoutMarketState::Complete)
            )
        })
        .await;

        Ok(())
    }

    pub async fn get_event_payout_attestations_used_to_permit_payout(
        &self,
        market: OutPoint,
    ) -> anyhow::Result<Option<Vec<PredictionMarketEventJson>>> {
        let result = self
            .module_api
            .get_event_payout_attestations_used_to_permit_payout(
                GetEventPayoutAttestationsUsedToPermitPayoutParams { market },
            )
            .await;

        Ok(result?.event_payout_attestations)
    }

    pub async fn new_order(
        &self,
        market: OutPoint,
        outcome: Outcome,
        side: Side,
        price: Amount,
        quantity: ContractOfOutcomeAmount,
    ) -> anyhow::Result<OrderId> {
        let operation_id = OperationId::new_random();
        let db = self.db.clone();
        let mut dbtx = db.begin_transaction().await;

        let order_id = {
            let mut stream = dbtx
                .find_by_prefix_sorted_descending(&db::OrderPrefixAll)
                .await;
            match stream.next().await {
                Some((mut key, _)) => {
                    key.0 .0 += 1;
                    key.0
                }
                None => OrderId(0),
            }
        };

        dbtx.insert_entry(&db::OrderKey(order_id), &OrderIdSlot::Reserved)
            .await;

        let order_key = self.order_id_to_key_pair(order_id);
        let owner = PublicKey::from_keypair(&order_key);

        let mut tx = TransactionBuilder::new();
        let mut orders_to_sync_on_accepted = BTreeSet::new();
        orders_to_sync_on_accepted.insert(order_id);
        let mut orders_to_sync_on_rejected = BTreeSet::new();
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
                    amount: price * quantity.0,
                    state_machines: Arc::new(move |tx_id, _| {
                        vec![PredictionMarketsStateMachine {
                            operation_id,
                            state: NewOrderState::Pending {
                                tx_id,
                                order_id,
                                orders_to_sync_on_accepted: orders_to_sync_on_accepted.clone(),
                                orders_to_sync_on_rejected: orders_to_sync_on_rejected.clone(),
                            }
                            .into(),
                        }]
                    }),
                };

                tx = tx.with_output(self.ctx.make_client_output(output));
            }
            Side::Sell => {
                let mut sources = BTreeMap::new();
                let mut sources_keys_combined = None;

                let possible_source_orders = Self::get_order_ids(
                    &mut dbtx.to_ref_nc(),
                    OrderFilter(
                        OrderPath::MarketOutcomeSide {
                            market,
                            outcome,
                            side,
                        },
                        OrderState::NonZeroContractOfOutcomeBalance,
                    ),
                )
                .await;

                let mut sourced_quantity = ContractOfOutcomeAmount::ZERO;
                for (i, loop_order_id) in possible_source_orders.into_iter().enumerate() {
                    if i == usize::from(self.cfg.gc.max_sell_order_sources) {
                        bail!("max number of sell order sources reached. try again with a quantity less than or equal to {}", sourced_quantity.0)
                    }

                    let mut loop_order = dbtx
                        .get_value(&db::OrderKey(loop_order_id))
                        .await
                        .unwrap()
                        .to_order()
                        .unwrap();

                    let loop_order_key = self.order_id_to_key_pair(loop_order_id);
                    let loop_sourced_quantity_from_order = loop_order
                        .contract_of_outcome_balance
                        .min(quantity - sourced_quantity);
                    loop_order.contract_of_outcome_balance -= loop_sourced_quantity_from_order;
                    sourced_quantity += loop_sourced_quantity_from_order;

                    sources.insert(
                        loop_order_key.public_key(),
                        loop_sourced_quantity_from_order,
                    );

                    dbtx.insert_entry(
                        &db::OrderKey(loop_order_id),
                        &OrderIdSlot::Order(loop_order),
                    )
                    .await;
                    orders_to_sync_on_accepted.insert(loop_order_id);
                    orders_to_sync_on_rejected.insert(loop_order_id);

                    sources_keys_combined = match sources_keys_combined {
                        None => Some(loop_order_key),
                        Some(combined_keys) => {
                            let p1 = combined_keys.secret_key();
                            let p2 = loop_order_key.secret_key();
                            let p3 = p1.add_tweak(&Scalar::from(p2))?;

                            Some(p3.keypair(secp256k1::SECP256K1))
                        }
                    };

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
                    amount: Amount::ZERO,
                    state_machines: Arc::new(move |tx_id, _| {
                        vec![PredictionMarketsStateMachine {
                            operation_id,
                            state: NewOrderState::Pending {
                                tx_id,
                                order_id,
                                orders_to_sync_on_accepted: orders_to_sync_on_accepted.clone(),
                                orders_to_sync_on_rejected: orders_to_sync_on_rejected.clone(),
                            }
                            .into(),
                        }]
                    }),
                    keys: vec![sources_keys_combined.unwrap()],
                };

                tx = tx.with_input(self.ctx.make_client_input(input));
            }
        }

        dbtx.commit_tx_result().await?;

        let (tx_id, _) = self
            .ctx
            .finalize_and_submit_transaction(
                operation_id,
                PredictionMarketsCommonInit::KIND.as_str(),
                |_, _| (),
                tx,
            )
            .await?;

        self.await_accepted(operation_id, tx_id).await?;
        self.await_state(operation_id, |s| {
            matches!(s, PredictionMarketState::NewOrder(NewOrderState::Complete))
        })
        .await;

        Ok(order_id)
    }

    pub async fn get_order(
        &self,
        order_id: OrderId,
        from_local_cache: bool,
    ) -> anyhow::Result<Option<Order>> {
        let mut dbtx = self.db.begin_transaction().await;

        let order_owner = self.order_id_to_key_pair(order_id).public_key();

        let res = match from_local_cache {
            true => Ok(dbtx
                .get_value(&db::OrderKey(order_id))
                .await
                .map(|v| v.to_order())
                .flatten()),

            false => {
                let result = self
                    .module_api
                    .get_order(GetOrderParams { order: order_owner })
                    .await?;

                if let Some(order) = result.order.as_ref() {
                    PredictionMarketsClientModule::save_order_to_db(
                        &mut dbtx.to_ref_nc(),
                        order_id,
                        order,
                    )
                    .await;
                }

                Ok(result.order)
            }
        };

        dbtx.commit_tx_result().await?;

        res
    }

    pub async fn get_orders_from_db(&self, filter: OrderFilter) -> BTreeMap<OrderId, Order> {
        Self::get_order_ids(&mut self.db.begin_transaction_nc().await, filter)
            .await
            .into_iter()
            .map(|order_id| async move {
                (
                    order_id,
                    self.get_order(order_id, true).await.unwrap().unwrap(),
                )
            })
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await
    }

    pub async fn stream_order_from_db<'a>(&self, id: OrderId) -> BoxStream<'a, Option<Order>> {
        let db = self.db.clone();

        Self::stream_order_from_db_internal(db, id).await
    }

    pub async fn stream_orders_from_db<'a>(
        &self,
        filter: OrderFilter,
    ) -> BoxStream<'a, (OrderId, BoxStream<'a, Option<Order>>)> {
        let db = self.db.clone();
        let mut new_order_reciever = self.new_order_broadcast.0.subscribe();

        let mut orders_to_stream =
            Self::get_order_ids(&mut db.begin_transaction_nc().await, filter).await;
        let stream = Box::pin(stream! {
            loop {
                while let Some(order_id) = orders_to_stream.pop_first() {
                    let stream = Self::stream_order_from_db_internal(db.clone(), order_id).await;

                    yield (order_id, stream);
                }

                select! {
                    res = new_order_reciever.recv() => {
                        if let Ok(order_id) = res {
                            let mut dbtx = db.begin_transaction_nc().await;
                            let order = dbtx.get_value(&db::OrderKey(order_id)).await.unwrap().to_order().unwrap();
                            if filter.filter(&order) {
                                orders_to_stream.insert(order_id);
                            }
                        }
                    }
                }
            }
        });

        stream
    }

    pub async fn cancel_order(&self, order_id: OrderId) -> anyhow::Result<()> {
        let operation_id = OperationId::new_random();

        let order_key = self.order_id_to_key_pair(order_id);
        let order_owner = order_key.public_key();

        let input = ClientInput {
            input: PredictionMarketsInput::CancelOrder { order: order_owner },
            state_machines: Arc::new(move |tx_id, _| {
                vec![PredictionMarketsStateMachine {
                    operation_id,
                    state: CancelOrderState::Pending {
                        tx_id,
                        order_to_sync_on_accepted: order_id,
                    }
                    .into(),
                }]
            }),
            amount: Amount::ZERO,
            keys: vec![order_key],
        };

        let tx = TransactionBuilder::new().with_input(self.ctx.make_client_input(input));
        let (tx_id, _) = self
            .ctx
            .finalize_and_submit_transaction(
                operation_id,
                PredictionMarketsCommonInit::KIND.as_str(),
                |_, _| (),
                tx,
            )
            .await?;

        self.await_accepted(operation_id, tx_id).await?;
        self.await_state(operation_id, |s| {
            matches!(
                s,
                PredictionMarketState::CancelOrder(CancelOrderState::Complete)
            )
        })
        .await;

        Ok(())
    }

    /// send all bitcoin balance from orders to primary module
    pub async fn send_order_bitcoin_balance_to_primary_module(&self) -> anyhow::Result<Amount> {
        let operation_id = OperationId::new_random();

        let mut dbtx = self.db.begin_transaction().await;

        let orders_with_non_zero_bitcoin_balance = Self::get_order_ids(
            &mut dbtx.to_ref_nc(),
            OrderFilter(OrderPath::All, OrderState::NonZeroBitcoinBalance),
        )
        .await;

        if orders_with_non_zero_bitcoin_balance.len() == 0 {
            return Ok(Amount::ZERO);
        }

        let mut total_amount = Amount::ZERO;
        let mut tx = TransactionBuilder::new();
        for order_id in orders_with_non_zero_bitcoin_balance {
            let order = self.get_order(order_id, true).await?.unwrap();
            let order_key = self.order_id_to_key_pair(order_id);

            let input = ClientInput {
                input: PredictionMarketsInput::ConsumeOrderBitcoinBalance {
                    order: order_key.public_key(),
                    amount: order.bitcoin_balance,
                },
                amount: order.bitcoin_balance,
                state_machines: Arc::new(move |tx_id, _| {
                    vec![PredictionMarketsStateMachine {
                        operation_id,
                        state: ConsumeOrderBitcoinBalanceState::Pending {
                            tx_id,
                            order_to_sync_on_accepted: order_id,
                        }
                        .into(),
                    }]
                }),
                keys: vec![order_key],
            };

            tx = tx.with_input(self.ctx.make_client_input(input));

            total_amount += order.bitcoin_balance;
        }

        let outpoint = |txid, _| OutPoint { txid, out_idx: 0 };
        let (tx_id, _) = self
            .ctx
            .finalize_and_submit_transaction(
                operation_id,
                PredictionMarketsCommonInit::KIND.as_str(),
                outpoint,
                tx,
            )
            .await?;

        self.await_accepted(operation_id, tx_id).await?;
        self.await_state(operation_id, |s| {
            matches!(
                s,
                PredictionMarketState::ConsumeOrderBitcoinBalance(
                    ConsumeOrderBitcoinBalanceState::Complete
                )
            )
        })
        .await;

        Ok(total_amount)
    }

    /// TODO docs
    pub async fn sync_possible_payouts(
        &self,
        market_specifier: Option<OutPoint>,
    ) -> anyhow::Result<()> {
        let mut dbtx = self.db.begin_transaction().await;

        let markets_to_check_payout: BTreeSet<_> = match market_specifier {
            Some(market) => iter::once(market).collect(),
            None => {
                dbtx.find_by_prefix(
                    &db::OrdersWithNonZeroContractOfOutcomeBalanceByMarketOutcomeSidePrefixAll,
                )
                .await
                .map(|(k, _)| k.market)
                .collect()
                .await
            }
        };

        let get_markets_result: Vec<_> = markets_to_check_payout
            .into_iter()
            .map(|market| async move { (market, self.get_market(market, false).await) })
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await;

        let mut orders_to_update = BTreeSet::new();
        for (market, res) in get_markets_result {
            if res?.ok_or(anyhow!("market not found"))?.1.payout.is_none() {
                continue;
            }

            let mut market_orders_mutated_by_payout: BTreeSet<_> = Self::get_order_ids(
                &mut dbtx.to_ref_nc(),
                OrderFilter(
                    OrderPath::Market { market },
                    OrderState::NonZeroContractOfOutcomeBalance,
                ),
            )
            .await;

            orders_to_update.append(&mut market_orders_mutated_by_payout);
        }

        self.sync_orders_from_federation_concurrent_with_self(
            orders_to_update.into_iter().collect(),
        )
        .await?;

        Ok(())
    }

    pub async fn sync_possible_order_matches(&self, order_path: OrderPath) -> anyhow::Result<()> {
        let active_quantiy_orders: BTreeSet<_> = Self::get_order_ids(
            &mut self.db.begin_transaction_nc().await,
            OrderFilter(order_path, OrderState::NonZeroQuantityWaitingForMatch),
        )
        .await;

        self.sync_orders_from_federation_concurrent_with_self(
            active_quantiy_orders.into_iter().collect(),
        )
        .await?;

        Ok(())
    }

    pub async fn watch_for_order_matches(
        &self,
        order_path: OrderPath,
    ) -> anyhow::Result<Pin<Box<dyn Future<Output = anyhow::Result<()>>>>> {
        let mut watch_args = Vec::new();
        match order_path {
            OrderPath::All => unimplemented!(),
            OrderPath::Market { market } => {
                let market_outcome_count = match self.get_market(market, true).await? {
                    Some(market) => market,
                    None => self
                        .get_market(market, false)
                        .await?
                        .ok_or(anyhow!("market does not exist"))?,
                }
                .0
                .event()?
                .outcome_count;

                for outcome in 0..market_outcome_count {
                    watch_args.push((market, outcome, Side::Buy));
                    watch_args.push((market, outcome, Side::Sell));
                }
            }
            OrderPath::MarketOutcome { market, outcome } => {
                watch_args.push((market, outcome, Side::Buy));
                watch_args.push((market, outcome, Side::Sell));
            }
            OrderPath::MarketOutcomeSide {
                market,
                outcome,
                side,
            } => {
                watch_args.push((market, outcome, side));
            }
        };

        let results = watch_args
            .into_iter()
            .map(|(market, outcome, side)| async move {
                self.watch_for_order_matches_on_market_outcome_side(market, outcome, side)
                    .await
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await;
        let mut close_signals = Vec::new();
        let mut close_all_and_error = false;
        for result in results {
            match result {
                Ok(s) => close_signals.push(s),
                Err(_) => close_all_and_error = true,
            }
        }
        if close_all_and_error {
            for s in close_signals {
                s.wait_close().await?;
            }
            bail!("failed initialization of order match watchers")
        }
        let stop_future = Box::pin(async move {
            let mut last_error = None;
            for s in close_signals {
                if let Err(e) = s.wait_close().await {
                    last_error = Some(e);
                }
            }
            if let Some(e) = last_error {
                bail!("failed to stop 1 or more watchers: {e}")
            }

            Ok(())
        });

        Ok(stop_future)
    }

    /// Scans for all orders that the client owns.
    pub async fn resync_order_slots(&self, gap_size_to_check: usize) -> anyhow::Result<()> {
        let mut order_id = OrderId(0);
        let mut slots_without_order = 0;
        loop {
            if let Some(_) = self.get_order(order_id, false).await? {
                slots_without_order = 0;
            } else {
                slots_without_order += 1;
                if slots_without_order == gap_size_to_check {
                    break;
                }
            }

            order_id.0 += 1;
        }

        Ok(())
    }

    /// get most recent candlesticks
    pub async fn get_candlesticks(
        &self,
        market: OutPoint,
        outcome: Outcome,
        candlestick_interval: Seconds,
        min_candlestick_timestamp: UnixTimestamp,
    ) -> anyhow::Result<BTreeMap<UnixTimestamp, Candlestick>> {
        let GetMarketOutcomeCandlesticksResult { candlesticks } = self
            .module_api
            .get_market_outcome_candlesticks(GetMarketOutcomeCandlesticksParams {
                market,
                outcome,
                candlestick_interval,
                min_candlestick_timestamp,
            })
            .await?;

        let candlesticks = candlesticks.into_iter().collect::<BTreeMap<_, _>>();

        Ok(candlesticks)
    }

    /// wait for new candlesticks
    pub async fn wait_candlesticks(
        &self,
        market: OutPoint,
        outcome: Outcome,
        candlestick_interval: Seconds,
        candlestick_timestamp: UnixTimestamp,
        candlestick_volume: ContractOfOutcomeAmount,
    ) -> anyhow::Result<BTreeMap<UnixTimestamp, Candlestick>> {
        let WaitMarketOutcomeCandlesticksResult { candlesticks } = self
            .module_api
            .wait_market_outcome_candlesticks(WaitMarketOutcomeCandlesticksParams {
                market,
                outcome,
                candlestick_interval,
                candlestick_timestamp,
                candlestick_volume,
            })
            .await?;

        let candlesticks = candlesticks.into_iter().collect::<BTreeMap<_, _>>();

        Ok(candlesticks)
    }

    pub async fn stream_candlesticks<'a>(
        &self,
        market: OutPoint,
        outcome: Outcome,
        candlestick_interval: Seconds,
        min_candlestick_timestamp: UnixTimestamp,
        min_duration_between_requests: Duration,
    ) -> BoxStream<'a, Vec<(UnixTimestamp, Candlestick)>> {
        let module_api = self.module_api.clone();

        Box::pin(stream! {
            let mut candlestick_timestamp = min_candlestick_timestamp;
            let mut candlestick_volume = ContractOfOutcomeAmount::ZERO;

            loop {
                let now = Instant::now();

                let res = module_api
                    .wait_market_outcome_candlesticks(WaitMarketOutcomeCandlesticksParams {
                        market,
                        outcome,
                        candlestick_interval,
                        candlestick_timestamp,
                        candlestick_volume,
                    })
                    .await
                    .map(|WaitMarketOutcomeCandlesticksResult { mut candlesticks }| {
                        candlesticks.sort_by(|a, b| a.0.cmp(&b.0));
                        candlesticks
                    });

                if let Ok(candlesticks) = res {
                    if let Some(newest_candle) = candlesticks.last() {
                        candlestick_timestamp = newest_candle.0;
                        candlestick_volume = newest_candle.1.volume;
                    }

                    yield candlesticks;
                }

                sleep_until(now + min_duration_between_requests).await;
            }
        })
    }

    /// Interacts with client saved markets.
    pub async fn save_market(&self, market: OutPoint) {
        let mut dbtx = self.db.begin_transaction().await;

        dbtx.insert_entry(&db::ClientSavedMarketsKey { market }, &UnixTimestamp::now())
            .await;
        dbtx.commit_tx().await;
    }

    /// Interacts with client saved markets.
    pub async fn unsave_market(&self, market: OutPoint) {
        let mut dbtx = self.db.begin_transaction().await;

        dbtx.remove_entry(&db::ClientSavedMarketsKey { market })
            .await;
        dbtx.commit_tx().await;
    }

    /// Interacts with client saved markets.
    ///
    /// return is Vec<(market outpoint, saved timestamp)>
    pub async fn get_saved_markets(&self) -> Vec<(OutPoint, UnixTimestamp)> {
        let mut dbtx = self.db.begin_transaction().await;

        dbtx.find_by_prefix(&db::ClientSavedMarketsPrefixAll)
            .await
            .map(|(k, v)| (k.market, v))
            .collect()
            .await
    }

    /// Interacts with client named payout control public keys
    pub async fn set_name_to_payout_control(
        &self,
        name: String,
        payout_control: Option<NostrPublicKeyHex>,
    ) {
        let mut dbtx = self.db.begin_transaction().await;

        match payout_control {
            Some(pk) => {
                dbtx.insert_entry(&db::ClientNamedPayoutControlsKey { name }, &pk)
                    .await;
            }
            None => {
                dbtx.remove_entry(&db::ClientNamedPayoutControlsKey { name })
                    .await;
            }
        }
        dbtx.commit_tx().await;
    }

    /// Interacts with client named payout control public keys
    pub async fn get_name_to_payout_control(&self, name: String) -> Option<NostrPublicKeyHex> {
        let mut dbtx = self.db.begin_transaction().await;

        dbtx.get_value(&db::ClientNamedPayoutControlsKey { name })
            .await
    }

    /// Interacts with client named payout control public keys
    pub async fn get_name_to_payout_control_map(&self) -> HashMap<String, NostrPublicKeyHex> {
        let mut dbtx = self.db.begin_transaction().await;

        dbtx.find_by_prefix(&db::ClientNamedPayoutControlsPrefixAll)
            .await
            .map(|(k, v)| (k.name, v))
            .collect()
            .await
    }
}

/// private
impl PredictionMarketsClientModule {
    fn order_id_to_key_pair(&self, order_id: OrderId) -> KeyPair {
        order_id.into_key_pair(self.root_secret.clone())
    }

    async fn save_order_to_db(dbtx: &mut DatabaseTransaction<'_>, id: OrderId, order: &Order) {
        dbtx.insert_entry(&db::OrderKey(id), &OrderIdSlot::Order(order.to_owned()))
            .await;

        dbtx.insert_entry(
            &db::OrdersByMarketOutcomeKey {
                market: order.market,
                outcome: order.outcome,
                side: order.side,
                order: id,
            },
            &(),
        )
        .await;

        if order.quantity_waiting_for_match != ContractOfOutcomeAmount::ZERO {
            dbtx.insert_entry(&db::OrderPriceTimePriorityKey::from_order(order), &id)
                .await;
        } else {
            dbtx.remove_entry(&db::OrderPriceTimePriorityKey::from_order(order))
                .await;
        }

        if order.contract_of_outcome_balance != ContractOfOutcomeAmount::ZERO {
            dbtx.insert_entry(
                &db::OrdersWithNonZeroContractOfOutcomeBalanceByMarketOutcomeSideKey {
                    market: order.market,
                    outcome: order.outcome,
                    side: order.side,
                    order: id,
                },
                &(),
            )
            .await;
        } else {
            dbtx.remove_entry(
                &db::OrdersWithNonZeroContractOfOutcomeBalanceByMarketOutcomeSideKey {
                    market: order.market,
                    outcome: order.outcome,
                    side: order.side,
                    order: id,
                },
            )
            .await;
        }

        if order.bitcoin_balance != Amount::ZERO {
            dbtx.insert_entry(
                &db::OrdersWithNonZeroBitcoinBalanceByMarketOutcomeSideKey {
                    market: order.market,
                    outcome: order.outcome,
                    side: order.side,
                    order: id,
                },
                &(),
            )
            .await;
        } else {
            dbtx.remove_entry(&db::OrdersWithNonZeroBitcoinBalanceByMarketOutcomeSideKey {
                market: order.market,
                outcome: order.outcome,
                side: order.side,
                order: id,
            })
            .await;
        }
    }

    async fn sync_orders_from_federation_concurrent(
        root_secret: DerivableSecret,
        module_api: DynModuleApi,
        db: Database,
        ids: Vec<OrderId>,
    ) -> anyhow::Result<()> {
        let mut futures = ids
            .into_iter()
            .map(|order_id| {
                let root_secret = root_secret.clone();
                let module_api = module_api.clone();
                async move {
                    let order_owner = order_id.into_key_pair(root_secret).public_key();

                    (
                        order_id,
                        module_api
                            .get_order(GetOrderParams { order: order_owner })
                            .await,
                    )
                }
            })
            .collect::<FuturesUnordered<_>>();

        let mut dbtx = db.begin_transaction().await;
        while let Some((order_id, res)) = futures.next().await {
            if let Some(order) = res?.order {
                PredictionMarketsClientModule::save_order_to_db(
                    &mut dbtx.to_ref_nc(),
                    order_id,
                    &order,
                )
                .await;
            }
        }
        dbtx.commit_tx_result().await?;

        Ok(())
    }

    async fn sync_orders_from_federation_concurrent_with_self(
        &self,
        ids: Vec<OrderId>,
    ) -> anyhow::Result<()> {
        Self::sync_orders_from_federation_concurrent(
            self.root_secret.clone(),
            self.module_api.clone(),
            self.db.clone(),
            ids,
        )
        .await
    }

    async fn await_accepted(
        &self,
        operation_id: OperationId,
        tx_id: TransactionId,
    ) -> anyhow::Result<()> {
        let tx_subscription = self.ctx.transaction_updates(operation_id).await;
        tx_subscription
            .await_tx_accepted(tx_id)
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(())
    }

    async fn await_state(
        &self,
        operation_id: OperationId,
        state_matcher: impl Fn(PredictionMarketState) -> bool,
    ) {
        let mut state_stream = self.notifier.subscribe(operation_id).await;
        while let Some(PredictionMarketsStateMachine {
            operation_id: _,
            state,
        }) = state_stream.next().await
        {
            if state_matcher(state) {
                break;
            }
        }
    }

    pub async fn stream_order_from_db_internal<'a>(
        db: Database,
        id: OrderId,
    ) -> BoxStream<'a, Option<Order>> {
        Box::pin(stream! {
            let mut previous_entry: Option<OrderIdSlot> = None;
            loop {
                let new_entry = db
                    .wait_key_check(&db::OrderKey(id), |db_value| { if db_value != previous_entry {
                        Some(db_value)
                    } else {
                        None
                    }})
                    .await
                    .0;

                yield new_entry.clone().map(|s| s.to_order()).flatten();

                previous_entry = new_entry;
            }
        })
    }

    async fn get_order_ids<'a>(
        dbtx: &mut DatabaseTransaction<'a>,
        filter: OrderFilter,
    ) -> BTreeSet<OrderId> {
        match filter.1 {
            OrderState::Any => {
                match filter.0 {
                    OrderPath::All => {
                        dbtx.find_by_prefix(&db::OrdersByMarketOutcomePrefixAll)
                            .await
                    }
                    OrderPath::Market { market } => {
                        dbtx.find_by_prefix(&db::OrdersByMarketOutcomePrefix1 { market })
                            .await
                    }
                    OrderPath::MarketOutcome { market, outcome } => {
                        dbtx.find_by_prefix(&db::OrdersByMarketOutcomePrefix2 { market, outcome })
                            .await
                    }
                    OrderPath::MarketOutcomeSide {
                        market,
                        outcome,
                        side,
                    } => {
                        dbtx.find_by_prefix(&db::OrdersByMarketOutcomePrefix3 {
                            market,
                            outcome,
                            side,
                        })
                        .await
                    }
                }
                .map(|(k, _)| k.order)
                .collect()
                .await
            }

            OrderState::NonZeroQuantityWaitingForMatch => {
                match filter.0 {
                    OrderPath::All => {
                        dbtx.find_by_prefix(&db::OrderPriceTimePriorityPrefixAll)
                            .await
                    }
                    OrderPath::Market { market } => {
                        dbtx.find_by_prefix(&db::OrderPriceTimePriorityPrefix1 { market })
                            .await
                    }
                    OrderPath::MarketOutcome { market, outcome } => {
                        dbtx.find_by_prefix(&db::OrderPriceTimePriorityPrefix2 { market, outcome })
                            .await
                    }
                    OrderPath::MarketOutcomeSide {
                        market,
                        outcome,
                        side,
                    } => {
                        dbtx.find_by_prefix(&db::OrderPriceTimePriorityPrefix3 {
                            market,
                            outcome,
                            side,
                        })
                        .await
                    }
                }
                .map(|(_, v)| v)
                .collect()
                .await
            }
            OrderState::NonZeroContractOfOutcomeBalance => match filter.0 {
                OrderPath::All => {
                    dbtx.find_by_prefix(
                        &db::OrdersWithNonZeroContractOfOutcomeBalanceByMarketOutcomeSidePrefixAll,
                    )
                    .await
                }
                OrderPath::Market { market } => {
                    dbtx.find_by_prefix(
                        &db::OrdersWithNonZeroContractOfOutcomeBalanceByMarketOutcomeSidePrefix1 {
                            market,
                        },
                    )
                    .await
                }
                OrderPath::MarketOutcome { market, outcome } => {
                    dbtx.find_by_prefix(
                        &db::OrdersWithNonZeroContractOfOutcomeBalanceByMarketOutcomeSidePrefix2 {
                            market,
                            outcome,
                        },
                    )
                    .await
                }
                OrderPath::MarketOutcomeSide {
                    market,
                    outcome,
                    side,
                } => {
                    dbtx.find_by_prefix(
                        &db::OrdersWithNonZeroContractOfOutcomeBalanceByMarketOutcomeSidePrefix3 {
                            market,
                            outcome,
                            side,
                        },
                    )
                    .await
                }
            }
            .map(|(k, _)| k.order)
            .collect()
            .await,
            OrderState::NonZeroBitcoinBalance => {
                match filter.0 {
                    OrderPath::All => {
                        dbtx.find_by_prefix(
                            &db::OrdersWithNonZeroBitcoinBalanceByMarketOutcomeSidePrefixAll,
                        )
                        .await
                    }
                    OrderPath::Market { market } => {
                        dbtx.find_by_prefix(
                            &db::OrdersWithNonZeroBitcoinBalanceByMarketOutcomeSidePrefix1 {
                                market,
                            },
                        )
                        .await
                    }
                    OrderPath::MarketOutcome { market, outcome } => {
                        dbtx.find_by_prefix(
                            &db::OrdersWithNonZeroBitcoinBalanceByMarketOutcomeSidePrefix2 {
                                market,
                                outcome,
                            },
                        )
                        .await
                    }
                    OrderPath::MarketOutcomeSide {
                        market,
                        outcome,
                        side,
                    } => {
                        dbtx.find_by_prefix(
                            &db::OrdersWithNonZeroBitcoinBalanceByMarketOutcomeSidePrefix3 {
                                market,
                                outcome,
                                side,
                            },
                        )
                        .await
                    }
                }
                .map(|(k, _)| k.order)
                .collect()
                .await
            }
        }
    }

    async fn watch_for_order_matches_on_market_outcome_side(
        &self,
        market: OutPoint,
        outcome: Outcome,
        side: Side,
    ) -> anyhow::Result<stop_signal::Sender> {
        let module_api = self.module_api.clone();
        let db = self.db.clone();
        let root_secret = self.root_secret.clone();
        let mut new_order_reciever = self.new_order_broadcast.0.subscribe();
        let (stop_tx, mut stop_rx) = stop_signal::new();

        self.sync_possible_order_matches(OrderPath::MarketOutcomeSide {
            market,
            outcome,
            side,
        })
        .await?;

        spawn(
            &format!("watch_orders_on_{market}_{outcome}_{side:?}"),
            async move {
                let mut order_to_watch = None;
                loop {
                    let wait_order_match_request = {
                        let mut dbtx = db.begin_transaction_nc().await;
                        if order_to_watch == None {
                            order_to_watch = dbtx
                                .find_by_prefix(&db::OrderPriceTimePriorityPrefix3 {
                                    market,
                                    outcome,
                                    side,
                                })
                                .await
                                .map(|(_, order_id)| order_id)
                                .next()
                                .await;
                        }

                        let order = if let Some(order_id) = order_to_watch {
                            Some(
                                dbtx.get_value(&db::OrderKey(order_id))
                                    .await
                                    .unwrap()
                                    .to_order()
                                    .unwrap(),
                            )
                        } else {
                            None
                        };

                        async {
                            if let Some(order_id) = order_to_watch {
                                module_api
                                    .wait_order_match(WaitOrderMatchParams {
                                        order: order_id
                                            .into_key_pair(root_secret.clone())
                                            .public_key(),
                                        current_quantity_waiting_for_match: order
                                            .unwrap()
                                            .quantity_waiting_for_match,
                                    })
                                    .await
                            } else {
                                futures::future::pending().await
                            }
                        }
                    };

                    select! {
                        _ = stop_rx.0.recv() => {
                            return;
                        }
                        res = wait_order_match_request => {
                            if let Ok(WaitOrderMatchResult {order}) = res {
                                while let Err(_) = {
                                    let mut dbtx = db.begin_transaction().await;
                                    Self::save_order_to_db(&mut dbtx.to_ref_nc(), order_to_watch.unwrap(), &order).await;
                                    dbtx.commit_tx_result().await
                                } {}
                                if order.quantity_waiting_for_match == ContractOfOutcomeAmount::ZERO {
                                    order_to_watch = None;
                                }
                            }
                        }
                        _ = new_order_reciever.recv() => {
                            let mut dbtx = db.begin_transaction_nc().await;
                            let order_priority_stream = dbtx
                                .find_by_prefix(&db::OrderPriceTimePriorityPrefix3 {
                                    market,
                                    outcome,
                                    side,
                                })
                                .await;
                            let highest_priority_orders_till_watched_order =
                                order_priority_stream
                                    .map(|(_, order_id)| order_id)
                                    .take_while(|order_id| {
                                        let order_id = *order_id;
                                        async move { Some(order_id) != order_to_watch }
                                    })
                                    .collect::<Vec<_>>()
                                    .await;
                            if highest_priority_orders_till_watched_order.len() == 0 {
                                continue;
                            }
                            let new_order_to_watch = highest_priority_orders_till_watched_order
                                .get(0)
                                .map(|id| *id);
                            let mut orders_to_sync = vec![];
                            if let Some(watched_order) = new_order_to_watch {
                                orders_to_sync.push(watched_order);
                            }
                            highest_priority_orders_till_watched_order
                                .into_iter()
                                    .skip(1)
                                    .for_each(|order| orders_to_sync.push(order));

                            while let Err(_) = {
                                Self::sync_orders_from_federation_concurrent(
                                    root_secret.clone(),
                                    module_api.clone(),
                                    db.clone(),
                                    orders_to_sync.clone()
                                )
                                .await
                            } {}
                            order_to_watch = new_order_to_watch;
                        }
                    }
                }
            },
        );

        Ok(stop_tx)
    }
}

/// Same as the ChildID used from the order root secret to derive order owner
#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    Encodable,
    Decodable,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
)]
pub struct OrderId(pub u64);

impl OrderId {
    const ORDER_PATH: ChildId = ChildId(0);

    pub fn into_key_pair(&self, root_secret: DerivableSecret) -> KeyPair {
        root_secret
            .child_key(Self::ORDER_PATH)
            .child_key(ChildId(self.0))
            .to_secp_key(&Secp256k1::new())
    }
}

impl FromStr for OrderId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(u64::from_str(s)?))
    }
}

pub fn market_outpoint_from_tx_id(tx_id: TransactionId) -> OutPoint {
    OutPoint {
        txid: tx_id,
        out_idx: 0,
    }
}
