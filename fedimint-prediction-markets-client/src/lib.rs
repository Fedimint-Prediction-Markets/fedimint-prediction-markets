use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::iter;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail};
use async_stream::stream;
use db::OrderIdSlot;
use fedimint_client::derivable_secret::{ChildId, DerivableSecret};
use fedimint_client::module::init::{ClientModuleInit, ClientModuleInitArgs};
use fedimint_client::module::recovery::NoModuleBackup;
use fedimint_client::module::{ClientContext, ClientModule, IClientModule};
use fedimint_client::sm::{Context, ModuleNotifier};
use fedimint_client::transaction::{ClientInput, ClientOutput, TransactionBuilder};
use fedimint_core::api::DynModuleApi;
use fedimint_core::core::{Decoder, OperationId};
use fedimint_core::db::{
    Database, DatabaseTransaction, DatabaseVersion, IDatabaseTransactionOpsCoreTyped,
};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::module::{
    ApiVersion, CommonModuleInit, ModuleCommon, ModuleInit, MultiApiVersion, TransactionItemAmount,
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
use secp256k1::{KeyPair, PublicKey, Scalar, Secp256k1, SecretKey};
use serde::{Deserialize, Serialize};
use states::{
    CancelOrderState, ConsumeOrderBitcoinBalanceState, NewMarketState, NewOrderState,
    PayoutMarketState, PredictionMarketState, PredictionMarketsStateMachine,
};
use tokio::select;
use tokio::sync::{broadcast, mpsc};

use crate::api::PredictionMarketsFederationApi;

mod api;
#[cfg(feature = "cli")]
mod cli;
mod db;
mod states;

#[derive(Debug)]
pub struct PredictionMarketsClientModule {
    cfg: PredictionMarketsClientConfig,
    root_secret: DerivableSecret,
    notifier: ModuleNotifier<PredictionMarketsStateMachine>,
    ctx: ClientContext<Self>,
    db: Database,
    module_api: DynModuleApi,

    pub new_order_broadcast: broadcast::Sender<OrderId>,
}

/// Data needed by the state machine
#[derive(Debug, Clone)]
pub struct PredictionMarketsClientContext {
    pub prediction_markets_decoder: Decoder,
    pub new_order_broadcast_sender: broadcast::Sender<OrderId>,
}

impl Context for PredictionMarketsClientContext {}

#[derive(Debug, Clone)]
pub struct PredictionMarketsClientInit;

#[apply(async_trait_maybe_send!)]
impl ModuleInit for PredictionMarketsClientInit {
    type Common = PredictionMarketsCommonInit;
    const DATABASE_VERSION: DatabaseVersion = DatabaseVersion(0);

    async fn dump_database(
        &self,
        _dbtx: &mut DatabaseTransaction<'_>,
        _prefix_names: Vec<String>,
    ) -> Box<dyn Iterator<Item = (String, Box<dyn erased_serde::Serialize + Send>)> + '_> {
        unimplemented!();
    }
}

/// Generates the client module
#[apply(async_trait_maybe_send!)]
impl ClientModuleInit for PredictionMarketsClientInit {
    type Module = PredictionMarketsClientModule;

    fn supported_api_versions(&self) -> MultiApiVersion {
        MultiApiVersion::try_from_iter([ApiVersion::new(0, 0)]).expect("no version conflicts")
    }

    async fn init(&self, args: &ClientModuleInitArgs<Self>) -> anyhow::Result<Self::Module> {
        let (new_order_broadcast, _) = broadcast::channel(100);

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
                    dbtx.commit_tx().await;

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
                    dbtx.commit_tx().await;
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
        let mut dbtx = self.db.begin_transaction().await;

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

        let order_key = self.order_id_to_key_pair(order_id);
        let owner = PublicKey::from_keypair(&order_key);

        let mut orders_to_sync_on_accepted = BTreeMap::new();
        orders_to_sync_on_accepted.insert(order_id, owner);

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
                        vec![PredictionMarketsStateMachine {
                            operation_id,
                            state: NewOrderState::Pending {
                                tx_id,
                                order_id,
                                orders_to_sync_on_accepted: orders_to_sync_on_accepted.clone(),
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

                let non_zero_market_outcome_orders: Vec<_> = dbtx
                    .find_by_prefix(&db::NonZeroOrdersByMarketOutcomePrefix2 { market, outcome })
                    .await
                    .map(|(key, _)| key.order)
                    .collect()
                    .await;

                let mut sourced_quantity = ContractOfOutcomeAmount::ZERO;
                for (n, order_id) in non_zero_market_outcome_orders.into_iter().enumerate() {
                    if n == usize::from(self.cfg.gc.max_sell_order_sources) {
                        bail!("max number of sell order sources reached. try again with a quantity less than or equal to {}", sourced_quantity.0)
                    }

                    let order = self.get_order(order_id, true).await.unwrap().unwrap();

                    if order.contract_of_outcome_balance == ContractOfOutcomeAmount::ZERO {
                        continue;
                    }

                    let order_key = self.order_id_to_key_pair(order_id);
                    let quantity_sourced_from_order = order
                        .contract_of_outcome_balance
                        .min(quantity - sourced_quantity);

                    sources.insert(order_key.public_key(), quantity_sourced_from_order);
                    orders_to_sync_on_accepted.insert(order_id, order_key.public_key());

                    sources_keys_combined = match sources_keys_combined {
                        None => Some(order_key),
                        Some(combined_keys) => {
                            let p1 = SecretKey::from_keypair(&combined_keys);
                            let p2 = SecretKey::from_keypair(&order_key);
                            let p3 = p1.add_tweak(&Scalar::from(p2))?;

                            Some(KeyPair::from_secret_key(secp256k1::SECP256K1, &p3))
                        }
                    };

                    sourced_quantity += quantity_sourced_from_order;
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
                    state_machines: Arc::new(move |tx_id, _| {
                        vec![PredictionMarketsStateMachine {
                            operation_id,
                            state: NewOrderState::Pending {
                                tx_id,
                                order_id,
                                orders_to_sync_on_accepted: orders_to_sync_on_accepted.clone(),
                            }
                            .into(),
                        }]
                    }),
                    keys: vec![sources_keys_combined.unwrap()],
                };

                tx = tx.with_input(self.ctx.make_client_input(input));
            }
        }

        dbtx.insert_entry(&db::OrderKey(order_id), &OrderIdSlot::Reserved)
            .await;
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

    pub async fn stream_order_from_db<'a>(&self, id: OrderId) -> BoxStream<'a, Option<Order>> {
        let db = self.db.clone();

        Box::pin(stream! {
            let mut previous_value = None;
            loop {
                let value = db
                    .wait_key_check(&db::OrderKey(id), |db_value| Some(db_value))
                    .await
                    .0
                    .map(|s| s.to_order())
                    .flatten();

                match previous_value {
                    None => yield value.clone(),
                    Some(ref inner) => {
                        if &value != inner {
                            yield value.clone();
                        }
                    }
                }

                previous_value = Some(value);
            }
        })
    }

    pub async fn cancel_order(&self, id: OrderId) -> anyhow::Result<()> {
        let operation_id = OperationId::new_random();

        let order_key = self.order_id_to_key_pair(id);
        let order_owner = order_key.public_key();

        let input = ClientInput {
            input: PredictionMarketsInput::CancelOrder { order: order_owner },
            state_machines: Arc::new(move |tx_id, _| {
                vec![PredictionMarketsStateMachine {
                    operation_id,
                    state: CancelOrderState::Pending {
                        tx_id,
                        order_to_sync_on_accepted: (id, order_owner),
                    }
                    .into(),
                }]
            }),
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

        let non_zero_orders = dbtx
            .find_by_prefix(&db::NonZeroOrdersByMarketOutcomePrefixAll)
            .await
            .map(|(key, _)| key.order)
            .collect::<Vec<_>>()
            .await;

        let mut orders_with_non_zero_bitcoin_balance = vec![];
        for order_id in non_zero_orders {
            let order = self.get_order(order_id, true).await?.unwrap();

            if order.bitcoin_balance != Amount::ZERO {
                orders_with_non_zero_bitcoin_balance.push((order_id, order));
            }
        }

        if orders_with_non_zero_bitcoin_balance.len() == 0 {
            return Ok(Amount::ZERO);
        }

        let mut total_amount = Amount::ZERO;
        let mut tx = TransactionBuilder::new();
        for (order_id, order) in orders_with_non_zero_bitcoin_balance {
            let order_key = self.order_id_to_key_pair(order_id);
            let order_owner = order_key.public_key();

            let input = ClientInput {
                input: PredictionMarketsInput::ConsumeOrderBitcoinBalance {
                    order: PublicKey::from_keypair(&order_key),
                    amount: order.bitcoin_balance,
                },
                state_machines: Arc::new(move |tx_id, _| {
                    vec![PredictionMarketsStateMachine {
                        operation_id,
                        state: ConsumeOrderBitcoinBalanceState::Pending {
                            tx_id,
                            order_to_sync_on_accepted: (order_id, order_owner),
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
                dbtx.find_by_prefix(&db::NonZeroOrdersByMarketOutcomePrefixAll)
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

            let mut market_orders: BTreeSet<_> = dbtx
                .find_by_prefix(&db::NonZeroOrdersByMarketOutcomePrefix1 { market })
                .await
                .map(|(k, _)| k.order)
                .collect()
                .await;

            orders_to_update.append(&mut market_orders);
        }

        self.sync_orders_from_federation_concurrent_with_self(
            orders_to_update.into_iter().collect(),
        )
        .await?;

        Ok(())
    }

    pub async fn sync_possible_order_matches(&self, filter: OrderFilter) -> anyhow::Result<()> {
        let non_zero_orders: BTreeSet<_> = self.get_non_zero_order_ids(filter).await;
        let mut orders_to_sync = Vec::new();
        for id in non_zero_orders {
            let order = self.get_order(id, true).await?.unwrap();
            if order.quantity_waiting_for_match != ContractOfOutcomeAmount::ZERO {
                orders_to_sync.push(id);
            }
        }

        self.sync_orders_from_federation_concurrent_with_self(orders_to_sync)
            .await?;

        Ok(())
    }

    pub async fn watch_orders_on_market_outcome_side(
        &self,
        market: OutPoint,
        outcome: Outcome,
        side: Side,
    ) -> anyhow::Result<mpsc::Sender<()>> {
        let module_api = self.module_api.clone();
        let db = self.db.clone();
        let root_secret = self.root_secret.clone();
        let mut new_order_reciever = self.new_order_broadcast.subscribe();
        let (stop_tx, mut stop_rx) = mpsc::channel(1);

        let active_quantity_orders = db
            .begin_transaction_nc()
            .await
            .find_by_prefix(&db::OrderPriceTimePriorityPrefix3 {
                market,
                outcome,
                side,
            })
            .await
            .map(|(_, order_id)| order_id)
            .collect::<Vec<_>>()
            .await;
        self.sync_orders_from_federation_concurrent_with_self(active_quantity_orders)
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

                        if let Some(order_id) = order_to_watch {
                            let order = dbtx
                                .get_value(&db::OrderKey(order_id))
                                .await
                                .unwrap()
                                .to_order()
                                .unwrap();
                            module_api.wait_order_match(WaitOrderMatchParams {
                                order: order_id.into_key_pair(root_secret.clone()).public_key(),
                                current_quantity_waiting_for_match: order
                                    .quantity_waiting_for_match,
                            })
                        } else {
                            Box::pin(futures::future::pending())
                        }
                    };

                    let new_order_signal = {
                        async {
                            loop {
                                let Ok(_) = new_order_reciever.recv().await else {
                                    continue;
                                };

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
                                order_to_watch = highest_priority_orders_till_watched_order
                                    .get(0)
                                    .map(|id| *id);
                                let mut orders_to_sync = vec![];
                                if let Some(watched_order) = order_to_watch {
                                    orders_to_sync.push(watched_order);
                                }
                                highest_priority_orders_till_watched_order
                                    .into_iter()
                                    .skip(1)
                                    .for_each(|order| orders_to_sync.push(order));

                                return orders_to_sync;
                            }
                        }
                    };

                    select! {
                        _ = stop_rx.recv() => return,
                        res = wait_order_match_request => {
                            if let Ok(WaitOrderMatchResult {order}) = res {
                                while let Err(_) = {
                                    let mut dbtx = db.begin_transaction().await;
                                    Self::save_order_to_db(&mut dbtx.to_ref_nc(), order_to_watch.unwrap(), &order).await;
                                    dbtx.commit_tx_result().await
                                } {}
                            }
                        }
                        orders_to_sync = new_order_signal => {
                            while let Err(_) = {
                                Self::sync_orders_from_federation_concurrent(
                                    root_secret.clone(),
                                    module_api.clone(),
                                    db.clone(),
                                    orders_to_sync.clone()
                                )
                                .await
                            } {}
                        }
                    }
                }
            },
        );

        Ok(stop_tx)
    }

    /// Get all orders in the db.
    /// Optionally provide a market (and outcome) to get only orders belonging
    /// to that market (and outcome)
    pub async fn get_order_ids(&self, filter: OrderFilter) -> BTreeSet<OrderId> {
        let mut dbtx = self.db.begin_transaction().await;

        match filter {
            OrderFilter::All => {
                dbtx.find_by_prefix(&db::OrdersByMarketOutcomePrefixAll)
                    .await
            }
            OrderFilter::Market(market) => {
                dbtx.find_by_prefix(&db::OrdersByMarketOutcomePrefix1 { market })
                    .await
            }
            OrderFilter::MarketOutcome(market, outcome) => {
                dbtx.find_by_prefix(&db::OrdersByMarketOutcomePrefix2 { market, outcome })
                    .await
            }
            OrderFilter::MarketOutcomeSide(market, outcome, side) => {
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

    pub async fn get_non_zero_order_ids(&self, filter: OrderFilter) -> BTreeSet<OrderId> {
        let mut dbtx = self.db.begin_transaction().await;

        match filter {
            OrderFilter::All => {
                dbtx.find_by_prefix(&db::NonZeroOrdersByMarketOutcomePrefixAll)
                    .await
            }
            OrderFilter::Market(market) => {
                dbtx.find_by_prefix(&db::NonZeroOrdersByMarketOutcomePrefix1 { market })
                    .await
            }
            OrderFilter::MarketOutcome(market, outcome) => {
                dbtx.find_by_prefix(&db::NonZeroOrdersByMarketOutcomePrefix2 { market, outcome })
                    .await
            }
            OrderFilter::MarketOutcomeSide(market, outcome, side) => {
                dbtx.find_by_prefix(&db::NonZeroOrdersByMarketOutcomePrefix3 {
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

        let mut candlestick_timestamp = min_candlestick_timestamp;
        let mut candlestick_volume = ContractOfOutcomeAmount::ZERO;

        Box::pin(stream! {
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

        if order.quantity_waiting_for_match != ContractOfOutcomeAmount::ZERO
            || order.contract_of_outcome_balance != ContractOfOutcomeAmount::ZERO
            || order.bitcoin_balance != Amount::ZERO
        {
            dbtx.insert_entry(
                &db::NonZeroOrdersByMarketOutcomeKey {
                    market: order.market,
                    outcome: order.outcome,
                    side: order.side,
                    order: id,
                },
                &(),
            )
            .await;
        } else {
            dbtx.remove_entry(&db::NonZeroOrdersByMarketOutcomeKey {
                market: order.market,
                outcome: order.outcome,
                side: order.side,
                order: id,
            })
            .await;
        }

        if order.quantity_waiting_for_match != ContractOfOutcomeAmount::ZERO {
            dbtx.insert_entry(&db::OrderPriceTimePriorityKey::from_order(order), &id)
                .await;
        } else {
            dbtx.remove_entry(&db::OrderPriceTimePriorityKey::from_order(order))
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
            new_order_broadcast_sender: self.new_order_broadcast.clone(),
        }
    }

    fn input_amount(
        &self,
        input: &<Self::Common as ModuleCommon>::Input,
    ) -> Option<TransactionItemAmount> {
        let amount;
        let fee;

        match input {
            PredictionMarketsInput::CancelOrder { order: _ } => {
                amount = Amount::ZERO;
                fee = Amount::ZERO;
            }
            PredictionMarketsInput::ConsumeOrderBitcoinBalance {
                order: _,
                amount: amount_to_free,
            } => {
                amount = *amount_to_free;
                fee = self.cfg.gc.consume_order_bitcoin_balance_fee;
            }
            PredictionMarketsInput::NewSellOrder {
                owner: _,
                market: _,
                outcome: _,
                price: _,
                sources: _,
            } => {
                amount = Amount::ZERO;
                fee = self.cfg.gc.new_order_fee;
            }
        }

        Some(TransactionItemAmount { amount, fee })
    }

    fn output_amount(
        &self,
        output: &<Self::Common as ModuleCommon>::Output,
    ) -> Option<TransactionItemAmount> {
        let amount;
        let fee;

        match output {
            PredictionMarketsOutput::NewMarket {
                event_json: _,
                contract_price: _,
                payout_control_weight_map: _,
                weight_required_for_payout: _,
            } => {
                amount = Amount::ZERO;
                fee = self.cfg.gc.new_market_fee;
            }
            PredictionMarketsOutput::NewBuyOrder {
                owner: _,
                market: _,
                outcome: _,
                price,
                quantity,
            } => {
                amount = *price * quantity.0;
                fee = self.cfg.gc.new_order_fee;
            }
            PredictionMarketsOutput::PayoutMarket {
                market: _,
                event_payout_attestations_json: _,
            } => {
                amount = Amount::ZERO;
                fee = Amount::ZERO;
            }
        }

        Some(TransactionItemAmount { amount, fee })
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

pub enum OrderFilter {
    All,
    Market(OutPoint),
    MarketOutcome(OutPoint, Outcome),
    MarketOutcomeSide(OutPoint, Outcome, Side),
}
