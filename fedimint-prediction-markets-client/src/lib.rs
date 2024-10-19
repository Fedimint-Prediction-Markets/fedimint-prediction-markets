use std::collections::{BTreeMap, HashMap};
use std::ffi;
use std::str::FromStr;
use std::sync::Arc;

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
    Committable, Database, DatabaseTransaction, DatabaseVersion, IDatabaseTransactionOpsCoreTyped,
};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::module::{
    ApiVersion, CommonModuleInit, ModuleCommon, ModuleInit, MultiApiVersion, TransactionItemAmount,
};
use fedimint_core::util::BoxStream;
use fedimint_core::{apply, async_trait_maybe_send, Amount, OutPoint, TransactionId};
use fedimint_prediction_markets_common::api::{
    GetEventPayoutAttestationsUsedToPermitPayoutParams, GetMarketDynamicParams,
    GetMarketOutcomeCandlesticksParams, GetMarketOutcomeCandlesticksResult, GetMarketParams,
    GetOrderParams, WaitMarketOutcomeCandlesticksParams, WaitMarketOutcomeCandlesticksResult,
};
use fedimint_prediction_markets_common::config::{GeneralConsensus, PredictionMarketsClientConfig};
use fedimint_prediction_markets_common::{
    Candlestick, ContractOfOutcomeAmount, EventJson, Market, NostrPublicKeyHex, Order, Outcome,
    PredictionMarketsCommonInit, PredictionMarketsInput, PredictionMarketsModuleTypes,
    PredictionMarketsOutput, Seconds, Side, UnixTimestamp, Weight, WeightRequiredForPayout,
};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use secp256k1::{KeyPair, PublicKey, Scalar, Secp256k1, SecretKey};
use serde::{Deserialize, Serialize};
use states::{PredictionMarketState, PredictionMarketsStateMachine};

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
    _notifier: ModuleNotifier<PredictionMarketsStateMachine>,
    ctx: ClientContext<Self>,
    db: Database,
    module_api: DynModuleApi,
}

/// Data needed by the state machine
#[derive(Debug, Clone)]
pub struct PredictionMarketsClientContext {
    pub prediction_markets_decoder: Decoder,
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
        Ok(PredictionMarketsClientModule {
            cfg: args.cfg().to_owned(),
            root_secret: args.module_root_secret().to_owned(),
            _notifier: args.notifier().to_owned(),
            ctx: args.context(),
            db: args.db().to_owned(),
            module_api: args.module_api().to_owned(),
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
        event_json: EventJson,
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
                    state: PredictionMarketState::NewMarket { tx_id },
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

        let tx_subscription = self.ctx.transaction_updates(operation_id).await;
        tx_subscription
            .await_tx_accepted(tx_id)
            .await
            .map_err(|e| anyhow!(e))?;

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
        event_payout_attestations_json: Vec<EventJson>,
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
                    state: PredictionMarketState::PayoutMarket { tx_id },
                }]
            }),
        };

        let tx = TransactionBuilder::new().with_output(self.ctx.make_client_output(output));
        let out_point = |txid, _| OutPoint { txid, out_idx: 0 };
        let (txid, _) = self
            .ctx
            .finalize_and_submit_transaction(
                operation_id,
                PredictionMarketsCommonInit::KIND.as_str(),
                out_point,
                tx,
            )
            .await?;

        let tx_subscription = self.ctx.transaction_updates(operation_id).await;
        tx_subscription
            .await_tx_accepted(txid)
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(())
    }

    pub async fn get_event_payout_attestations_used_to_permit_payout(
        &self,
        market: OutPoint,
    ) -> anyhow::Result<Option<Vec<EventJson>>> {
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
                            state: PredictionMarketState::NewOrder {
                                tx_id,
                                order: order_id,
                                sources: vec![],
                            },
                        }]
                    }),
                };

                tx = tx.with_output(self.ctx.make_client_output(output));
            }
            Side::Sell => {
                let mut sources = BTreeMap::new();
                let mut sources_for_state_machine = vec![];
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

                    sources.insert(
                        PublicKey::from_keypair(&order_key),
                        quantity_sourced_from_order,
                    );
                    sources_for_state_machine.push(order_id);
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
                            state: PredictionMarketState::NewOrder {
                                tx_id,
                                order: order_id,
                                sources: sources_for_state_machine.to_owned(),
                            },
                        }]
                    }),
                    keys: vec![sources_keys_combined.unwrap()],
                };

                tx = tx.with_input(self.ctx.make_client_input(input));
            }
        }

        PredictionMarketsClientModule::reserve_order_id_slot(&mut dbtx, order_id).await;
        dbtx.commit_tx_result().await?;

        let (txid, _) = self
            .ctx
            .finalize_and_submit_transaction(
                operation_id,
                PredictionMarketsCommonInit::KIND.as_str(),
                |_, _| (),
                tx,
            )
            .await?;

        let tx_subscription = self.ctx.transaction_updates(operation_id).await;
        tx_subscription
            .await_tx_accepted(txid)
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(order_id)
    }

    pub async fn get_order(
        &self,
        id: OrderId,
        from_local_cache: bool,
    ) -> anyhow::Result<Option<Order>> {
        let mut dbtx = self.db.begin_transaction().await;

        let order_owner = self.order_id_to_key_pair(id).public_key();

        match from_local_cache {
            true => Ok(dbtx
                .get_value(&db::OrderKey(id))
                .await
                .map(|v| match v {
                    OrderIdSlot::Reserved => None,
                    OrderIdSlot::Order(order) => Some(order),
                })
                .flatten()),

            false => {
                let result = self
                    .module_api
                    .get_order(GetOrderParams { order: order_owner })
                    .await?;

                if let Some(order) = result.order.as_ref() {
                    PredictionMarketsClientModule::save_order_to_db(&mut dbtx, id, order).await;

                    dbtx.commit_tx_result().await?;
                }

                Ok(result.order)
            }
        }
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
                    state: PredictionMarketState::CancelOrder { tx_id, order: id },
                }]
            }),
            keys: vec![order_key],
        };

        let tx = TransactionBuilder::new().with_input(self.ctx.make_client_input(input));
        let (txid, _) = self
            .ctx
            .finalize_and_submit_transaction(
                operation_id,
                PredictionMarketsCommonInit::KIND.as_str(),
                |_, _| (),
                tx,
            )
            .await?;

        let tx_subscription = self.ctx.transaction_updates(operation_id).await;
        tx_subscription
            .await_tx_accepted(txid)
            .await
            .map_err(|e| anyhow!(e))?;

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

            let input = ClientInput {
                input: PredictionMarketsInput::ConsumeOrderBitcoinBalance {
                    order: PublicKey::from_keypair(&order_key),
                    amount: order.bitcoin_balance,
                },
                state_machines: Arc::new(move |tx_id, _| {
                    vec![PredictionMarketsStateMachine {
                        operation_id,
                        state: PredictionMarketState::ConsumeOrderBitcoinBalance {
                            tx_id,
                            order: order_id,
                        },
                    }]
                }),
                keys: vec![order_key],
            };

            tx = tx.with_input(self.ctx.make_client_input(input));

            total_amount += order.bitcoin_balance;
        }

        let outpoint = |txid, _| OutPoint { txid, out_idx: 0 };
        let (txid, _) = self
            .ctx
            .finalize_and_submit_transaction(
                operation_id,
                PredictionMarketsCommonInit::KIND.as_str(),
                outpoint,
                tx,
            )
            .await?;

        let tx_subscription = self.ctx.transaction_updates(operation_id).await;
        tx_subscription
            .await_tx_accepted(txid)
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(total_amount)
    }

    /// Update all orders in db that could possibly be unsynced between
    /// federation and local order cache because of an order match or
    /// because of an operation the client has performed.
    ///
    /// Setting sync_possible_payouts to true also syncs orders that could have
    /// changed because of a market payout.
    ///
    /// Optionally provide a market (and outcome) to update only orders
    /// belonging to that market (and outcome). This option does not effect
    /// updating orders that have changed because of an operation the client has
    /// performed.
    ///
    /// Returns orders that recieved mutating update. Returned orders are
    /// filtered by market and outcome.
    pub async fn sync_orders(
        &self,
        sync_possible_payouts: bool,
        market: Option<OutPoint>,
        outcome: Option<Outcome>,
    ) -> anyhow::Result<BTreeMap<OrderId, Order>> {
        let mut dbtx = self.db.begin_transaction().await;

        let mut orders_to_update = HashMap::new();

        let non_zero_orders: Vec<_> = match market {
            None => {
                dbtx.find_by_prefix(&db::NonZeroOrdersByMarketOutcomePrefixAll)
                    .await
                    .map(|(key, _)| key.order)
                    .collect()
                    .await
            }
            Some(market) => match outcome {
                None => {
                    dbtx.find_by_prefix(&db::NonZeroOrdersByMarketOutcomePrefix1 { market })
                        .await
                        .map(|(key, _)| key.order)
                        .collect()
                        .await
                }
                Some(outcome) => {
                    dbtx.find_by_prefix(&db::NonZeroOrdersByMarketOutcomePrefix2 {
                        market,
                        outcome,
                    })
                    .await
                    .map(|(key, _)| key.order)
                    .collect()
                    .await
                }
            },
        };

        for order_id in non_zero_orders {
            let order = self.get_order(order_id, true).await.unwrap().unwrap();

            if order.quantity_waiting_for_match == ContractOfOutcomeAmount::ZERO
                && (!sync_possible_payouts
                    || order.contract_of_outcome_balance == ContractOfOutcomeAmount::ZERO)
            {
                continue;
            }

            orders_to_update.insert(order_id, ());
        }

        let mut stream = dbtx.find_by_prefix(&db::OrderNeedsUpdatePrefixAll).await;
        while let Some((key, _)) = stream.next().await {
            orders_to_update.insert(key.0, ());
        }

        let mut changed_orders = BTreeMap::new();
        let mut get_order_futures_unordered = orders_to_update
            .into_keys()
            .map(|id| async move {
                (
                    // id of order
                    id,
                    // order we have currently in cache
                    self.get_order(id, true).await,
                    // updated order
                    self.get_order(id, false).await,
                )
            })
            .collect::<FuturesUnordered<_>>();
        while let Some((id, from_cache, updated)) = get_order_futures_unordered.next().await {
            if let Err(e) = updated {
                bail!("Error getting order from federation: {:?}", e)
            }

            let updated = updated?;
            if from_cache? != updated {
                let order = updated.expect("should always be some");

                if let Some(market) = market {
                    if order.market != market {
                        continue;
                    }
                }

                if let Some(outcome) = outcome {
                    if order.outcome != outcome {
                        continue;
                    }
                }

                changed_orders.insert(id, order);
            }
        }

        Ok(changed_orders)
    }

    /// Get all orders in the db.
    /// Optionally provide a market (and outcome) to get only orders belonging
    /// to that market (and outcome)
    pub async fn get_orders_from_db(
        &self,
        market: Option<OutPoint>,
        outcome: Option<Outcome>,
    ) -> BTreeMap<OrderId, Order> {
        let mut dbtx = self.db.begin_transaction().await;

        let orders_by_market_outcome_result: Vec<_> = match market {
            None => {
                dbtx.find_by_prefix(&db::OrdersByMarketOutcomePrefixAll)
                    .await
                    .collect()
                    .await
            }
            Some(market) => match outcome {
                None => {
                    dbtx.find_by_prefix(&db::OrdersByMarketOutcomePrefix1 { market })
                        .await
                        .collect()
                        .await
                }
                Some(outcome) => {
                    dbtx.find_by_prefix(&db::OrdersByMarketOutcomePrefix2 { market, outcome })
                        .await
                        .collect()
                        .await
                }
            },
        };

        let mut orders = BTreeMap::new();
        for order_id in orders_by_market_outcome_result
            .iter()
            .map(|(key, _)| key.order)
        {
            let order = self
                .get_order(order_id, true)
                .await
                .expect("should never error")
                .expect("should always be some");
            orders.insert(order_id, order);
        }

        orders
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
        &'a self,
        market: OutPoint,
        outcome: Outcome,
        candlestick_interval: Seconds,
        min_candlestick_timestamp: UnixTimestamp,
    ) -> BoxStream<'a, anyhow::Result<Vec<(UnixTimestamp, Candlestick)>>> {
        let mut candlestick_timestamp = min_candlestick_timestamp;
        let mut candlestick_volume = ContractOfOutcomeAmount::ZERO;

        Box::pin(stream! {
            loop {
                let res = self
                    .wait_candlesticks(
                        market,
                        outcome,
                        candlestick_interval,
                        candlestick_timestamp,
                        candlestick_volume,
                    )
                    .await
                    .map(|c| c.into_iter().collect::<Vec<_>>());

                if let Ok(v) = &res {
                    if let Some(newest_candle) = v.last() {
                        candlestick_timestamp = newest_candle.0;
                        candlestick_volume = newest_candle.1.volume;
                    }
                }

                yield res;
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
    const ORDER_FROM_ROOT_SECRET: ChildId = ChildId(0);

    fn order_id_to_key_pair(&self, id: OrderId) -> KeyPair {
        self.root_secret
            .child_key(Self::ORDER_FROM_ROOT_SECRET)
            .child_key(ChildId(id.0))
            .to_secp_key(&Secp256k1::new())
    }

    async fn save_order_to_db(
        dbtx: &mut DatabaseTransaction<'_, Committable>,
        id: OrderId,
        order: &Order,
    ) {
        dbtx.insert_entry(&db::OrderKey(id), &OrderIdSlot::Order(order.to_owned()))
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

        dbtx.remove_entry(&db::OrderNeedsUpdateKey(id)).await;
    }

    async fn reserve_order_id_slot(dbtx: &mut DatabaseTransaction<'_, Committable>, id: OrderId) {
        dbtx.insert_entry(&db::OrderKey(id), &OrderIdSlot::Reserved)
            .await;
    }

    async fn unreserve_order_id_slot(mut dbtx: DatabaseTransaction<'_>, id: OrderId) {
        dbtx.remove_entry(&db::OrderKey(id)).await;
    }

    async fn set_order_needs_update(mut dbtx: DatabaseTransaction<'_>, ids: Vec<OrderId>) {
        for id in ids {
            dbtx.insert_entry(&db::OrderNeedsUpdateKey(id), &()).await;
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
        args: &[ffi::OsString],
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

impl FromStr for OrderId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(u64::from_str(s)?))
    }
}

pub fn market_outpoint_from_txid(txid: TransactionId) -> OutPoint {
    OutPoint { txid, out_idx: 0 }
}
