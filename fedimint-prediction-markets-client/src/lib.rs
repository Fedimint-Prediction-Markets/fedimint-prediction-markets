use std::collections::{BTreeMap, HashMap};
use std::ffi;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail};
use async_stream::stream;
use bitcoin::Denomination;
use db::OrderIdSlot;
use fedimint_client::derivable_secret::{ChildId, DerivableSecret};
use fedimint_client::module::init::{ClientModuleInit, ClientModuleInitArgs};
use fedimint_client::module::recovery::NoModuleBackup;
use fedimint_client::module::{ClientContext, ClientModule, IClientModule};
use fedimint_client::sm::{Context, ModuleNotifier};
use fedimint_client::transaction::{ClientInput, ClientOutput, TransactionBuilder};
use fedimint_client::DynGlobalClientContext;
use fedimint_core::api::DynModuleApi;
use fedimint_core::core::{Decoder, OperationId};
use fedimint_core::db::{
    Committable, Database, DatabaseTransaction, IDatabaseTransactionOpsCoreTyped,
};
use fedimint_core::module::{
    ApiVersion, CommonModuleInit, ModuleCommon, ModuleInit, MultiApiVersion, TransactionItemAmount,
};
use fedimint_core::util::BoxStream;
use fedimint_core::{apply, async_trait_maybe_send, Amount, OutPoint, TransactionId};
use fedimint_prediction_markets_common::api::{
    GetMarketOutcomeCandlesticksParams, GetMarketOutcomeCandlesticksResult,
    GetPayoutControlMarketsParams, WaitMarketOutcomeCandlesticksParams,
};
use fedimint_prediction_markets_common::config::PredictionMarketsClientConfig;
use fedimint_prediction_markets_common::{
    Candlestick, ContractOfOutcomeAmount, Market, MarketInformation, Order, OrderIdClientSide,
    Outcome, PredictionMarketsCommonInit, PredictionMarketsInput, PredictionMarketsModuleTypes,
    PredictionMarketsOutput, Seconds, Side, UnixTimestamp, Weight, WeightRequiredForPayout,
};
use futures::stream::FuturesUnordered;
use futures::{future, StreamExt};
use secp256k1::{KeyPair, PublicKey, Secp256k1};
use states::PredictionMarketsStateMachine;
use tokio::time::Instant;

use crate::api::PredictionMarketsFederationApi;

mod api;
mod db;
mod states;

#[derive(Debug)]
pub struct PredictionMarketsClientModule {
    cfg: PredictionMarketsClientConfig,
    root_secret: DerivableSecret,
    _notifier: ModuleNotifier<DynGlobalClientContext, PredictionMarketsStateMachine>,
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
        MultiApiVersion::try_from_iter([ApiVersion { major: 0, minor: 0 }])
            .expect("no version conflicts")
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
    /// Get payout control public key that client controls.
    pub fn get_client_payout_control(&self) -> PublicKey {
        let key = self.get_payout_control_key_pair();

        PublicKey::from_keypair(&key)
    }

    /// Create new market
    pub async fn new_market(
        &self,
        contract_price: Amount,
        outcomes: Outcome,
        payout_control_weights: BTreeMap<PublicKey, Weight>,
        weight_required_for_payout: WeightRequiredForPayout,
        payout_controls_fee_per_contract: Amount,
        information: MarketInformation,
    ) -> anyhow::Result<OutPoint> {
        let operation_id = OperationId::new_random();

        let output = ClientOutput {
            output: PredictionMarketsOutput::NewMarket {
                contract_price,
                outcomes,
                payout_control_weights,
                weight_required_for_payout,
                payout_controls_fee_per_contract,
                information,
            },
            state_machines: Arc::new(move |tx_id, _| {
                vec![PredictionMarketsStateMachine::NewMarket {
                    operation_id,
                    tx_id,
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

    /// Get market
    pub async fn get_market(
        &self,
        market_out_point: OutPoint,
        from_local_cache: bool,
    ) -> anyhow::Result<Option<Market>> {
        let mut dbtx = self.db.begin_transaction().await;

        match from_local_cache {
            true => Ok(dbtx
                .get_value(&db::MarketKey {
                    market: market_out_point,
                })
                .await),

            false => {
                // if in finished state in db, just return db version
                let market = dbtx
                    .get_value(&db::MarketKey {
                        market: market_out_point,
                    })
                    .await;
                if let Some(market) = market {
                    if market.payout.is_some() {
                        return Ok(Some(market));
                    }
                }

                let market_option = self.module_api.get_market(market_out_point).await?;
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

    /// Get client payout controls markets.
    pub async fn get_client_payout_control_markets(
        &self,
        from_local_cache: bool,
        markets_created_after_and_including: UnixTimestamp,
    ) -> anyhow::Result<BTreeMap<UnixTimestamp, Vec<OutPoint>>> {
        let payout_control = self.get_client_payout_control();
        let mut dbtx = self.db.begin_transaction().await;

        if !from_local_cache {
            let newest_market_in_db = dbtx
                .find_by_prefix_sorted_descending(&db::ClientPayoutControlMarketsPrefixAll)
                .await
                .next()
                .await
                .map(|(key, _)| key.market_created)
                .unwrap_or(UnixTimestamp::ZERO);

            let get_payout_control_markets_result = self
                .module_api
                .get_payout_control_markets(GetPayoutControlMarketsParams {
                    payout_control,
                    markets_created_after_and_including: newest_market_in_db,
                })
                .await?;

            for market_out_point in get_payout_control_markets_result.markets {
                let market = self
                    .get_market(market_out_point, false)
                    .await?
                    .expect("should always produce market");

                dbtx.insert_entry(
                    &db::ClientPayoutControlMarketsKey {
                        market_created: market.created_consensus_timestamp,
                        market: market_out_point,
                    },
                    &(),
                )
                .await;
            }
        }

        let mut payout_control_markets_stream = dbtx
            .find_by_prefix_sorted_descending(&db::ClientPayoutControlMarketsPrefixAll)
            .await
            .map(|(k, _)| (k.market_created, k.market))
            .take_while(|(market_created, _)| {
                future::ready(market_created >= &markets_created_after_and_including)
            });

        let mut payout_control_markets = BTreeMap::new();
        while let Some((market_created, market)) = payout_control_markets_stream.next().await {
            if !payout_control_markets.contains_key(&market_created) {
                payout_control_markets.insert(market_created, Vec::new());
            }

            let v = payout_control_markets.get_mut(&market_created).unwrap();
            v.push(market);
        }
        drop(payout_control_markets_stream);

        dbtx.commit_tx().await;

        Ok(payout_control_markets)
    }

    /// get market's payout control proposals.
    pub async fn get_market_payout_control_proposals(
        &self,
        market: OutPoint,
        from_local_cache: bool,
    ) -> anyhow::Result<BTreeMap<PublicKey, Vec<Amount>>> {
        let mut dbtx = self.db.begin_transaction().await;

        match from_local_cache {
            true => Ok(dbtx
                .find_by_prefix(&db::MarketPayoutControlProposalPrefix1 { market })
                .await
                .map(|(key, value)| (key.payout_control, value))
                .collect::<BTreeMap<_, _>>()
                .await),

            false => {
                let market_payout_control_proposals = self
                    .module_api
                    .get_market_payout_control_proposals(market)
                    .await?;

                dbtx.remove_by_prefix(&db::MarketPayoutControlProposalPrefix1 { market })
                    .await;
                for (payout_control, outcome_payout) in market_payout_control_proposals.iter() {
                    dbtx.insert_entry(
                        &db::MarketPayoutControlProposalKey {
                            market,
                            payout_control: payout_control.to_owned(),
                        },
                        outcome_payout,
                    )
                    .await;
                }
                dbtx.commit_tx().await;

                Ok(market_payout_control_proposals)
            }
        }
    }

    /// propose payout using client's payout control.
    pub async fn propose_payout(
        &self,
        market_out_point: OutPoint,
        outcome_payouts: Vec<Amount>,
    ) -> anyhow::Result<()> {
        let operation_id = OperationId::new_random();

        let payout_control_key = self.get_payout_control_key_pair();

        let input = ClientInput {
            input: PredictionMarketsInput::PayoutProposal {
                market: market_out_point,
                payout_control: PublicKey::from_keypair(&payout_control_key),
                outcome_payouts,
            },
            state_machines: Arc::new(move |tx_id, _| {
                vec![PredictionMarketsStateMachine::ProposePayout {
                    operation_id,
                    tx_id,
                }]
            }),
            keys: vec![payout_control_key],
        };

        let tx = TransactionBuilder::new().with_input(self.ctx.make_client_input(input));
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

    /// Create new order
    pub async fn new_order(
        &self,
        market: OutPoint,
        outcome: Outcome,
        side: Side,
        price: Amount,
        quantity: ContractOfOutcomeAmount,
    ) -> anyhow::Result<OrderIdClientSide> {
        let operation_id = OperationId::new_random();
        let mut dbtx = self.db.begin_transaction().await;

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
                        vec![PredictionMarketsStateMachine::NewOrder {
                            operation_id,
                            tx_id,
                            order: order_id,
                            sources: vec![],
                        }]
                    }),
                };

                tx = tx.with_output(self.ctx.make_client_output(output));
            }
            Side::Sell => {
                let mut sources_for_input = BTreeMap::new();
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

                    let order_key = self.order_id_to_key_pair(order_id);
                    let quantity_sourced_from_order = order
                        .contract_of_outcome_balance
                        .min(quantity - sourced_quantity);

                    sources_for_input.insert(
                        PublicKey::from_keypair(&order_key),
                        quantity_sourced_from_order,
                    );
                    sources_for_state_machine.push(order_id);
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

                tx = tx.with_input(self.ctx.make_client_input(input));
            }
        }

        PredictionMarketsClientModule::reserve_order_id_slot(&mut dbtx, order_id).await;
        dbtx.commit_tx().await;

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

        Ok(order_id)
    }

    /// Get order
    pub async fn get_order(
        &self,
        id: OrderIdClientSide,
        from_local_cache: bool,
    ) -> anyhow::Result<Option<Order>> {
        let mut dbtx = self.db.begin_transaction().await;

        let order_key = self.order_id_to_key_pair(id);
        let order_owner = PublicKey::from_keypair(&order_key);

        match from_local_cache {
            true => Ok(match dbtx.get_value(&db::OrderKey { id }).await {
                Some(d) => match d {
                    OrderIdSlot::Reserved => None,
                    OrderIdSlot::Order(order) => Some(order),
                },

                None => None,
            }),

            false => {
                let order_option = self.module_api.get_order(order_owner).await?;

                if let Some(order) = order_option.as_ref() {
                    PredictionMarketsClientModule::save_order_to_db(&mut dbtx, id, order).await;

                    dbtx.commit_tx().await;
                }

                Ok(order_option)
            }
        }
    }

    /// Cancel order
    pub async fn cancel_order(&self, id: OrderIdClientSide) -> anyhow::Result<()> {
        let operation_id = OperationId::new_random();

        let order_key = self.order_id_to_key_pair(id);

        let input = ClientInput {
            input: PredictionMarketsInput::CancelOrder {
                order: PublicKey::from_keypair(&order_key),
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

        let tx = TransactionBuilder::new().with_input(self.ctx.make_client_input(input));
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
            let order = self
                .get_order(order_id, true)
                .await?
                .expect("should always produce order");

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
                    vec![PredictionMarketsStateMachine::ConsumeOrderBitcoinBalance {
                        operation_id,
                        tx_id,
                        order: order_id,
                    }]
                }),
                keys: vec![order_key],
            };

            tx = tx.with_input(self.ctx.make_client_input(input));

            total_amount = total_amount + order.bitcoin_balance;
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
    ) -> anyhow::Result<BTreeMap<OrderIdClientSide, Order>> {
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
            let order = self
                .get_order(order_id, true)
                .await
                .expect("should never error because from local cache")
                .expect("should always produce order");

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
            orders_to_update.insert(key.order, ());
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
    ) -> BTreeMap<OrderIdClientSide, Order> {
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
    pub async fn resync_order_slots(&self, gap_size_to_check: u16) -> anyhow::Result<()> {
        let mut order_id = OrderIdClientSide(0);
        let mut slots_without_order = 0u16;
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

    /// create stream of candlesticks
    pub async fn stream_candlesticks(
        &self,
        market: OutPoint,
        outcome: Outcome,
        candlestick_interval: Seconds,
        min_candlestick_timestamp: UnixTimestamp,
        min_duration_between_requests_milliseconds: u64,
    ) -> BoxStream<'static, BTreeMap<UnixTimestamp, Candlestick>> {
        let module_api = self.module_api.to_owned();

        let mut current_candlestick_timestamp = min_candlestick_timestamp;
        let mut current_candlestick_volume = ContractOfOutcomeAmount::ZERO;

        Box::pin(stream! {
            loop {
                let start_api_request = Instant::now();
                let api_result = module_api.wait_market_outcome_candlesticks(WaitMarketOutcomeCandlesticksParams {
                    market,
                    outcome,
                    candlestick_interval,
                    candlestick_timestamp: current_candlestick_timestamp,
                    candlestick_volume: current_candlestick_volume,
                }).await;

                match api_result {
                    Ok(r) => {
                        let b = r.candlesticks.into_iter().collect::<BTreeMap<_, _>>();
                        if b.len() != 0 {
                            let (newest_candlestick_timestamp, newest_candlestick) = b.last_key_value().expect("should always be some");

                            current_candlestick_timestamp = newest_candlestick_timestamp.to_owned();
                            current_candlestick_volume = newest_candlestick.volume;

                            yield b;
                        }
                    }
                    Err(_) => {
                        // wait some time on error
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }

                tokio::time::sleep(
                    Duration::from_millis(min_duration_between_requests_milliseconds).saturating_sub(
                        Instant::now().duration_since(start_api_request)
                    )
                ).await;
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
    /// return map is saved timestamp to market.
    pub async fn get_saved_markets(&self) -> BTreeMap<UnixTimestamp, OutPoint> {
        let mut dbtx = self.db.begin_transaction().await;

        dbtx.find_by_prefix(&db::ClientSavedMarketsPrefixAll)
            .await
            .map(|(k, v)| (v, k.market))
            .collect()
            .await
    }

    /// Interacts with client named payout control public keys
    pub async fn assign_name_to_payout_control(&self, payout_control: PublicKey, name: String) {
        let mut dbtx = self.db.begin_transaction().await;

        dbtx.insert_entry(&db::ClientNamedPayoutControlsKey { payout_control }, &name)
            .await;
        dbtx.commit_tx().await;
    }

    /// Interacts with client named payout control public keys
    pub async fn unassign_name_from_payout_control(&self, payout_control: PublicKey) {
        let mut dbtx = self.db.begin_transaction().await;

        dbtx.remove_entry(&db::ClientNamedPayoutControlsKey { payout_control })
            .await;
        dbtx.commit_tx().await;
    }

    /// Interacts with client named payout control public keys
    pub async fn get_payout_control_name(&self, payout_control: PublicKey) -> Option<String> {
        let mut dbtx = self.db.begin_transaction().await;

        dbtx.get_value(&db::ClientNamedPayoutControlsKey { payout_control })
            .await
    }

    /// Interacts with client named payout control public keys
    pub async fn get_payout_control_name_map(&self) -> HashMap<PublicKey, String> {
        let mut dbtx = self.db.begin_transaction().await;

        dbtx.find_by_prefix(&db::ClientNamedPayoutControlsPrefixAll)
            .await
            .map(|(k, v)| (k.payout_control, v))
            .collect()
            .await
    }

    /// Spend all bitcoin balance of client payout control to primary module
    ///
    /// Returns how much bitcoin was sent
    pub async fn send_payout_control_bitcoin_balance_to_primary_module(
        &self,
    ) -> anyhow::Result<Amount> {
        let operation_id = OperationId::new_random();

        let payout_control_balance = self
            .module_api
            .get_payout_control_balance(self.get_client_payout_control())
            .await?;

        if payout_control_balance == Amount::ZERO {
            return Ok(payout_control_balance);
        }

        let mut tx = TransactionBuilder::new();
        let input = ClientInput {
            input: PredictionMarketsInput::ConsumePayoutControlBitcoinBalance {
                payout_control: self.get_client_payout_control(),
                amount: payout_control_balance,
            },
            state_machines: Arc::new(move |tx_id, _| {
                vec![
                    PredictionMarketsStateMachine::ConsumePayoutControlBitcoinBalance {
                        operation_id,
                        tx_id,
                    },
                ]
            }),
            keys: vec![self.get_payout_control_key_pair()],
        };
        tx = tx.with_input(self.ctx.make_client_input(input));

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

        Ok(payout_control_balance)
    }
}

/// private
impl PredictionMarketsClientModule {
    const MARKET_PAYOUT_CONTROL_FROM_ROOT_SECRET: ChildId = ChildId(0);
    const ORDER_FROM_ROOT_SECRET: ChildId = ChildId(1);

    fn get_payout_control_key_pair(&self) -> KeyPair {
        self.root_secret
            .child_key(Self::MARKET_PAYOUT_CONTROL_FROM_ROOT_SECRET)
            .to_secp_key(&Secp256k1::new())
    }

    fn order_id_to_key_pair(&self, id: OrderIdClientSide) -> KeyPair {
        self.root_secret
            .child_key(Self::ORDER_FROM_ROOT_SECRET)
            .child_key(ChildId(id.0))
            .to_secp_key(&Secp256k1::new())
    }

    async fn save_order_to_db(
        dbtx: &mut DatabaseTransaction<'_, Committable>,
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

    async fn reserve_order_id_slot(
        dbtx: &mut DatabaseTransaction<'_, Committable>,
        order: OrderIdClientSide,
    ) {
        dbtx.insert_entry(&db::OrderKey { id: order }, &OrderIdSlot::Reserved)
            .await;
    }

    async fn unreserve_order_id_slot(mut dbtx: DatabaseTransaction<'_>, order: OrderIdClientSide) {
        dbtx.remove_entry(&db::OrderKey { id: order }).await;
    }

    async fn set_order_needs_update(
        mut dbtx: DatabaseTransaction<'_>,
        orders: Vec<OrderIdClientSide>,
    ) {
        for order in orders {
            dbtx.insert_entry(&db::OrderNeedsUpdateKey { order }, &())
                .await;
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
            PredictionMarketsInput::PayoutProposal {
                market: _,
                payout_control: _,
                outcome_payouts: _,
            } => {
                amount = Amount::ZERO;
                fee = self.cfg.gc.payout_proposal_fee;
            }
            PredictionMarketsInput::CancelOrder { order: _ } => {
                amount = Amount::ZERO;
                fee = Amount::ZERO;
            }
            PredictionMarketsInput::ConsumeOrderBitcoinBalance {
                order: _,
                amount: amount_to_free,
            } => {
                amount = amount_to_free.to_owned();
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
            PredictionMarketsInput::ConsumePayoutControlBitcoinBalance {
                payout_control: _,
                amount: amount_to_free,
            } => {
                amount = amount_to_free.to_owned();
                fee = self.cfg.gc.consume_payout_control_bitcoin_balance_fee;
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
                contract_price: _,
                outcomes: _,
                payout_control_weights: _,
                weight_required_for_payout: _,
                payout_controls_fee_per_contract: _,
                information: _,
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
                amount = price.to_owned() * quantity.0;
                fee = self.cfg.gc.new_order_fee;
            }
        }

        Some(TransactionItemAmount { amount, fee })
    }

    async fn handle_cli_command(
        &self,
        args: &[ffi::OsString],
    ) -> anyhow::Result<serde_json::Value> {
        const SUPPORTED_COMMANDS: &str = "new-market, get-market, new-order, get-order, cancel-order, sync-orders, get-client-payout-control, get-candlesticks, recover-orders, withdraw-available-bitcoin, list-orders, propose-payout, get-market-payout-control-proposals, get-client-payout-control-markets";

        if args.is_empty() {
            bail!("Expected to be called with at least 1 argument: <command> …")
        }

        let command = args[0].to_string_lossy();

        match command.as_ref() {
            "get-client-payout-control" => {
                if args.len() != 1 {
                    bail!("`get-client-payout-control` expects 0 arguments")
                }

                Ok(serde_json::to_value(self.get_client_payout_control())?)
            }

            "new-market" => {
                if args.len() != 4 {
                    bail!("`new-market` command expects 3 arguments: <outcomes> <contract_price_msats> <payout_controls_fee_per_contract_msats>")
                }

                let outcomes: Outcome = args[1].to_string_lossy().parse()?;
                let contract_price =
                    Amount::from_str_in(&args[2].to_string_lossy(), Denomination::MilliSatoshi)?;
                let payout_controls_fee_per_contract =
                    Amount::from_str_in(&args[3].to_string_lossy(), Denomination::MilliSatoshi)?;

                let mut payout_control_weights = BTreeMap::new();
                payout_control_weights.insert(self.get_client_payout_control(), 1);

                let weight_required = 1;

                let market_out_point = self
                    .new_market(
                        contract_price,
                        outcomes,
                        payout_control_weights,
                        weight_required,
                        payout_controls_fee_per_contract,
                        MarketInformation {
                            title: "my market".to_owned(),
                            description: "this is my market".to_owned(),
                            outcome_titles: (0..outcomes)
                                .map(|i| {
                                    let mut title = String::new();

                                    title.push_str("Outcome ");
                                    title.push_str(&i.to_string());

                                    title
                                })
                                .collect(),
                            expected_payout_timestamp: UnixTimestamp::ZERO,
                        },
                    )
                    .await?;

                Ok(serde_json::to_value(market_out_point.txid)?)
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

                Ok(serde_json::to_value(
                    self.get_market(out_point, false).await?,
                )?)
            }

            "get-client-payout-control-markets" => {
                if args.len() != 1 {
                    bail!("`get-client-payout-control-markets` expects 0 arguments")
                }

                let payout_control_markets = self
                    .get_client_payout_control_markets(false, UnixTimestamp::ZERO)
                    .await?;

                Ok(serde_json::to_value(payout_control_markets)?)
            }

            "get-market-payout-control-proposals" => {
                if args.len() != 2 {
                    bail!("`get-market-payout-control-proposals` command expects 1 argument: <market_txid>")
                }

                let Ok(txid) = TransactionId::from_str(&args[1].to_string_lossy()) else {
                    bail!("Error getting transaction id");
                };

                let out_point = OutPoint { txid, out_idx: 0 };

                Ok(serde_json::to_value(
                    self.get_market_payout_control_proposals(out_point, false)
                        .await?,
                )?)
            }

            "propose-payout" => {
                if args.len() < 4 {
                    return Err(anyhow::format_err!(
                        "`propose-payout` command expects at least 3 arguments: <market_txid> <outcome_0_payout> <outcome_1_payout> ..."
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
                    self.propose_payout(market_out_point, outcome_payouts)
                        .await?,
                )?)
            }

            "new-order" => {
                if args.len() != 6 {
                    bail!("`new-order` command expects 5 arguments: <market_txid> <outcome> <side> <price_msats> <quantity>")
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
                    self.new_order(out_point, outcome, side, price, quantity)
                        .await?,
                )?)
            }

            "list-orders" => {
                if args.len() < 1 || args.len() > 3 {
                    bail!("`list-orders` command has 2 optional arguments: (market_txid) (outcome)")
                }

                let mut market: Option<OutPoint> = None;
                if let Some(arg_tx_id) = args.get(1) {
                    market = Some(OutPoint {
                        txid: TransactionId::from_str(&arg_tx_id.to_string_lossy())?,
                        out_idx: 0,
                    });
                };

                let mut outcome: Option<Outcome> = None;
                if let Some(arg_outcome) = args.get(2) {
                    outcome = Some(Outcome::from_str(&arg_outcome.to_string_lossy())?);
                }

                Ok(serde_json::to_value(
                    self.get_orders_from_db(market, outcome).await,
                )?)
            }

            "get-order" => {
                if args.len() != 2 {
                    bail!("`get-order` command expects 1 argument: <order_id>")
                }

                let id = OrderIdClientSide(args[1].to_string_lossy().parse()?);

                Ok(serde_json::to_value(self.get_order(id, false).await?)?)
            }

            "cancel-order" => {
                if args.len() != 2 {
                    bail!("`cancel-order` command expects 1 argument: <order_id>")
                }

                let id = OrderIdClientSide(args[1].to_string_lossy().parse()?);

                Ok(serde_json::to_value(self.cancel_order(id).await?)?)
            }

            "withdraw-available-bitcoin" => {
                if args.len() != 1 {
                    bail!("`withdraw-available-bitcoin` command expects 0 arguments")
                }

                let mut m = HashMap::new();
                m.insert(
                    "withdrawed_from_orders",
                    self.send_order_bitcoin_balance_to_primary_module().await?,
                );
                m.insert(
                    "withdrawed_from_payout_control",
                    self.send_payout_control_bitcoin_balance_to_primary_module()
                        .await?,
                );

                Ok(serde_json::to_value(m)?)
            }

            "sync-orders" => {
                if args.len() < 1 || args.len() > 3 {
                    bail!("`sync-order` command accepts 2 optional arguments: (market_txid) (outcome)")
                }

                let mut market: Option<OutPoint> = None;
                if let Some(arg_tx_id) = args.get(1) {
                    market = Some(OutPoint {
                        txid: TransactionId::from_str(&arg_tx_id.to_string_lossy())?,
                        out_idx: 0,
                    });
                };

                let mut outcome: Option<Outcome> = None;
                if let Some(arg_outcome) = args.get(2) {
                    outcome = Some(Outcome::from_str(&arg_outcome.to_string_lossy())?);
                }

                Ok(serde_json::to_value(
                    self.sync_orders(true, market, outcome).await?,
                )?)
            }

            "recover-orders" => {
                if args.len() != 1 && args.len() != 2 {
                    bail!(
                        "`recover-orders` command accepts 1 optional argument: (gap_size_checked)"
                    )
                }

                let mut gap_size_to_check = 20u16;
                if let Some(s) = args.get(1) {
                    gap_size_to_check = s.to_string_lossy().parse()?;
                }

                Ok(serde_json::to_value(
                    self.resync_order_slots(gap_size_to_check).await?,
                )?)
            }

            "get-candlesticks" => {
                if args.len() != 4 && args.len() != 5 {
                    bail!("`get-candlesticks` command expects 3 arguments and has 1 optional argument: <market_txid> <outcome> <candlestick_interval_seconds> (min_candlestick_timestamp)")
                }

                let Ok(txid) = TransactionId::from_str(&args[1].to_string_lossy()) else {
                    bail!("Error getting transaction id");
                };
                let market = OutPoint { txid, out_idx: 0 };

                let outcome: Outcome = args[2].to_string_lossy().parse()?;

                let candlestick_interval: Seconds = args[3].to_string_lossy().parse()?;

                let mut min_candlestick_timestamp = UnixTimestamp::ZERO;
                if let Some(s) = args.get(4) {
                    min_candlestick_timestamp = UnixTimestamp(s.to_string_lossy().parse()?)
                }

                let candlesticks = self
                    .get_candlesticks(
                        market,
                        outcome,
                        candlestick_interval,
                        min_candlestick_timestamp,
                    )
                    .await?
                    .into_iter()
                    .map(|(key, value)| (key.0.to_string(), value))
                    .collect::<BTreeMap<String, Candlestick>>();

                Ok(serde_json::to_value(candlesticks)?)
            }

            "help" => {
                let mut m = HashMap::new();
                m.insert("supported_commands", SUPPORTED_COMMANDS);

                Ok(serde_json::to_value(m)?)
            }

            command => {
                bail!("Unknown command: {command}, supported commands: {SUPPORTED_COMMANDS}")
            }
        }
    }

    fn supports_backup(&self) -> bool {
        false
    }
}
