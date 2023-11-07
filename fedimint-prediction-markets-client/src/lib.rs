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
    Candlestick, ContractOfOutcomeAmount, GetMarketOutcomeCandlesticksParams,
    GetMarketOutcomeCandlesticksResult, GetOutcomeControlMarketsParams, Market, MarketInformation,
    Order, OrderIdClientSide, Outcome, Seconds, Side, UnixTimestamp, Weight, WeightRequired,
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
    /// Get outcome control public key that client controls.
    fn get_outcome_control_public_key(&self) -> XOnlyPublicKey;

    /// Create new market
    async fn new_market(
        &self,
        contract_price: Amount,
        outcomes: Outcome,
        outcome_control_weights: BTreeMap<XOnlyPublicKey, Weight>,
        weight_required: WeightRequired,
        information: MarketInformation,
    ) -> anyhow::Result<OutPoint>;

    /// Get Market
    async fn get_market(
        &self,
        market: OutPoint,
        from_local_cache: bool,
    ) -> anyhow::Result<Option<Market>>;

    /// Get all market [OutPoint]s that our outcome control key has some sort of authority over.
    ///
    /// Returns (market creation time) to (market outpoint)
    async fn get_outcome_control_markets(
        &self,
        sync_new_markets: bool,
        markets_created_after_and_including: UnixTimestamp,
    ) -> anyhow::Result<BTreeMap<UnixTimestamp, OutPoint>>;

    /// Get market outcome control proposals
    /// outcome control to proposed payout
    async fn get_market_outcome_control_proposals(
        &self,
        market: OutPoint,
        from_local_cache: bool,
    ) -> anyhow::Result<BTreeMap<XOnlyPublicKey, Vec<Amount>>>;

    /// Propose outcome payout
    async fn propose_outcome_payout(
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

    /// Spend all bitcoin balance on orders to primary module
    async fn send_order_bitcoin_balance_to_primary_module(&self) -> anyhow::Result<()>;

    /// Get all orders that could possibly be unsynced between federation and local order cache because
    /// of an order match.
    ///
    /// Optionally provide a market (and outcome) to update only orders belonging to that market (and outcome)
    ///
    /// Setting sync_possible_payouts to true syncs order that could have changed because of a market payout
    /// as well as an order match.
    async fn sync_orders(
        &self,
        market: Option<OutPoint>,
        outcome: Option<Outcome>,
        sync_possible_payouts: bool,
    ) -> anyhow::Result<()>;

    /// Get all orders in the db.
    /// Optionally provide a market (and outcome) to get only orders belonging to that market (and outcome)
    async fn get_orders_from_db(
        &self,
        market: Option<OutPoint>,
        outcome: Option<Outcome>,
    ) -> BTreeMap<OrderIdClientSide, Order>;

    /// Used to recover orders in case of recovery from seed
    async fn recover_orders(&self) -> anyhow::Result<()>;

    /// Candlesticks
    async fn get_candlesticks(
        &self,
        market: OutPoint,
        outcome: Outcome,
        candlestick_interval: Seconds,
        candlestick_count: u32,
    ) -> anyhow::Result<BTreeMap<UnixTimestamp, Candlestick>>;
}

#[apply(async_trait_maybe_send!)]
impl OddsMarketsClientExt for Client {
    fn get_outcome_control_public_key(&self) -> XOnlyPublicKey {
        let (prediction_markets, _) = self.get_first_module::<PredictionMarketsClientModule>(&KIND);

        let key = prediction_markets.get_outcome_control_key_pair();

        XOnlyPublicKey::from_keypair(&key).0
    }

    async fn new_market(
        &self,
        contract_price: Amount,
        outcomes: Outcome,
        outcome_control_weights: BTreeMap<XOnlyPublicKey, Weight>,
        weight_required: WeightRequired,
        information: MarketInformation,
    ) -> anyhow::Result<OutPoint> {
        let (_, instance) = self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let operation_id = OperationId::new_random();

        let output = ClientOutput {
            output: PredictionMarketsOutput::NewMarket {
                contract_price,
                outcomes,
                outcome_control_weights,
                weight_required,
                information,
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
                // if in finished state in db, just return db version
                {
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
                }

                let market_option = instance.api.get_market(market_out_point).await?;

                if let Some(market) = market_option.as_ref() {
                    // if this is the first time we have recieved this market with a payout, refresh
                    // market outcome control proposals so that Self::get_market_outcome_control_proposals
                    // can always use local db if market in local db has payout.
                    if market.payout.is_some() {
                        _ = self
                            .get_market_outcome_control_proposals(market_out_point, false)
                            .await?;
                    }

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

    async fn get_outcome_control_markets(
        &self,
        sync_new_markets: bool,
        markets_created_after_and_including: UnixTimestamp,
    ) -> anyhow::Result<BTreeMap<UnixTimestamp, OutPoint>> {
        let (prediction_markets, instance) =
            self.get_first_module::<PredictionMarketsClientModule>(&KIND);

        let mut dbtx = instance.db.begin_transaction().await;

        if sync_new_markets {
            let outcome_control =
                XOnlyPublicKey::from_keypair(&prediction_markets.get_outcome_control_key_pair()).0;
            let markets_created_after_and_including = {
                let mut stream = dbtx
                    .find_by_prefix_sorted_descending(&db::OutcomeControlMarketsPrefixAll)
                    .await;

                stream
                    .next()
                    .await
                    .map(|(key, _)| key.market_created)
                    .unwrap_or(UnixTimestamp::ZERO)
            };
            let get_outcome_control_markets_result = instance
                .api
                .get_outcome_control_markets(GetOutcomeControlMarketsParams {
                    outcome_control,
                    markets_created_after_and_including,
                })
                .await?;

            for market_out_point in get_outcome_control_markets_result.markets {
                let market = self
                    .get_market(market_out_point, false)
                    .await?
                    .expect("should always produce market");

                dbtx.insert_entry(
                    &db::OutcomeControlMarketsKey {
                        market_created: market.created_consensus_timestamp,
                        market: market_out_point,
                    },
                    &(),
                )
                .await;
            }
        }

        let mut market_stream_newest_first = dbtx
            .find_by_prefix_sorted_descending(&db::OutcomeControlMarketsPrefixAll)
            .await;
        let mut outcome_control_markets = BTreeMap::new();
        loop {
            let Some((key, _)) = market_stream_newest_first.next().await else {
                break;
            };
            if key.market_created < markets_created_after_and_including {
                break;
            }

            outcome_control_markets.insert(key.market_created, key.market);
        }

        drop(market_stream_newest_first);
        dbtx.commit_tx().await;

        Ok(outcome_control_markets)
    }

    async fn get_market_outcome_control_proposals(
        &self,
        market: OutPoint,
        from_local_cache: bool,
    ) -> anyhow::Result<BTreeMap<XOnlyPublicKey, Vec<Amount>>> {
        let (_, instance) = self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let mut dbtx = instance.db.begin_transaction().await;

        match from_local_cache {
            true => Ok(dbtx
                .find_by_prefix(&db::MarketOutcomeControlProposalPrefix1 { market })
                .await
                .map(|(key, value)| (key.outcome_control, value))
                .collect::<BTreeMap<_, _>>()
                .await),

            false => {
                // if market has payout in local db, Self::get_market has called Self::get_market_outcome_control_proposals to
                // get final version of market outcome control proposals.
                if let Some(market_db) = self.get_market(market, true).await? {
                    if market_db.payout.is_some() {
                        return self
                            .get_market_outcome_control_proposals(market, true)
                            .await;
                    }
                }

                let market_outcome_control_proposals = instance
                    .api
                    .get_market_outcome_control_proposals(market)
                    .await?;

                dbtx.remove_by_prefix(&db::MarketOutcomeControlProposalPrefix1 { market })
                    .await;
                for (outcome_control, outcome_payout) in market_outcome_control_proposals.iter() {
                    dbtx.insert_entry(
                        &db::MarketOutcomeControlProposalKey {
                            market,
                            outcome_control: outcome_control.to_owned(),
                        },
                        outcome_payout,
                    )
                    .await;
                }
                dbtx.commit_tx().await;

                Ok(market_outcome_control_proposals)
            }
        }
    }

    async fn propose_outcome_payout(
        &self,
        market_out_point: OutPoint,
        outcome_payouts: Vec<Amount>,
    ) -> anyhow::Result<()> {
        let (prediction_markets, instance) =
            self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let operation_id = OperationId::new_random();

        let outcome_control_key = prediction_markets.get_outcome_control_key_pair();

        let input = ClientInput {
            input: PredictionMarketsInput::PayoutProposal {
                market: market_out_point,
                outcome_control: XOnlyPublicKey::from_keypair(&outcome_control_key).0,
                outcome_payouts,
            },
            state_machines: Arc::new(move |tx_id, _| {
                vec![PredictionMarketsStateMachine::ProposePayout {
                    operation_id,
                    tx_id,
                }]
            }),
            keys: vec![outcome_control_key],
        };

        let tx = TransactionBuilder::new().with_input(input.into_dyn(instance.id));
        let out_point = |txid, _| OutPoint { txid, out_idx: 0 };
        let txid = self
            .finalize_and_submit_transaction(
                operation_id,
                PredictionMarketsCommonGen::KIND.as_str(),
                out_point,
                tx,
            )
            .await?;

        let tx_subscription = self.transaction_updates(operation_id).await;
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

                    let order_key = prediction_markets.order_id_to_key_pair(order_id);
                    let quantity_sourced_from_order = order
                        .contract_of_outcome_balance
                        .min(quantity - sourced_quantity);

                    sources_for_input.insert(
                        XOnlyPublicKey::from_keypair(&order_key).0,
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

    async fn send_order_bitcoin_balance_to_primary_module(&self) -> anyhow::Result<()> {
        let (prediction_markets, instance) =
            self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let operation_id = OperationId::new_random();

        let mut dbtx = instance.db.begin_transaction().await;

        let non_zero_orders = dbtx
            .find_by_prefix(&db::NonZeroOrdersByMarketOutcomePrefixAll)
            .await
            .map(|(key, _)| key.order)
            .collect::<Vec<_>>()
            .await;

        let mut orders_with_non_zero_bitcoin_balance = BTreeMap::new();
        for order_id in non_zero_orders {
            let order = self
                .get_order(order_id, true)
                .await?
                .expect("should always produce order");

            if order.bitcoin_balance != Amount::ZERO {
                orders_with_non_zero_bitcoin_balance.insert(order_id, order);
            }
        }

        let mut tx = TransactionBuilder::new();
        for (order_id, order) in orders_with_non_zero_bitcoin_balance {
            let order_key = prediction_markets.order_id_to_key_pair(order_id);

            let input = ClientInput {
                input: PredictionMarketsInput::ConsumeOrderBitcoinBalance {
                    order: XOnlyPublicKey::from_keypair(&order_key).0,
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

            tx = tx.with_input(input.into_dyn(instance.id));
        }

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

    async fn sync_orders(
        &self,
        market: Option<OutPoint>,
        outcome: Option<Outcome>,
        sync_possible_payouts: bool,
    ) -> anyhow::Result<()> {
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

        let mut possibly_changed_orders = vec![];
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

            possibly_changed_orders.push(order_id);
        }
        let get_order_futures = possibly_changed_orders
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

    async fn get_orders_from_db(
        &self,
        market: Option<OutPoint>,
        outcome: Option<Outcome>,
    ) -> BTreeMap<OrderIdClientSide, Order> {
        let (_, instance) = self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let mut dbtx = instance.db.begin_transaction().await;

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

    async fn recover_orders(&self) -> anyhow::Result<()> {
        const GAP_SIZE_CHECKED: u8 = 25;

        let mut order_id = OrderIdClientSide(0);
        let mut slots_without_order = 0u8;
        loop {
            if let Some(_) = self.get_order(order_id, false).await? {
                slots_without_order = 0;
            } else {
                slots_without_order += 1;
                if slots_without_order == GAP_SIZE_CHECKED {
                    break;
                }
            }

            order_id.0 += 1;
        }

        Ok(())
    }

    async fn get_candlesticks(
        &self,
        market: OutPoint,
        outcome: Outcome,
        candlestick_interval: Seconds,
        candlestick_count: u32,
    ) -> anyhow::Result<BTreeMap<UnixTimestamp, Candlestick>> {
        let (_, instance) = self.get_first_module::<PredictionMarketsClientModule>(&KIND);

        let GetMarketOutcomeCandlesticksResult { candlesticks } = instance
            .api
            .get_market_outcome_candlesticks(GetMarketOutcomeCandlesticksParams {
                market,
                outcome,
                candlestick_interval,
                candlestick_count,
            })
            .await?;

        let candlesticks = candlesticks.into_iter().collect::<BTreeMap<_, _>>();

        Ok(candlesticks)
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

    fn get_outcome_control_key_pair(&self) -> KeyPair {
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

    async fn consume_order_bitcoin_balance_accepted(
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
        let amount;
        let fee;

        match input {
            PredictionMarketsInput::PayoutProposal {
                market: _,
                outcome_control: _,
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
                fee = self
                    .cfg
                    .gc
                    .consumer_order_bitcoin_balance_fee;
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

        TransactionItemAmount { amount, fee }
    }

    fn output_amount(
        &self,
        output: &<Self::Common as ModuleCommon>::Output,
    ) -> TransactionItemAmount {
        let amount;
        let fee;

        match output {
            PredictionMarketsOutput::NewMarket {
                contract_price: _,
                outcomes: _,
                outcome_control_weights: _,
                weight_required: _,
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
            "get-outcome-control-public-key" => {
                Ok(serde_json::to_value(client.get_outcome_control_public_key())?)
            }

            "new-market" => {
                if args.len() != 3 {
                    return Err(anyhow::format_err!(
                        "`new-market` command expects 2 argument: <contract_price_msats> <outcomes>"
                    ));
                }

                let contract_price =
                    Amount::from_str_in(&args[1].to_string_lossy(), Denomination::MilliSatoshi)?;
                let outcomes: Outcome = args[2].to_string_lossy().parse()?;

                let mut outcome_control_weights = BTreeMap::new();
                outcome_control_weights.insert(client.get_outcome_control_public_key(), 1);

                let weight_required = 1;

                let market_out_point = client
                    .new_market(
                        contract_price,
                        outcomes,
                        outcome_control_weights,
                        weight_required,
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

                Ok(serde_json::to_value(client.get_market(out_point, false).await?)?)
            }

            "get-outcome-control-markets" => {
                let outcome_control_markets = client.get_outcome_control_markets(true, UnixTimestamp::ZERO)
                    .await?
                    .into_iter()
                    .map(|(_,market)|market)
                    .collect::<Vec<_>>();

                Ok(serde_json::to_value(outcome_control_markets)?)
            }

            "get-market-outcome-control-proposals" => {
                if args.len() != 2 {
                    return Err(anyhow::format_err!(
                        "`get-market` command expects 1 argument: <market_txid>"
                    ));
                }

                let Ok(txid) = TransactionId::from_str(&args[1].to_string_lossy()) else {
                    bail!("Error getting transaction id");
                };

                let out_point = OutPoint { txid, out_idx: 0 };

                Ok(serde_json::to_value(client.get_market_outcome_control_proposals(out_point, false).await?)?)
            }

            "propose-outcome-payout" => {
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
                        .propose_outcome_payout(market_out_point, outcome_payouts)
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
                if args.len() < 1 || args.len() > 3{
                    return Err(anyhow::format_err!(
                        "`list-orders` command only accepts 2 argument: (market_out_point) (outcome)"
                    ));
                }

                let mut market: Option<OutPoint> = None;
                if let Some(arg_tx_id) = args.get(1) {
                    market = Some(OutPoint{txid: TransactionId::from_str(&arg_tx_id.to_string_lossy())?, out_idx: 0});
                };

                let mut outcome: Option<Outcome> = None;
                if let Some(arg_outcome) = args.get(2) {
                    outcome = Some(Outcome::from_str(&arg_outcome.to_string_lossy())?);
                }

                Ok(serde_json::to_value(client.get_orders_from_db(market, outcome).await)?)
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

            "withdraw-available-bitcoin" => {
                Ok(serde_json::to_value(client.send_order_bitcoin_balance_to_primary_module().await?)?)
            }

            "sync-orders" => {
                if args.len() < 1 || args.len() > 3{
                    return Err(anyhow::format_err!(
                        "`sync-order` command only accepts 2 argument: (market_out_point) (outcome)"
                    ));
                }

                let mut market: Option<OutPoint> = None;
                if let Some(arg_tx_id) = args.get(1) {
                    market = Some(OutPoint{txid: TransactionId::from_str(&arg_tx_id.to_string_lossy())?, out_idx: 0});
                };

                let mut outcome: Option<Outcome> = None;
                if let Some(arg_outcome) = args.get(2) {
                    outcome = Some(Outcome::from_str(&arg_outcome.to_string_lossy())?);
                }

                Ok(serde_json::to_value(client.sync_orders(market, outcome, true).await?)?)
            }

            "recover-orders" => {
                Ok(serde_json::to_value(client.recover_orders().await?)?)
            }

            "get-candlesticks" => {
                let Ok(txid) = TransactionId::from_str(&args[1].to_string_lossy()) else {
                    bail!("Error getting transaction id");
                };

                let out_point = OutPoint { txid, out_idx: 0 };

                let outcome: Outcome = args[2].to_string_lossy().parse()?;

                let candlestick_interval: Seconds = args[3].to_string_lossy().parse()?;

                let candlesticks = client.
                    get_candlesticks(out_point, outcome, candlestick_interval, 500)
                    .await?
                    .into_iter()
                    .map(|(key,value)| (key.seconds.to_string(), value))
                    .collect::<BTreeMap<String, Candlestick>>();

                Ok(serde_json::to_value(candlesticks)?)
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
