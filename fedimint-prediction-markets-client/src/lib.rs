use std::collections::{BTreeMap, HashMap};
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
    GetMarketOutcomeCandlesticksResult, GetPayoutControlMarketsParams, Market, MarketInformation,
    Order, OrderIdClientSide, Outcome, Seconds, Side, UnixTimestamp, Weight, WeightRequiredForPayout,
};

use fedimint_core::{apply, async_trait_maybe_send, Amount, OutPoint, TransactionId};
use fedimint_prediction_markets_common::config::PredictionMarketsClientConfig;
use fedimint_prediction_markets_common::{
    PredictionMarketsCommonGen, PredictionMarketsInput, PredictionMarketsModuleTypes,
    PredictionMarketsOutput, KIND,
};

use futures::stream::FuturesUnordered;
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
    /// Get payout control public key that client controls.
    fn get_payout_control_public_key(&self) -> XOnlyPublicKey;

    /// Create new market
    async fn new_market(
        &self,
        contract_price: Amount,
        outcomes: Outcome,
        payout_control_weights: BTreeMap<XOnlyPublicKey, Weight>,
        weight_required_for_payout: WeightRequiredForPayout,
        information: MarketInformation,
    ) -> anyhow::Result<OutPoint>;

    /// Get Market
    async fn get_market(
        &self,
        market: OutPoint,
        from_local_cache: bool,
    ) -> anyhow::Result<Option<Market>>;

    /// Get all market [OutPoint]s that our payout control key has some sort of authority over.
    ///
    /// Returns (market creation time) to (market outpoint)
    async fn get_payout_control_markets(
        &self,
        sync_new_markets: bool,
        markets_created_after_and_including: UnixTimestamp,
    ) -> anyhow::Result<BTreeMap<UnixTimestamp, OutPoint>>;

    /// Propose payout
    async fn propose_payout(
        &self,
        market: OutPoint,
        outcome_payouts: Vec<Amount>,
    ) -> anyhow::Result<()>;

    /// Get market payout control proposals
    /// payout control to proposed payout
    async fn get_market_payout_control_proposals(
        &self,
        market: OutPoint,
        from_local_cache: bool,
    ) -> anyhow::Result<BTreeMap<XOnlyPublicKey, Vec<Amount>>>;

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
    ///
    /// Returns how much bitcoin was sent
    async fn send_order_bitcoin_balance_to_primary_module(&self) -> anyhow::Result<Amount>;

    /// Update all orders in db that could possibly be unsynced between federation and local order cache because
    /// of an order match or because of an operation the client has performed.
    ///
    /// Setting sync_possible_payouts to true also syncs orders that could have changed because of a market payout.
    ///
    /// Optionally provide a market (and outcome) to update only orders belonging to that market (and outcome).
    /// This option does not effect updating orders that have changed because of an operation the client has performed.
    /// 
    /// Returns number of orders updated
    async fn sync_orders(
        &self,
        sync_possible_payouts: bool,
        market: Option<OutPoint>,
        outcome: Option<Outcome>,
    ) -> anyhow::Result<u64>;

    /// Get all orders in the db.
    /// Optionally provide a market (and outcome) to get only orders belonging to that market (and outcome)
    async fn get_orders_from_db(
        &self,
        market: Option<OutPoint>,
        outcome: Option<Outcome>,
    ) -> BTreeMap<OrderIdClientSide, Order>;

    /// Used to recover orders in case of recovery from seed
    async fn recover_orders(&self, gap_size_to_check: u16) -> anyhow::Result<()>;

    /// get most recent candlesticks
    async fn get_candlesticks(
        &self,
        market: OutPoint,
        outcome: Outcome,
        candlestick_interval: Seconds,
        min_candlestick_timestamp: UnixTimestamp,
    ) -> anyhow::Result<BTreeMap<UnixTimestamp, Candlestick>>;
}

#[apply(async_trait_maybe_send!)]
impl OddsMarketsClientExt for Client {
    fn get_payout_control_public_key(&self) -> XOnlyPublicKey {
        let (prediction_markets, _) = self.get_first_module::<PredictionMarketsClientModule>(&KIND);

        let key = prediction_markets.get_payout_control_key_pair();

        XOnlyPublicKey::from_keypair(&key).0
    }

    async fn new_market(
        &self,
        contract_price: Amount,
        outcomes: Outcome,
        payout_control_weights: BTreeMap<XOnlyPublicKey, Weight>,
        weight_required_for_payout: WeightRequiredForPayout,
        information: MarketInformation,
    ) -> anyhow::Result<OutPoint> {
        let (_, instance) = self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let operation_id = OperationId::new_random();

        let output = ClientOutput {
            output: PredictionMarketsOutput::NewMarket {
                contract_price,
                outcomes,
                payout_control_weights,
                weight_required_for_payout,
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
                    // market payout control proposals so that Self::get_market_payout_control_proposals
                    // can always use local db if market in local db has payout.
                    if market.payout.is_some() {
                        _ = self
                            .get_market_payout_control_proposals(market_out_point, false)
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

    async fn get_payout_control_markets(
        &self,
        sync_new_markets: bool,
        markets_created_after_and_including: UnixTimestamp,
    ) -> anyhow::Result<BTreeMap<UnixTimestamp, OutPoint>> {
        let (prediction_markets, instance) =
            self.get_first_module::<PredictionMarketsClientModule>(&KIND);

        let mut dbtx = instance.db.begin_transaction().await;

        if sync_new_markets {
            let payout_control =
                XOnlyPublicKey::from_keypair(&prediction_markets.get_payout_control_key_pair()).0;
            let markets_created_after_and_including = {
                let mut stream = dbtx
                    .find_by_prefix_sorted_descending(&db::PayoutControlMarketsPrefixAll)
                    .await;

                stream
                    .next()
                    .await
                    .map(|(key, _)| key.market_created)
                    .unwrap_or(UnixTimestamp::ZERO)
            };
            let get_payout_control_markets_result = instance
                .api
                .get_payout_control_markets(GetPayoutControlMarketsParams {
                    payout_control,
                    markets_created_after_and_including,
                })
                .await?;

            for market_out_point in get_payout_control_markets_result.markets {
                let market = self
                    .get_market(market_out_point, false)
                    .await?
                    .expect("should always produce market");

                dbtx.insert_entry(
                    &db::PayoutControlMarketsKey {
                        market_created: market.created_consensus_timestamp,
                        market: market_out_point,
                    },
                    &(),
                )
                .await;
            }
        }

        let mut market_stream_newest_first = dbtx
            .find_by_prefix_sorted_descending(&db::PayoutControlMarketsPrefixAll)
            .await;
        let mut payout_control_markets = BTreeMap::new();
        loop {
            let Some((key, _)) = market_stream_newest_first.next().await else {
                break;
            };
            if key.market_created < markets_created_after_and_including {
                break;
            }

            payout_control_markets.insert(key.market_created, key.market);
        }

        drop(market_stream_newest_first);
        dbtx.commit_tx().await;

        Ok(payout_control_markets)
    }

    async fn get_market_payout_control_proposals(
        &self,
        market: OutPoint,
        from_local_cache: bool,
    ) -> anyhow::Result<BTreeMap<XOnlyPublicKey, Vec<Amount>>> {
        let (_, instance) = self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let mut dbtx = instance.db.begin_transaction().await;

        match from_local_cache {
            true => Ok(dbtx
                .find_by_prefix(&db::MarketPayoutControlProposalPrefix1 { market })
                .await
                .map(|(key, value)| (key.payout_control, value))
                .collect::<BTreeMap<_, _>>()
                .await),

            false => {
                // if market has payout in local db, Self::get_market has called Self::get_market_payout_control_proposals to
                // get final version of market payout control proposals.
                if let Some(market_db) = self.get_market(market, true).await? {
                    if market_db.payout.is_some() {
                        return self.get_market_payout_control_proposals(market, true).await;
                    }
                }

                let market_payout_control_proposals = instance
                    .api
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

    async fn propose_payout(
        &self,
        market_out_point: OutPoint,
        outcome_payouts: Vec<Amount>,
    ) -> anyhow::Result<()> {
        let (prediction_markets, instance) =
            self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let operation_id = OperationId::new_random();

        let payout_control_key = prediction_markets.get_payout_control_key_pair();

        let input = ClientInput {
            input: PredictionMarketsInput::PayoutProposal {
                market: market_out_point,
                payout_control: XOnlyPublicKey::from_keypair(&payout_control_key).0,
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

    async fn send_order_bitcoin_balance_to_primary_module(&self) -> anyhow::Result<Amount> {
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

        let mut total_amount = Amount::ZERO;

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

            total_amount = total_amount + order.bitcoin_balance;
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

        Ok(total_amount)
    }

    async fn sync_orders(
        &self,
        sync_possible_payouts: bool,
        market: Option<OutPoint>,
        outcome: Option<Outcome>,
    ) -> anyhow::Result<u64> {
        let (_, instance) = self.get_first_module::<PredictionMarketsClientModule>(&KIND);
        let mut dbtx = instance.db.begin_transaction().await;

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

        let updated_order_count = orders_to_update.len().try_into()?;

        let mut get_order_futures_unordered = orders_to_update
            .into_keys()
            .map(|id| async move { self.get_order(id.to_owned(), false).await })
            .collect::<FuturesUnordered<_>>();
        while let Some(result) = get_order_futures_unordered.next().await {
            result?.expect("should always produce order");
        }

        Ok(updated_order_count)
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

    async fn recover_orders(&self, gap_size_to_check: u16) -> anyhow::Result<()> {
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

    async fn get_candlesticks(
        &self,
        market: OutPoint,
        outcome: Outcome,
        candlestick_interval: Seconds,
        min_candlestick_timestamp: UnixTimestamp,
    ) -> anyhow::Result<BTreeMap<UnixTimestamp, Candlestick>> {
        let (_, instance) = self.get_first_module::<PredictionMarketsClientModule>(&KIND);

        let GetMarketOutcomeCandlesticksResult { candlesticks } = instance
            .api
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
}

#[derive(Debug)]
pub struct PredictionMarketsClientModule {
    cfg: PredictionMarketsClientConfig,
    root_secret: DerivableSecret,
    _notifier: ModuleNotifier<DynGlobalClientContext, PredictionMarketsStateMachine>,
}

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
                fee = self.cfg.gc.consumer_order_bitcoin_balance_fee;
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
                payout_control_weights: _,
                weight_required_for_payout: _,
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
            bail!("Expected to be called with at least 1 arguments: <command> …")
        }

        let command = args[0].to_string_lossy();

        match command.as_ref() {
            "get-payout-control-public-key" => {
                if args.len() != 1 {
                    bail!("`get-payout-control-public-key` expects 0 arguments")
                }

                Ok(serde_json::to_value(client.get_payout_control_public_key())?)
            }

            "new-market" => {
                if args.len() != 3 {
                    bail!("`new-market` command expects 2 argument: <contract_price_msats> <outcomes>")
                }

                let contract_price =
                    Amount::from_str_in(&args[1].to_string_lossy(), Denomination::MilliSatoshi)?;
                let outcomes: Outcome = args[2].to_string_lossy().parse()?;

                let mut payout_control_weights = BTreeMap::new();
                payout_control_weights.insert(client.get_payout_control_public_key(), 1);

                let weight_required = 1;

                let market_out_point = client
                    .new_market(
                        contract_price,
                        outcomes,
                        payout_control_weights,
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

            "get-payout-control-markets" => {
                if args.len() != 1 && args.len() != 2 {
                    bail!("`get-payout-control-markets` expects 0 arguments")
                }

                let payout_control_markets = client.get_payout_control_markets(true, UnixTimestamp::ZERO)
                    .await?
                    .into_iter()
                    .map(|(_,market)|market)
                    .collect::<Vec<_>>();

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

                Ok(serde_json::to_value(client.get_market_payout_control_proposals(out_point, false).await?)?)
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
                    client
                        .propose_payout(market_out_point, outcome_payouts)
                        .await?,
                )?)
            }

            "new-order" => {
                if args.len() != 6 {
                    bail!("`new-order` command expects 5 argument: <market_txid> <outcome> <side> <price_msats> <quantity>")
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
                if args.len() < 1 || args.len() > 3 {
                    bail!("`list-orders` command has 2 optional arguments: (market_txid) (outcome)")
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
                    bail!("`get-order` command expects 1 argument: <order_id>")
                }

                let id = OrderIdClientSide(args[1].to_string_lossy().parse()?);

                Ok(serde_json::to_value(
                    client.get_order(id, false).await?,
                )?)
            }

            "cancel-order" => {
                if args.len() != 2 {
                    bail!("`cancel-order` command expects 1 argument: <order_id>")
                }

                let id = OrderIdClientSide(args[1].to_string_lossy().parse()?);

                Ok(serde_json::to_value(client.cancel_order(id).await?)?)
            }

            "withdraw-available-bitcoin" => {
                if args.len() != 1 {
                    bail!("`withdraw-available-bitcoin` command expects 0 arguments")
                }

                Ok(serde_json::to_value(client.send_order_bitcoin_balance_to_primary_module().await?)?)
            }

            "sync-orders" => {
                if args.len() < 1 || args.len() > 3 {
                    bail!("`sync-order` command only accepts 2 argument: (market_txid) (outcome)")
                }

                let mut market: Option<OutPoint> = None;
                if let Some(arg_tx_id) = args.get(1) {
                    market = Some(OutPoint{txid: TransactionId::from_str(&arg_tx_id.to_string_lossy())?, out_idx: 0});
                };

                let mut outcome: Option<Outcome> = None;
                if let Some(arg_outcome) = args.get(2) {
                    outcome = Some(Outcome::from_str(&arg_outcome.to_string_lossy())?);
                }

                Ok(serde_json::to_value(client.sync_orders(true, market, outcome).await?)?)
            }

            "recover-orders" => {
                if args.len() != 1 && args.len() != 2 {
                    bail!("`recover-orders` command accepts 1 optional arguments: (gap_size_checked)")
                }

                let mut gap_size_to_check = 20u16;
                if let Some(s) = args.get(1) {
                    gap_size_to_check = s.to_string_lossy().parse()?;
                }

                Ok(serde_json::to_value(client.recover_orders(gap_size_to_check).await?)?)
            }

            "get-candlesticks" => {
                if args.len() != 4 && args.len() != 5 {
                    bail!("`get-candlesticks` command expects 3 argument and has 1 optional argument: <market_txid> <outcome> <candlestick_interval_seconds> (min_candlestick_timestamp)")
                }

                let Ok(txid) = TransactionId::from_str(&args[1].to_string_lossy()) else {
                    bail!("Error getting transaction id");
                };
                let market = OutPoint { txid, out_idx: 0 };

                let outcome: Outcome = args[2].to_string_lossy().parse()?;

                let candlestick_interval: Seconds = args[3].to_string_lossy().parse()?;

                let mut min_candlestick_timestamp = UnixTimestamp::ZERO;
                if let Some(s) = args.get(4) {
                    min_candlestick_timestamp = UnixTimestamp{
                        seconds: s.to_string_lossy().parse()?
                    }
                }

                let candlesticks = client.
                    get_candlesticks(market, outcome, candlestick_interval, min_candlestick_timestamp)
                    .await?
                    .into_iter()
                    .map(|(key,value)| (key.seconds.to_string(), value))
                    .collect::<BTreeMap<String, Candlestick>>();

                Ok(serde_json::to_value(candlesticks)?)
            }

            command => bail!(
                "Unknown command: {command}, supported commands: new-market, get-market, new-order, get-order, cancel-order, sync-orders, get-payout-control-public-key, get-candlesticks, recover-orders, withdraw-available-bitcoin, list-orders, propose-payout, get-market-payout-control-proposals, get-payout-control-markets"
            ),
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
