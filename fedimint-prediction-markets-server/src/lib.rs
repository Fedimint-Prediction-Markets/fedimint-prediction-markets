use std::collections::{BTreeMap, HashMap};

use std::string::ToString;
use std::time::Duration;

use anyhow::bail;
use async_trait::async_trait;

use db::DbKeyPrefix;
use fedimint_core::config::{
    ConfigGenModuleParams, DkgResult, ServerModuleConfig, ServerModuleConsensusConfig,
    TypedServerModuleConfig, TypedServerModuleConsensusConfig,
};
use fedimint_core::db::{Database, DatabaseVersion, MigrationMap, ModuleDatabaseTransaction};

use fedimint_core::module::audit::Audit;
use fedimint_core::module::{
    api_endpoint, ApiEndpoint, ApiError, ConsensusProposal, CoreConsensusVersion,
    ExtendsCommonModuleInit, InputMeta, IntoModuleError, ModuleConsensusVersion, ModuleError,
    PeerHandle, ServerModuleInit, SupportedModuleApiVersions, TransactionItemAmount,
};
use fedimint_core::server::DynServerModule;
use fedimint_core::task::TaskGroup;
use fedimint_core::{push_db_pair_items, Amount, OutPoint, PeerId, ServerModule};
pub use fedimint_prediction_markets_common::config::{
    PredictionMarketsClientConfig, PredictionMarketsConfig, PredictionMarketsConfigConsensus,
    PredictionMarketsConfigLocal, PredictionMarketsConfigPrivate, PredictionMarketsGenParams,
};
use fedimint_prediction_markets_common::{
    Candlestick, ContractAmount, ContractOfOutcomeAmount, GetMarketOutcomeCandlesticksParams,
    GetMarketOutcomeCandlesticksResult, GetPayoutControlMarketsParams,
    GetPayoutControlMarketsResult, Market, Order, Outcome, Payout, Seconds, Side, SignedAmount,
    TimeOrdering, UnixTimestamp, WeightRequiredForPayout,
};
pub use fedimint_prediction_markets_common::{
    PredictionMarketsCommonGen, PredictionMarketsConsensusItem, PredictionMarketsError,
    PredictionMarketsInput, PredictionMarketsModuleTypes, PredictionMarketsOutput,
    PredictionMarketsOutputOutcome, CONSENSUS_VERSION, KIND,
};
use futures::{future, StreamExt};

use secp256k1::XOnlyPublicKey;

use strum::IntoEnumIterator;

mod db;

/// Generates the module
#[derive(Debug, Clone)]
pub struct PredictionMarketsGen;

// TODO: Boilerplate-code
impl ExtendsCommonModuleInit for PredictionMarketsGen {
    type Common = PredictionMarketsCommonGen;
}

/// Implementation of server module non-consensus functions
#[async_trait]
impl ServerModuleInit for PredictionMarketsGen {
    type Params = PredictionMarketsGenParams;
    const DATABASE_VERSION: DatabaseVersion = DatabaseVersion(0);

    /// Returns the version of this module
    fn versions(&self, _core: CoreConsensusVersion) -> &[ModuleConsensusVersion] {
        &[CONSENSUS_VERSION]
    }

    fn supported_api_versions(&self) -> SupportedModuleApiVersions {
        SupportedModuleApiVersions::from_raw(1, 0, &[(0, 0)])
    }

    /// Initialize the module
    async fn init(
        &self,
        cfg: ServerModuleConfig,
        _db: Database,
        _task_group: &mut TaskGroup,
    ) -> anyhow::Result<DynServerModule> {
        Ok(PredictionMarkets::new(cfg.to_typed()?).into())
    }

    /// DB migrations to move from old to newer versions
    fn get_database_migrations(&self) -> MigrationMap {
        let migrations = MigrationMap::new();
        migrations
    }

    /// Generates configs for all peers in a trusted manner for testing
    fn trusted_dealer_gen(
        &self,
        peers: &[PeerId],
        params: &ConfigGenModuleParams,
    ) -> BTreeMap<PeerId, ServerModuleConfig> {
        let params = self.parse_params(params).unwrap();

        // Generate a config for each peer
        peers
            .iter()
            .map(|&peer| {
                let config = PredictionMarketsConfig {
                    local: PredictionMarketsConfigLocal {
                        peer_count: peers.len().try_into().expect("should never fail"),
                    },
                    private: PredictionMarketsConfigPrivate {
                        example: "test".to_owned(),
                    },
                    consensus: PredictionMarketsConfigConsensus {
                        gc: params.consensus.gc.to_owned(),
                    },
                };
                (peer, config.to_erased())
            })
            .collect()
    }

    /// Generates configs for all peers in an untrusted manner
    async fn distributed_gen(
        &self,
        peers: &PeerHandle,
        params: &ConfigGenModuleParams,
    ) -> DkgResult<ServerModuleConfig> {
        let params = self.parse_params(params).unwrap();

        Ok(PredictionMarketsConfig {
            local: PredictionMarketsConfigLocal {
                peer_count: peers
                    .peer_ids()
                    .len()
                    .try_into()
                    .expect("should never fail"),
            },
            private: PredictionMarketsConfigPrivate {
                example: "test".to_owned(),
            },
            consensus: PredictionMarketsConfigConsensus {
                gc: params.consensus.gc,
            },
        }
        .to_erased())
    }

    /// Converts the consensus config into the client config
    fn get_client_config(
        &self,
        config: &ServerModuleConsensusConfig,
    ) -> anyhow::Result<PredictionMarketsClientConfig> {
        let config = PredictionMarketsConfigConsensus::from_erased(config)?;
        Ok(PredictionMarketsClientConfig { gc: config.gc })
    }

    /// Validates the private/public key of configs
    fn validate_config(
        &self,
        _identity: &PeerId,
        config: ServerModuleConfig,
    ) -> anyhow::Result<()> {
        let _config = config.to_typed::<PredictionMarketsConfig>()?;

        Ok(())
    }

    /// Dumps all database items for debugging
    async fn dump_database(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        prefix_names: Vec<String>,
    ) -> Box<dyn Iterator<Item = (String, Box<dyn erased_serde::Serialize + Send>)> + '_> {
        // TODO: Boilerplate-code
        let mut items: BTreeMap<String, Box<dyn erased_serde::Serialize + Send>> = BTreeMap::new();
        let filtered_prefixes = db::DbKeyPrefix::iter().filter(|f| {
            prefix_names.is_empty() || prefix_names.contains(&f.to_string().to_lowercase())
        });

        for table in filtered_prefixes {
            match table {
                DbKeyPrefix::Outcome => {
                    push_db_pair_items!(
                        dbtx,
                        db::OutcomePrefixAll,
                        db::OutcomeKey,
                        PredictionMarketsOutputOutcome,
                        items,
                        "Output Outcome"
                    );
                }
                DbKeyPrefix::Market => {
                    push_db_pair_items!(
                        dbtx,
                        db::MarketPrefixAll,
                        db::MarketKey,
                        Market,
                        items,
                        "Market"
                    );
                }
                DbKeyPrefix::Order => {
                    push_db_pair_items!(
                        dbtx,
                        db::OrderPrefixAll,
                        db::OrderKey,
                        Order,
                        items,
                        "Order"
                    );
                }
                DbKeyPrefix::NextOrderTimeOrdering => {
                    push_db_pair_items!(
                        dbtx,
                        db::NextOrderTimeOrderingPrefixAll,
                        db::NextOrderTimePriorityKey,
                        TimeOrdering,
                        items,
                        "NextOrderTimePriority"
                    );
                }
                DbKeyPrefix::OrdersByMarket => {
                    push_db_pair_items!(
                        dbtx,
                        db::OrdersByMarketPrefixAll,
                        db::OrdersByMarketKey,
                        (),
                        items,
                        "OrdersByMarket"
                    );
                }
                DbKeyPrefix::OrderPriceTimePriority => {
                    push_db_pair_items!(
                        dbtx,
                        db::OrderPriceTimePriorityPrefixAll,
                        db::OrderPriceTimePriorityKey,
                        XOnlyPublicKey,
                        items,
                        "OrderPriceTimePriority"
                    );
                }
                DbKeyPrefix::MarketPayoutControlProposal => {
                    push_db_pair_items!(
                        dbtx,
                        db::MarketPayoutControlProposalPrefixAll,
                        db::MarketPayoutControlVoteKey,
                        Vec<Amount>,
                        items,
                        "MarketPayoutControlVote"
                    );
                }
                DbKeyPrefix::MarketOutcomePayoutsProposals => {
                    push_db_pair_items!(
                        dbtx,
                        db::MarketOutcomePayoutsProposalsPrefixAll,
                        db::MarketOutcomePayoutsVotesKey,
                        (),
                        items,
                        "MarketOutcomePayoutsProposals"
                    );
                }
                DbKeyPrefix::MarketOutcomeCandlesticks => {
                    push_db_pair_items!(
                        dbtx,
                        db::MarketOutcomeCandlesticksPrefixAll,
                        db::MarketOutcomeCandlesticksKey,
                        Candlestick,
                        items,
                        "MarketOutcomeCandleSticks"
                    );
                }
                DbKeyPrefix::PayoutControlMarkets => {
                    push_db_pair_items!(
                        dbtx,
                        db::PayoutControlMarketsPrefixAll,
                        db::PayoutControlMarketsKey,
                        (),
                        items,
                        "PayoutControlMarkets"
                    );
                }
                DbKeyPrefix::PeersProposedTimestamp => {
                    push_db_pair_items!(
                        dbtx,
                        db::PeersProposedTimestampPrefixAll,
                        db::PeersProposedTimestampKey,
                        UnixTimestamp,
                        items,
                        "PeersProposedTimestampKey"
                    );
                }
            }
        }

        Box::new(items.into_iter())
    }
}

/// Dummy module
#[derive(Debug)]
pub struct PredictionMarkets {
    pub cfg: PredictionMarketsConfig,
}

/// Implementation of consensus for the server module
#[async_trait]
impl ServerModule for PredictionMarkets {
    /// Define the consensus types
    type Common = PredictionMarketsModuleTypes;
    type Gen = PredictionMarketsGen;
    type VerificationCache = PredictionMarketsCache;

    async fn await_consensus_proposal(&self, dbtx: &mut ModuleDatabaseTransaction<'_>) {
        tokio::time::sleep(Duration::from_millis(350)).await;

        let next_consensus_timestamp = {
            let mut current = self.get_consensus_timestamp(dbtx).await;
            current.0 += self.cfg.consensus.gc.timestamp_interval;
            current
        };

        tokio::time::sleep(next_consensus_timestamp.duration_till()).await;
    }

    async fn consensus_proposal(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
    ) -> ConsensusProposal<PredictionMarketsConsensusItem> {
        let mut items = vec![];

        let timestamp_to_propose =
            UnixTimestamp::now().round_down(self.cfg.consensus.gc.timestamp_interval);
        let consensus_timestamp = self.get_consensus_timestamp(dbtx).await;

        if timestamp_to_propose > consensus_timestamp {
            items.push(PredictionMarketsConsensusItem::TimestampProposal(
                timestamp_to_propose,
            ));
        }

        ConsensusProposal::new_auto_trigger(items)
    }

    async fn process_consensus_item<'a, 'b>(
        &'a self,
        dbtx: &mut ModuleDatabaseTransaction<'b>,
        consensus_item: PredictionMarketsConsensusItem,
        peer_id: PeerId,
    ) -> anyhow::Result<()> {
        match consensus_item {
            PredictionMarketsConsensusItem::TimestampProposal(new) => {
                if !new.divisible(self.cfg.consensus.gc.timestamp_interval) {
                    bail!("new unix timestamp is not divisible by timestamp interval");
                }

                match dbtx
                    .insert_entry(&db::PeersProposedTimestampKey { peer_id }, &new)
                    .await
                {
                    Some(current) => {
                        if new <= current {
                            bail!("new unix timestamp is not after current unix timestamp")
                        } else {
                            Ok(())
                        }
                    }
                    None => Ok(()),
                }
            }
        }
    }

    fn build_verification_cache<'a>(
        &'a self,
        _inputs: impl Iterator<Item = &'a PredictionMarketsInput> + Send,
    ) -> Self::VerificationCache {
        PredictionMarketsCache
    }

    async fn process_input<'a, 'b, 'c>(
        &'a self,
        dbtx: &mut ModuleDatabaseTransaction<'c>,
        input: &'b PredictionMarketsInput,
        _cache: &Self::VerificationCache,
    ) -> Result<InputMeta, ModuleError> {
        let amount;
        let fee;
        let pub_keys;

        match input {
            PredictionMarketsInput::NewSellOrder {
                owner,
                market: market_out_point,
                outcome,
                price,
                sources,
            } => {
                // check that order does not already exists for owner
                if let Some(_) = dbtx.get_value(&db::OrderKey(owner.to_owned())).await {
                    return Err(PredictionMarketsError::OrderAlreadyExists)
                        .into_module_error_other();
                }

                // get market
                let Some(market) = dbtx
                    .get_value(&db::MarketKey(market_out_point.to_owned()))
                    .await
                else {
                    return Err(PredictionMarketsError::MarketDoesNotExist)
                        .into_module_error_other();
                };

                // check if payout has already occurred
                if market.payout.is_some() {
                    return Err(PredictionMarketsError::MarketFinished).into_module_error_other();
                }

                // get quantity from sources, verifying public keys of sources
                let Ok((quantity, source_order_public_keys)) =
                    Self::process_contract_of_outcome_sources(
                        dbtx,
                        sources,
                        market_out_point,
                        outcome,
                    )
                    .await
                else {
                    return Err(PredictionMarketsError::OrderValidationFailed)
                        .into_module_error_other();
                };

                // verify order params
                if let Err(_) = Order::validate_order_params(
                    &market,
                    &self.cfg.consensus.gc.max_order_quantity,
                    outcome,
                    price,
                    &quantity,
                ) {
                    return Err(PredictionMarketsError::OrderValidationFailed)
                        .into_module_error_other();
                }

                // set input meta
                amount = Amount::ZERO;
                fee = self.cfg.consensus.gc.new_order_fee;
                pub_keys = source_order_public_keys;

                // process order
                self.process_new_order(
                    dbtx,
                    market,
                    owner.to_owned(),
                    market_out_point.to_owned(),
                    outcome.to_owned(),
                    Side::Sell,
                    price.to_owned(),
                    quantity.to_owned(),
                )
                .await;
            }
            PredictionMarketsInput::ConsumeOrderBitcoinBalance {
                order: order_owner,
                amount: amount_to_consume,
            } => {
                // get order
                let Some(mut order) = dbtx.get_value(&db::OrderKey(order_owner.to_owned())).await
                else {
                    return Err(PredictionMarketsError::OrderDoesNotExist)
                        .into_module_error_other();
                };

                // check if order has sufficent balance
                if &order.bitcoin_balance < amount_to_consume {
                    return Err(PredictionMarketsError::NotEnoughFunds).into_module_error_other();
                }

                // set input meta
                amount = amount_to_consume.to_owned();
                fee = self.cfg.consensus.gc.consumer_order_bitcoin_balance_fee;
                pub_keys = vec![order_owner.to_owned()];

                // update order's bitcoin balance
                order.bitcoin_balance = order.bitcoin_balance - amount_to_consume.to_owned();
                dbtx.insert_entry(&db::OrderKey(order_owner.to_owned()), &order)
                    .await;
            }
            PredictionMarketsInput::CancelOrder { order: order_owner } => {
                // get order
                let Some(mut order) = dbtx.get_value(&db::OrderKey(order_owner.to_owned())).await
                else {
                    return Err(PredictionMarketsError::OrderDoesNotExist)
                        .into_module_error_other();
                };

                // check if order already finished
                if order.quantity_waiting_for_match == ContractOfOutcomeAmount::ZERO {
                    return Err(PredictionMarketsError::OrderAlreadyFinished)
                        .into_module_error_other();
                }

                // set input meta
                amount = Amount::ZERO;
                fee = Amount::ZERO;
                pub_keys = vec![order_owner.to_owned()];

                // cancel order
                Self::cancel_order(dbtx, order_owner.to_owned(), &mut order).await;
            }
            PredictionMarketsInput::PayoutProposal {
                market: market_out_point,
                payout_control,
                outcome_payouts,
            } => {
                // get market
                let Some(mut market) = dbtx
                    .get_value(&db::MarketKey(market_out_point.to_owned()))
                    .await
                else {
                    return Err(PredictionMarketsError::MarketDoesNotExist)
                        .into_module_error_other();
                };

                // check if payout already exists
                if market.payout.is_some() {
                    return Err(PredictionMarketsError::PayoutAlreadyExists)
                        .into_module_error_other();
                }

                // Check that payout control exist on market
                if let None = market.payout_controls_weights.get(payout_control) {
                    return Err(PredictionMarketsError::PayoutValidationFailed)
                        .into_module_error_other();
                }

                // validate payout params
                if let Err(_) = Payout::validate_payout_params(&market, outcome_payouts) {
                    return Err(PredictionMarketsError::PayoutValidationFailed)
                        .into_module_error_other();
                }

                // set input meta
                amount = Amount::ZERO;
                fee = self.cfg.consensus.gc.payout_proposal_fee;
                pub_keys = vec![payout_control.to_owned()];

                let consensus_timestamp = self.get_consensus_timestamp(dbtx).await;

                // clear out old payout proposal if exists
                if let Some(outcome_payouts) = dbtx
                    .get_value(&db::MarketPayoutControlProposalKey {
                        market: market_out_point.to_owned(),
                        payout_control: payout_control.to_owned(),
                    })
                    .await
                {
                    dbtx.remove_entry(&db::MarketOutcomePayoutsProposalsKey {
                        market: market_out_point.to_owned(),
                        outcome_payouts,
                        payout_control: payout_control.to_owned(),
                    })
                    .await
                    .expect("should always remove value");
                }

                // add payout proposal to db
                dbtx.insert_entry(
                    &db::MarketPayoutControlProposalKey {
                        market: market_out_point.to_owned(),
                        payout_control: payout_control.to_owned(),
                    },
                    outcome_payouts,
                )
                .await;
                dbtx.insert_entry(
                    &db::MarketOutcomePayoutsProposalsKey {
                        market: market_out_point.to_owned(),
                        outcome_payouts: outcome_payouts.to_owned(),
                        payout_control: payout_control.to_owned(),
                    },
                    &(),
                )
                .await;

                let payout_triggered = {
                    let payout_controls_with_proposal: Vec<_> = dbtx
                        .find_by_prefix(&db::MarketOutcomePayoutsProposalsPrefix2 {
                            market: market_out_point.to_owned(),
                            outcome_payouts: outcome_payouts.to_owned(),
                        })
                        .await
                        .map(|(key, _)| key.payout_control)
                        .collect()
                        .await;

                    let mut total_weight_of_votes: WeightRequiredForPayout = 0;
                    for payout_control in payout_controls_with_proposal {
                        let weight = market
                            .payout_controls_weights
                            .get(&payout_control)
                            .expect("should always find payout_control");
                        total_weight_of_votes += WeightRequiredForPayout::from(weight.to_owned())
                    }

                    if total_weight_of_votes >= market.weight_required_for_payout {
                        true
                    } else {
                        false
                    }
                };

                if payout_triggered {
                    // process payout
                    let market_orders: Vec<_> = dbtx
                        .find_by_prefix(&db::OrdersByMarketPrefix1 {
                            market: market_out_point.to_owned(),
                        })
                        .await
                        .map(|(key, _)| key.order)
                        .collect()
                        .await;

                    let mut total_payout = Amount::ZERO;
                    for order_owner in market_orders {
                        let mut order = dbtx
                            .get_value(&db::OrderKey(order_owner))
                            .await
                            .expect("should always find order");

                        Self::cancel_order(dbtx, order_owner, &mut order).await;

                        let payout_per_contract_of_outcome = outcome_payouts
                            .get(usize::from(order.outcome))
                            .expect("should be some");
                        let order_payout = payout_per_contract_of_outcome.to_owned()
                            * order.contract_of_outcome_balance.0;

                        order.contract_of_outcome_balance = ContractOfOutcomeAmount::ZERO;
                        order.bitcoin_balance = order.bitcoin_balance + order_payout;
                        order.bitcoin_cost = order.bitcoin_cost - SignedAmount::from(order_payout);

                        dbtx.insert_entry(&db::OrderKey(order_owner), &order).await;

                        total_payout = total_payout + order_payout;
                    }

                    assert_eq!(
                        market.contract_price * market.open_contracts.0,
                        total_payout
                    );

                    // save payout to market
                    market.open_contracts = ContractAmount::ZERO;
                    market.payout = Some(Payout {
                        outcome_payouts: outcome_payouts.to_owned(),
                        occurred_consensus_timestamp: consensus_timestamp,
                    });
                    dbtx.insert_new_entry(&db::MarketKey(market_out_point.to_owned()), &market)
                        .await;
                }
            }
        }

        Ok(InputMeta {
            amount: TransactionItemAmount { amount, fee },
            // IMPORTANT: include the pubkey to validate the user signed this tx
            pub_keys,
        })
    }

    async fn process_output<'a, 'b>(
        &'a self,
        dbtx: &mut ModuleDatabaseTransaction<'b>,
        output: &'a PredictionMarketsOutput,
        out_point: OutPoint,
    ) -> Result<TransactionItemAmount, ModuleError> {
        let amount;
        let fee;

        match output {
            PredictionMarketsOutput::NewMarket {
                contract_price,
                outcomes,
                payout_control_weights,
                weight_required_for_payout,
                information,
            } => {
                // verify market params
                if let Err(_) = Market::validate_market_params(
                    &self.cfg.consensus.gc.max_contract_price,
                    &self.cfg.consensus.gc.max_market_outcomes,
                    &self.cfg.consensus.gc.max_payout_control_keys,
                    contract_price,
                    outcomes,
                    payout_control_weights,
                    information,
                ) {
                    return Err(PredictionMarketsError::MarketValidationFailed)
                        .into_module_error_other();
                }

                // set output meta
                amount = Amount::ZERO;
                fee = self.cfg.consensus.gc.new_market_fee;

                // save outcome
                dbtx.insert_new_entry(
                    &db::OutcomeKey(out_point),
                    &PredictionMarketsOutputOutcome::NewMarket,
                )
                .await;

                let consensus_timestamp = self.get_consensus_timestamp(dbtx).await;

                // save market
                dbtx.insert_new_entry(
                    &db::MarketKey(out_point),
                    &Market {
                        contract_price: contract_price.to_owned(),
                        outcomes: outcomes.to_owned(),
                        payout_controls_weights: payout_control_weights.to_owned(),
                        weight_required_for_payout: weight_required_for_payout.to_owned(),
                        information: information.to_owned(),
                        created_consensus_timestamp: consensus_timestamp,

                        open_contracts: ContractAmount::ZERO,
                        payout: None,
                    },
                )
                .await;

                // save starting next order time priority
                dbtx.insert_new_entry(
                    &db::NextOrderTimeOrderingKey { market: out_point },
                    &TimeOrdering(0),
                )
                .await;

                // PayoutControlMarkets index insert
                for (payout_control, _) in payout_control_weights.iter() {
                    dbtx.insert_new_entry(
                        &db::PayoutControlMarketsKey {
                            payout_control: payout_control.to_owned(),
                            market_created: consensus_timestamp,
                            market: out_point,
                        },
                        &(),
                    )
                    .await;
                }
            }
            PredictionMarketsOutput::NewBuyOrder {
                owner,
                market: market_out_point,
                outcome,
                price,
                quantity,
            } => {
                // check that order does not already exists for owner
                if let Some(_) = dbtx.get_value(&db::OrderKey(owner.to_owned())).await {
                    return Err(PredictionMarketsError::OrderAlreadyExists)
                        .into_module_error_other();
                }

                // get market
                let Some(market) = dbtx
                    .get_value(&db::MarketKey(market_out_point.to_owned()))
                    .await
                else {
                    return Err(PredictionMarketsError::MarketDoesNotExist)
                        .into_module_error_other();
                };

                // check if payout has already occurred
                if market.payout.is_some() {
                    return Err(PredictionMarketsError::MarketFinished).into_module_error_other();
                }

                // verify order params
                if let Err(_) = Order::validate_order_params(
                    &market,
                    &self.cfg.consensus.gc.max_order_quantity,
                    outcome,
                    price,
                    quantity,
                ) {
                    return Err(PredictionMarketsError::OrderValidationFailed)
                        .into_module_error_other();
                }

                // set output meta
                amount = price.to_owned() * quantity.0;
                fee = self.cfg.consensus.gc.new_order_fee;

                // save outcome
                dbtx.insert_new_entry(
                    &db::OutcomeKey(out_point),
                    &PredictionMarketsOutputOutcome::NewBuyOrder,
                )
                .await;

                // process order
                self.process_new_order(
                    dbtx,
                    market,
                    owner.to_owned(),
                    market_out_point.to_owned(),
                    outcome.to_owned(),
                    Side::Buy,
                    price.to_owned(),
                    quantity.to_owned(),
                )
                .await;
            }
        }

        Ok(TransactionItemAmount { amount, fee })
    }

    async fn output_status(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        out_point: OutPoint,
    ) -> Option<PredictionMarketsOutputOutcome> {
        dbtx.get_value(&db::OutcomeKey(out_point)).await
    }

    async fn audit(&self, dbtx: &mut ModuleDatabaseTransaction<'_>, audit: &mut Audit) {
        // bitcoin owed for open contracts across markets
        audit
            .add_items(dbtx, KIND.as_str(), &db::MarketPrefixAll, |_, market| {
                -((market.contract_price * market.open_contracts.0).msats as i64)
            })
            .await;

        // bitcoin owed for collateral held for buy orders and in field bitcoin_balance on orders
        audit
            .add_items(dbtx, KIND.as_str(), &db::OrderPrefixAll, |_, order| {
                let mut milli_sat = 0i64;
                if let Side::Buy = order.side {
                    milli_sat -= (order.price * order.quantity_waiting_for_match.0).msats as i64
                }
                milli_sat -= order.bitcoin_balance.msats as i64;

                milli_sat
            })
            .await;
    }

    fn api_endpoints(&self) -> Vec<ApiEndpoint<Self>> {
        vec![
            api_endpoint! {
                "get_market",
                async |module: &PredictionMarkets, context, market: OutPoint| -> Option<Market> {
                    module.api_get_market(&mut context.dbtx(), market).await
                }
            },
            api_endpoint! {
                "get_order",
                async |module: &PredictionMarkets, context, order: XOnlyPublicKey| -> Option<Order> {
                    module.api_get_order(&mut context.dbtx(), order).await
                }
            },
            api_endpoint! {
                "get_payout_control_markets",
                async |module: &PredictionMarkets, context, params: GetPayoutControlMarketsParams| -> GetPayoutControlMarketsResult {
                    module.api_get_payout_control_markets(&mut context.dbtx(), params).await
                }
            },
            api_endpoint! {
                "get_market_payout_control_proposals",
                async |module: &PredictionMarkets, context, market: OutPoint| -> BTreeMap<XOnlyPublicKey,Vec<Amount>> {
                    module.api_get_market_payout_control_proposals(&mut context.dbtx(), market).await
                }
            },
            api_endpoint! {
                "get_market_outcome_candlesticks",
                async |module: &PredictionMarkets, context, params: GetMarketOutcomeCandlesticksParams| -> GetMarketOutcomeCandlesticksResult {
                    module.api_get_market_outcome_candlesticks(&mut context.dbtx(), params).await
                }
            },
        ]
    }
}

// api endpoints
impl PredictionMarkets {
    async fn api_get_market(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        market: OutPoint,
    ) -> Result<Option<Market>, ApiError> {
        Ok(dbtx.get_value(&db::MarketKey(market)).await)
    }

    async fn api_get_order(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        order: XOnlyPublicKey,
    ) -> Result<Option<Order>, ApiError> {
        Ok(dbtx.get_value(&db::OrderKey(order)).await)
    }

    async fn api_get_payout_control_markets(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        params: GetPayoutControlMarketsParams,
    ) -> Result<GetPayoutControlMarketsResult, ApiError> {
        let mut markets = vec![];

        let mut stream = dbtx
            .find_by_prefix_sorted_descending(&db::PayoutControlMarketsPrefix1 {
                payout_control: params.payout_control,
            })
            .await;
        loop {
            let Some((key, _)) = stream.next().await else {
                break;
            };

            if key.market_created < params.markets_created_after_and_including {
                break;
            }

            markets.push(key.market);
        }

        Ok(GetPayoutControlMarketsResult { markets })
    }

    async fn api_get_market_payout_control_proposals(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        market: OutPoint,
    ) -> Result<BTreeMap<XOnlyPublicKey, Vec<Amount>>, ApiError> {
        Ok(dbtx
            .find_by_prefix(&db::MarketPayoutControlProposalPrefix1 { market })
            .await
            .map(|(key, value)| (key.payout_control, value))
            .collect()
            .await)
    }

    async fn api_get_market_outcome_candlesticks(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        params: GetMarketOutcomeCandlesticksParams,
    ) -> Result<GetMarketOutcomeCandlesticksResult, ApiError> {
        let candlesticks = dbtx
            .find_by_prefix_sorted_descending(&db::MarketOutcomeCandlesticksPrefix3 {
                market: params.market,
                outcome: params.outcome,
                candlestick_interval: params.candlestick_interval,
            })
            .await
            .take_while(|(key, _)| {
                future::ready(key.candlestick_timestamp >= params.min_candlestick_timestamp)
            })
            .map(|(key, value)| (key.candlestick_timestamp, value))
            .collect::<Vec<(UnixTimestamp, Candlestick)>>()
            .await;

        Ok(GetMarketOutcomeCandlesticksResult { candlesticks })
    }
}

// market operations
impl PredictionMarkets {
    async fn get_next_order_time_ordering(
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        market: OutPoint,
    ) -> TimeOrdering {
        let time_ordering = dbtx
            .get_value(&db::NextOrderTimeOrderingKey { market })
            .await
            .expect("should always produce value");

        // increment
        dbtx.insert_entry(
            &db::NextOrderTimeOrderingKey { market },
            &TimeOrdering(time_ordering.0 + 1),
        )
        .await;

        time_ordering
    }

    async fn process_contract_of_outcome_sources(
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        sources: &BTreeMap<XOnlyPublicKey, ContractOfOutcomeAmount>,
        market: &OutPoint,
        outcome: &Outcome,
    ) -> Result<(ContractOfOutcomeAmount, Vec<XOnlyPublicKey>), ()> {
        let mut total_amount = ContractOfOutcomeAmount::ZERO;
        let mut source_order_public_keys = Vec::new();

        for (order_owner, quantity) in sources {
            let Some(mut order) = dbtx.get_value(&db::OrderKey(order_owner.to_owned())).await
            else {
                return Err(());
            };

            if market != &order.market
                || outcome != &order.outcome
                || quantity == &ContractOfOutcomeAmount::ZERO
                || quantity > &order.contract_of_outcome_balance
            {
                return Err(());
            }

            order.contract_of_outcome_balance =
                order.contract_of_outcome_balance - quantity.to_owned();
            dbtx.insert_entry(&db::OrderKey(order_owner.to_owned()), &order)
                .await;

            total_amount = total_amount + quantity.to_owned();
            source_order_public_keys.push(order_owner.to_owned());
        }

        Ok((total_amount, source_order_public_keys))
    }

    async fn process_new_order(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        mut market: Market,
        order_owner: XOnlyPublicKey,
        market_out_point: OutPoint,
        outcome: Outcome,
        side: Side,
        price: Amount,
        quantity: ContractOfOutcomeAmount,
    ) {
        let consensus_timestamp = self.get_consensus_timestamp(dbtx).await;
        let beginning_market_open_contracts = market.open_contracts;

        let mut order_cache = OrderCache::new();
        let mut price_quantity_cache = PriceQuantityCache::new();
        let mut candlestick_data_creator = CandlestickDataCreator::new(
            &self.cfg.consensus.gc.candlestick_intervals,
            self.cfg
                .consensus
                .gc
                .max_candlesticks_kept_per_market_outcome_interval,
            consensus_timestamp,
            market_out_point,
        );

        let mut order = Order {
            market: market_out_point,
            outcome,
            side,
            price,
            original_quantity: quantity,
            time_ordering: PredictionMarkets::get_next_order_time_ordering(dbtx, market_out_point)
                .await,
            created_consensus_timestamp: consensus_timestamp,

            quantity_waiting_for_match: quantity,
            contract_of_outcome_balance: ContractOfOutcomeAmount::ZERO,
            bitcoin_balance: Amount::ZERO,

            bitcoin_cost: SignedAmount::ZERO,
        };

        while order.quantity_waiting_for_match > ContractOfOutcomeAmount::ZERO {
            let own = Self::get_outcome_side_best_price_quantity(
                dbtx,
                &mut order_cache,
                &mut price_quantity_cache,
                &market_out_point,
                &outcome,
                &side.opposite(),
            )
            .await;
            let other = Self::get_other_outcome_sides_best_price_quantity(
                dbtx,
                &mut order_cache,
                &mut price_quantity_cache,
                &market_out_point,
                &market,
                &outcome,
                &side,
            )
            .await;

            let mut matches_own = false;
            if let Some((own_price, _)) = own {
                matches_own = if let Some((other_price, _)) = other {
                    match side {
                        Side::Buy => order.price >= own_price && other_price >= own_price.into(),
                        Side::Sell => order.price <= own_price && other_price <= own_price.into(),
                    }
                } else {
                    match side {
                        Side::Buy => order.price >= own_price,
                        Side::Sell => order.price <= own_price,
                    }
                };
            }

            let mut matches_other = false;
            if !matches_own {
                if let Some((other_price, _)) = other {
                    matches_other = match side {
                        Side::Buy => SignedAmount::from(order.price) >= other_price,
                        Side::Sell => SignedAmount::from(order.price) <= other_price,
                    }
                }
            }

            // process own outcome match (contract swap)
            if matches_own {
                let (own_price, own_quantity) = own.expect("should always be some");
                let satisfied_quantity = order.quantity_waiting_for_match.min(own_quantity);

                Self::process_market_quantity(
                    dbtx,
                    &mut order_cache,
                    &mut candlestick_data_creator,
                    &market_out_point,
                    &market,
                    &outcome,
                    &side.opposite(),
                    satisfied_quantity,
                )
                .await;
                price_quantity_cache.sub_outcome_side_quantity(
                    outcome,
                    side.opposite(),
                    satisfied_quantity,
                );

                order.quantity_waiting_for_match =
                    order.quantity_waiting_for_match - satisfied_quantity;

                match side {
                    Side::Buy => {
                        order.contract_of_outcome_balance =
                            order.contract_of_outcome_balance + satisfied_quantity;

                        order.bitcoin_balance = order.bitcoin_balance
                            + ((order.price - own_price) * satisfied_quantity.0);

                        order.bitcoin_cost = order.bitcoin_cost
                            + SignedAmount::from(own_price * satisfied_quantity.0);
                    }
                    Side::Sell => {
                        order.bitcoin_balance =
                            order.bitcoin_balance + (own_price * satisfied_quantity.0);

                        order.bitcoin_cost = order.bitcoin_cost
                            - SignedAmount::from(own_price * satisfied_quantity.0);
                    }
                }

                candlestick_data_creator
                    .add(dbtx, outcome, own_price, satisfied_quantity)
                    .await;

            // process other outcome match (contract creation/destruction)
            } else if matches_other {
                let (other_price, other_quantity) = other.expect("should always be some");
                let satisfied_quantity = order.quantity_waiting_for_match.min(other_quantity);

                for i in 0..market.outcomes {
                    if i != outcome {
                        Self::process_market_quantity(
                            dbtx,
                            &mut order_cache,
                            &mut candlestick_data_creator,
                            &market_out_point,
                            &market,
                            &i,
                            &side,
                            satisfied_quantity,
                        )
                        .await;
                        price_quantity_cache.sub_outcome_side_quantity(i, side, satisfied_quantity);
                    }
                }

                order.quantity_waiting_for_match =
                    order.quantity_waiting_for_match - satisfied_quantity;

                match side {
                    Side::Buy => {
                        order.contract_of_outcome_balance =
                            order.contract_of_outcome_balance + satisfied_quantity;

                        order.bitcoin_balance = order.bitcoin_balance
                            + (Amount::try_from(SignedAmount::from(order.price) - other_price)
                                .expect("should always convert")
                                * satisfied_quantity.0);

                        order.bitcoin_cost =
                            order.bitcoin_cost + (other_price * satisfied_quantity.0);

                        market.open_contracts =
                            market.open_contracts + ContractAmount(satisfied_quantity.0);
                    }
                    Side::Sell => {
                        order.bitcoin_balance = order.bitcoin_balance
                            + (Amount::try_from(other_price).expect("should always convert")
                                * satisfied_quantity.0);

                        order.bitcoin_cost =
                            order.bitcoin_cost - (other_price * satisfied_quantity.0);

                        market.open_contracts =
                            market.open_contracts - ContractAmount(satisfied_quantity.0);
                    }
                }

                candlestick_data_creator
                    .add(
                        dbtx,
                        outcome,
                        other_price.try_into().unwrap_or(Amount::ZERO),
                        satisfied_quantity,
                    )
                    .await;

            // nothing satisfies
            } else {
                break;
            }
        }

        // save new order to db
        dbtx.insert_new_entry(&db::OrderKey(order_owner), &order)
            .await;
        dbtx.insert_new_entry(
            &db::OrdersByMarketKey {
                market: market_out_point,
                order: order_owner,
            },
            &(),
        )
        .await;
        if order.quantity_waiting_for_match != ContractOfOutcomeAmount::ZERO {
            dbtx.insert_new_entry(
                &db::OrderPriceTimePriorityKey::from_market_and_order(&market, &order),
                &order_owner,
            )
            .await
        }

        // save market if changed
        if market.open_contracts != beginning_market_open_contracts {
            dbtx.insert_entry(&db::MarketKey(market_out_point), &market)
                .await;
        }

        // save order cache
        order_cache.save(dbtx).await;

        // save candlestick
        candlestick_data_creator.save(dbtx).await;
    }

    // Note: the quantity returned can be lower than the actual quantity
    // at a given price.
    async fn get_outcome_side_best_price_quantity(
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        order_cache: &mut OrderCache,
        price_quantity_cache: &mut PriceQuantityCache,
        market: &OutPoint,
        outcome: &Outcome,
        side: &Side,
    ) -> Option<(Amount, ContractOfOutcomeAmount)> {
        // This setting is used to adjust the max number of orders used
        // to create the returning price_quantity. This setting is here to
        // fix a possible dos attack where the attacker could create many
        // orders at the same price in order to make this logic run
        // very slow.
        const MAX_ORDERS_QUERIED: usize = 4;

        if let Some(price_quantity) = price_quantity_cache
            .m
            .get(&(outcome.to_owned(), side.to_owned()))
        {
            return Some(price_quantity.to_owned());
        }

        let mut order_price_time_priority_stream = dbtx
            .find_by_prefix(&db::OrderPriceTimePriorityPrefix3 {
                market: market.clone(),
                outcome: outcome.clone(),
                side: side.clone(),
            })
            .await;

        let mut top_price_priority_orders = vec![];
        let first_price_priority = match order_price_time_priority_stream.next().await {
            Some((key, order)) => {
                top_price_priority_orders.push(order);
                key.price_priority
            }
            None => {
                return None;
            }
        };

        for _ in 1..MAX_ORDERS_QUERIED {
            match order_price_time_priority_stream.next().await {
                Some((key, order)) => {
                    if key.price_priority == first_price_priority {
                        top_price_priority_orders.push(order)
                    } else {
                        break;
                    }
                }
                None => break,
            }
        }

        drop(order_price_time_priority_stream);

        let mut price = Amount::ZERO;
        let mut quantity = ContractOfOutcomeAmount::ZERO;
        for order_owner in top_price_priority_orders {
            let order = order_cache.get(dbtx, &order_owner).await;

            price = order.price;
            quantity = quantity + order.quantity_waiting_for_match;
        }

        price_quantity_cache
            .m
            .insert((outcome.to_owned(), side.to_owned()), (price, quantity));

        // Weird issue here where not having to_owned() shows error in ide but still compiles as expected.
        Some((price, quantity).to_owned())
    }

    async fn get_other_outcome_sides_best_price_quantity(
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        order_cache: &mut OrderCache,
        price_quantity_cache: &mut PriceQuantityCache,
        market_out_point: &OutPoint,
        market: &Market,
        outcome: &Outcome,
        side: &Side,
    ) -> Option<(SignedAmount, ContractOfOutcomeAmount)> {
        let mut price = SignedAmount::from(market.contract_price);
        let mut quantity = ContractOfOutcomeAmount(u64::MAX);

        for i in 0..market.outcomes {
            if &i != outcome {
                match Self::get_outcome_side_best_price_quantity(
                    dbtx,
                    order_cache,
                    price_quantity_cache,
                    market_out_point,
                    &i,
                    &side,
                )
                .await
                {
                    Some((outcome_side_price, outcome_side_quantity)) => {
                        price = price - outcome_side_price.into();
                        quantity = quantity.min(outcome_side_quantity);
                    }
                    None => return None,
                }
            }
        }

        Some((price, quantity))
    }

    async fn process_market_quantity(
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        order_cache: &mut OrderCache,
        candlestick_data_creator: &mut CandlestickDataCreator,
        market_out_point: &OutPoint,
        market: &Market,
        outcome: &Outcome,
        side: &Side,
        quantity: ContractOfOutcomeAmount,
    ) {
        let mut price = None;
        let mut quantity_remaining = quantity;

        while quantity_remaining > ContractOfOutcomeAmount::ZERO {
            let (_, order_owner) = dbtx
                .find_by_prefix(&db::OrderPriceTimePriorityPrefix3 {
                    market: market_out_point.to_owned(),
                    outcome: outcome.to_owned(),
                    side: side.to_owned(),
                })
                .await
                .next()
                .await
                .expect("should always produce order");

            let order = order_cache.get_mut(dbtx, &order_owner).await;
            let satisfied_quantity = order.quantity_waiting_for_match.min(quantity);

            order.quantity_waiting_for_match =
                order.quantity_waiting_for_match - satisfied_quantity;
            quantity_remaining = quantity_remaining - satisfied_quantity;

            match side {
                Side::Buy => {
                    order.contract_of_outcome_balance =
                        order.contract_of_outcome_balance + satisfied_quantity;

                    order.bitcoin_cost =
                        order.bitcoin_cost + SignedAmount::from(order.price * satisfied_quantity.0);
                }
                Side::Sell => {
                    order.bitcoin_balance =
                        order.bitcoin_balance + (order.price * satisfied_quantity.0);

                    order.bitcoin_cost =
                        order.bitcoin_cost - SignedAmount::from(order.price * satisfied_quantity.0);
                }
            }

            if order.quantity_waiting_for_match == ContractOfOutcomeAmount::ZERO {
                dbtx.remove_entry(&db::OrderPriceTimePriorityKey::from_market_and_order(
                    &market, &order,
                ))
                .await
                .expect("should always find entry to remove");
            }

            if let None = price {
                price = Some(order.price);
            }
        }

        candlestick_data_creator
            .add(
                dbtx,
                outcome.to_owned(),
                price.expect("should always be some"),
                quantity,
            )
            .await;
    }

    async fn cancel_order(
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        order_owner: XOnlyPublicKey,
        order: &mut Order,
    ) {
        if order.quantity_waiting_for_match != ContractOfOutcomeAmount::ZERO {
            // move quantity waiting for match based on side
            match order.side {
                Side::Buy => {
                    order.bitcoin_balance =
                        order.bitcoin_balance + (order.price * order.quantity_waiting_for_match.0)
                }
                Side::Sell => {
                    order.contract_of_outcome_balance =
                        order.contract_of_outcome_balance + order.quantity_waiting_for_match
                }
            }
            order.quantity_waiting_for_match = ContractOfOutcomeAmount::ZERO;
            dbtx.insert_entry(&db::OrderKey(order_owner.to_owned()), &order)
                .await;

            let market = dbtx
                .get_value(&db::MarketKey(order.market))
                .await
                .expect("should always find market");
            dbtx.remove_entry(&db::OrderPriceTimePriorityKey::from_market_and_order(
                &market, &order,
            ))
            .await
            .expect("should always remove order");
        }
    }

    async fn get_consensus_timestamp(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
    ) -> UnixTimestamp {
        let mut peers_proposed_unix_timestamps: Vec<_> = dbtx
            .find_by_prefix(&db::PeersProposedTimestampPrefixAll)
            .await
            .map(|(_, unix_timestamp)| unix_timestamp)
            .collect()
            .await;

        peers_proposed_unix_timestamps.sort_unstable();

        let Some(i) = peers_proposed_unix_timestamps
            .len()
            .checked_sub((usize::from(self.cfg.local.peer_count) + 1) / 2)
        else {
            return UnixTimestamp::ZERO;
        };

        peers_proposed_unix_timestamps
            .get(i)
            .expect("should always be some")
            .to_owned()
    }
}

/// An in-memory cache we could use for faster validation
#[derive(Debug, Clone)]
pub struct PredictionMarketsCache;

impl fedimint_core::server::VerificationCache for PredictionMarketsCache {}

impl PredictionMarkets {
    /// Create new module instance
    pub fn new(cfg: PredictionMarketsConfig) -> PredictionMarkets {
        PredictionMarkets { cfg }
    }
}

pub struct OrderCache {
    map: HashMap<XOnlyPublicKey, Order>,
    mut_orders: HashMap<XOnlyPublicKey, ()>,
}

impl OrderCache {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            mut_orders: HashMap::new(),
        }
    }

    async fn get<'a>(
        &'a mut self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        order_owner: &XOnlyPublicKey,
    ) -> &'a Order {
        if !self.map.contains_key(order_owner) {
            let order = dbtx
                .get_value(&db::OrderKey(order_owner.to_owned()))
                .await
                .expect("OrderCache always expects order to exist");
            self.map.insert(order_owner.to_owned(), order);
        }

        self.map
            .get(order_owner)
            .expect("should always produce order")
    }

    async fn get_mut<'a>(
        &'a mut self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        order_owner: &XOnlyPublicKey,
    ) -> &'a mut Order {
        if !self.map.contains_key(order_owner) {
            let order = dbtx
                .get_value(&db::OrderKey(order_owner.to_owned()))
                .await
                .expect("OrderCache always expects order to exist");
            self.map.insert(order_owner.to_owned(), order);
        }

        self.mut_orders.insert(order_owner.to_owned(), ());

        self.map
            .get_mut(order_owner)
            .expect("should always produce order")
    }

    async fn save(self, dbtx: &mut ModuleDatabaseTransaction<'_>) {
        for (order_owner, _) in self.mut_orders {
            let order = self
                .map
                .get(&order_owner)
                .expect("should always produce order");
            dbtx.insert_entry(&db::OrderKey(order_owner), order).await;
        }
    }
}

pub struct PriceQuantityCache {
    pub m: HashMap<(Outcome, Side), (Amount, ContractOfOutcomeAmount)>,
}

impl PriceQuantityCache {
    fn new() -> Self {
        Self { m: HashMap::new() }
    }

    fn sub_outcome_side_quantity(
        &mut self,
        outcome: Outcome,
        side: Side,
        quantity: ContractOfOutcomeAmount,
    ) {
        let key = (outcome, side);

        let price_quantity = self
            .m
            .get_mut(&key)
            .expect("PriceQuantityCache: sub_outcome_side ran on None outcome side");

        price_quantity.1 = price_quantity.1 - quantity;

        if price_quantity.1 == ContractOfOutcomeAmount::ZERO {
            self.m.remove(&key);
        }
    }
}

pub struct CandlestickDataCreator {
    market: OutPoint,
    consensus_max_candlesticks_kept_per_market_outcome_interval: u64,
    consensus_timestamp: UnixTimestamp,

    candlestick_intervals: Vec<(
        // candlestick interval
        Seconds,
        // outcome to candlstick
        HashMap<Outcome, Candlestick>,
    )>,
}

impl CandlestickDataCreator {
    fn new(
        consensus_candlestick_intervals: &Vec<Seconds>,
        consensus_max_candlesticks_kept_per_market_outcome_interval: u64,
        consensus_timestamp: UnixTimestamp,
        market: OutPoint,
    ) -> Self {
        Self {
            market,
            consensus_max_candlesticks_kept_per_market_outcome_interval,
            consensus_timestamp,

            candlestick_intervals: consensus_candlestick_intervals
                .iter()
                .map(|candlestick_interval_seconds| {
                    (candlestick_interval_seconds.to_owned(), HashMap::new())
                })
                .collect(),
        }
    }

    async fn add(
        &mut self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        outcome: Outcome,
        price: Amount,
        volume: ContractOfOutcomeAmount,
    ) {
        for (candlestick_interval_seconds, candlesticks_by_outcome) in
            self.candlestick_intervals.iter_mut()
        {
            let candlestick_timestamp = self
                .consensus_timestamp
                .round_down(candlestick_interval_seconds.to_owned());

            if !candlesticks_by_outcome.contains_key(&outcome) {
                let candlestick_in_db_or_new = dbtx
                    .get_value(&db::MarketOutcomeCandlesticksKey {
                        market: self.market,
                        outcome,
                        candlestick_interval: candlestick_interval_seconds.to_owned(),
                        candlestick_timestamp,
                    })
                    .await
                    .unwrap_or(Candlestick {
                        open: price,
                        close: price,
                        high: price,
                        low: price,
                        volume: ContractOfOutcomeAmount::ZERO,
                    });

                candlesticks_by_outcome.insert(outcome, candlestick_in_db_or_new);
            }

            let candlestick = candlesticks_by_outcome
                .get_mut(&outcome)
                .expect("should always produce candlestick");

            candlestick.close = price;
            candlestick.high = Amount::from_msats(candlestick.high.msats.max(price.msats));
            candlestick.low = Amount::from_msats(candlestick.low.msats.min(price.msats));
            candlestick.volume = candlestick.volume + volume;
        }
    }

    async fn save(mut self, dbtx: &mut ModuleDatabaseTransaction<'_>) {
        self.clean(dbtx).await;

        for (candlestick_interval, candlesticks_by_outcome) in self.candlestick_intervals {
            let candlestick_timestamp = self
                .consensus_timestamp
                .round_down(candlestick_interval.to_owned());

            for (outcome, candlestick) in candlesticks_by_outcome {
                dbtx.insert_entry(
                    &db::MarketOutcomeCandlesticksKey {
                        market: self.market,
                        outcome,
                        candlestick_interval,
                        candlestick_timestamp,
                    },
                    &candlestick,
                )
                .await;
            }
        }
    }

    async fn clean(&mut self, dbtx: &mut ModuleDatabaseTransaction<'_>) {
        for (candlestick_interval, candlesticks_by_outcome) in self.candlestick_intervals.iter() {
            let candlestick_timestamp = self
                .consensus_timestamp
                .round_down(candlestick_interval.to_owned());

            let min_candlestick_timestamp = UnixTimestamp(
                candlestick_timestamp.0
                    - (candlestick_interval
                        * self.consensus_max_candlesticks_kept_per_market_outcome_interval),
            );

            for (outcome, _) in candlesticks_by_outcome.iter() {
                let mut stream = dbtx
                    .find_by_prefix(&db::MarketOutcomeCandlesticksPrefix3 {
                        market: self.market,
                        outcome: outcome.to_owned(),
                        candlestick_interval: candlestick_interval.to_owned(),
                    })
                    .await;

                let mut keys_to_remove = vec![];
                loop {
                    let Some((key, _)) = stream.next().await else {
                        break;
                    };

                    if key.candlestick_timestamp < min_candlestick_timestamp {
                        keys_to_remove.push(key);
                    } else {
                        break;
                    }
                }

                drop(stream);

                for key in keys_to_remove {
                    dbtx.remove_entry(&key)
                        .await
                        .expect("should always be some");
                }
            }
        }
    }
}
