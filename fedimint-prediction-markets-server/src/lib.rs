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
    ContractOfOutcomeAmount, ContractOfOutcomeSource, Market, Order, Outcome, Side, SignedAmount,
    TimeOrdering, UnixTimestamp,
};
pub use fedimint_prediction_markets_common::{
    PredictionMarketsCommonGen, PredictionMarketsConsensusItem, PredictionMarketsError,
    PredictionMarketsInput, PredictionMarketsModuleTypes, PredictionMarketsOutput,
    PredictionMarketsOutputOutcome, CONSENSUS_VERSION, KIND,
};
use futures::StreamExt;

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
                        new_market_fee: params.consensus.new_market_fee,
                        max_contract_price: params.consensus.max_contract_price,
                        max_market_outcomes: params.consensus.max_market_outcomes,

                        new_order_fee: params.consensus.new_order_fee,
                        max_order_quantity: params.consensus.max_order_quantity,

                        timestamp_interval_seconds: params.consensus.timestamp_interval_seconds,
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
                new_market_fee: params.consensus.new_market_fee,
                max_contract_price: params.consensus.max_contract_price,
                max_market_outcomes: params.consensus.max_market_outcomes,

                new_order_fee: params.consensus.new_order_fee,
                max_order_quantity: params.consensus.max_order_quantity,

                timestamp_interval_seconds: params.consensus.timestamp_interval_seconds,
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
        Ok(PredictionMarketsClientConfig {
            new_market_fee: config.new_market_fee,
            new_order_fee: config.new_order_fee,
            max_contract_price: config.max_contract_price,
            max_order_quantity: config.max_order_quantity,
            max_market_outcomes: config.max_market_outcomes,
        })
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
                DbKeyPrefix::OutcomeControlMarkets => {
                    push_db_pair_items!(
                        dbtx,
                        db::OutcomeControlMarketsPrefixAll,
                        db::OutcomeControlMarketsKey,
                        (),
                        items,
                        "OutcomeControlMarkets"
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
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let next_consensus_timestamp = {
            let mut current = self.get_consensus_timestamp(dbtx).await;
            current.seconds += self.cfg.consensus.timestamp_interval_seconds;
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
            UnixTimestamp::now().round_down(self.cfg.consensus.timestamp_interval_seconds);
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
                if !new.divisible(self.cfg.consensus.timestamp_interval_seconds) {
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
                if !self.validate_order_params(&market, outcome, price, &quantity) {
                    return Err(PredictionMarketsError::OrderValidationFailed)
                        .into_module_error_other();
                }

                // set input meta
                amount = Amount::ZERO;
                fee = self.cfg.consensus.new_order_fee;
                pub_keys = source_order_public_keys;

                // process order
                self.process_new_order(
                    dbtx,
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
                fee = Amount::ZERO;
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
            PredictionMarketsInput::PayoutMarket {
                market: market_out_point,
                payout,
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

                // validate payout
                let total_payout: Amount =
                    payout.outcome_payouts.iter().map(|v| v.to_owned()).sum();
                if total_payout != market.contract_price
                    || payout.outcome_payouts.len() != usize::from(market.outcomes)
                {
                    return Err(PredictionMarketsError::PayoutValidationFailed)
                        .into_module_error_other();
                }

                // set input meta
                amount = Amount::ZERO;
                fee = Amount::ZERO;
                pub_keys = vec![market.outcome_control];

                // process payout
                let market_orders: Vec<_> = dbtx
                    .find_by_prefix(&db::OrdersByMarketPrefix1 {
                        market: market_out_point.to_owned(),
                    })
                    .await
                    .map(|(key, _)| key.order)
                    .collect()
                    .await;

                for order_owner in market_orders {
                    let mut order = dbtx
                        .get_value(&db::OrderKey(order_owner))
                        .await
                        .expect("should always find order");

                    Self::cancel_order(dbtx, order_owner, &mut order).await;

                    let payout_per_contract_of_outcome = payout
                        .outcome_payouts
                        .get(usize::from(order.outcome))
                        .expect("should be some");
                    order.bitcoin_balance = order.bitcoin_balance
                        + (payout_per_contract_of_outcome.to_owned()
                            * order.contract_of_outcome_balance.0);
                    order.contract_of_outcome_balance = ContractOfOutcomeAmount::ZERO;

                    dbtx.insert_entry(&db::OrderKey(order_owner), &order).await;
                }

                // save payout to market
                market.payout = Some(payout.to_owned());
                dbtx.insert_new_entry(&db::MarketKey(market_out_point.to_owned()), &market)
                    .await;
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
                outcome_control,
                description,
            } => {
                // verify market params
                if contract_price == &Amount::ZERO
                    || contract_price > &self.cfg.consensus.max_contract_price
                    || outcomes < &2
                    || outcomes > &self.cfg.consensus.max_market_outcomes
                {
                    return Err(PredictionMarketsError::MarketValidationFailed)
                        .into_module_error_other();
                }

                // set output meta
                amount = Amount::ZERO;
                fee = self.cfg.consensus.new_market_fee;

                // save outcome
                dbtx.insert_new_entry(
                    &db::OutcomeKey(out_point),
                    &PredictionMarketsOutputOutcome::NewMarket,
                )
                .await;

                // save market
                dbtx.insert_new_entry(
                    &db::MarketKey(out_point),
                    &Market {
                        contract_price: contract_price.to_owned(),
                        outcomes: outcomes.to_owned(),
                        outcome_control: outcome_control.to_owned(),
                        description: description.to_owned(),
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

                // OutcomeControlMarkets index insert
                dbtx.insert_new_entry(
                    &db::OutcomeControlMarketsKey {
                        outcome_control: outcome_control.to_owned(),
                        market: out_point,
                    },
                    &(),
                )
                .await;
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

                // validate order params
                if !self.validate_order_params(&market, outcome, price, quantity) {
                    return Err(PredictionMarketsError::OrderValidationFailed)
                        .into_module_error_other();
                }

                // set output meta
                amount = price.to_owned() * quantity.0;
                fee = self.cfg.consensus.new_order_fee;

                // save outcome
                dbtx.insert_new_entry(
                    &db::OutcomeKey(out_point),
                    &PredictionMarketsOutputOutcome::NewBuyOrder,
                )
                .await;

                // process order
                self.process_new_order(
                    dbtx,
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

    async fn audit(&self, _dbtx: &mut ModuleDatabaseTransaction<'_>, _audit: &mut Audit) {}

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
                "get_outcome_control_markets",
                async |module: &PredictionMarkets, context, outcome_control: XOnlyPublicKey| -> Vec<OutPoint> {
                    module.api_get_outcome_control_markets(&mut context.dbtx(), outcome_control).await
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

    async fn api_get_outcome_control_markets(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        outcome_control: XOnlyPublicKey,
    ) -> Result<Vec<OutPoint>, ApiError> {
        let result: Vec<OutPoint> = dbtx
            .find_by_prefix(&db::OutcomeControlMarketsPrefix1 { outcome_control })
            .await
            .map(|(key, _)| key.market)
            .collect()
            .await;

        Ok(result)
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
        sources: &Vec<ContractOfOutcomeSource>,
        market: &OutPoint,
        outcome: &Outcome,
    ) -> Result<(ContractOfOutcomeAmount, Vec<XOnlyPublicKey>), ()> {
        let mut total_amount = ContractOfOutcomeAmount::ZERO;
        let mut source_order_public_keys = Vec::new();

        let mut duplicate_source_check = HashMap::new();

        for ContractOfOutcomeSource {
            order: order_owner,
            quantity,
        } in sources
        {
            if let Some(_) = duplicate_source_check.insert(order_owner, ()) {
                return Err(());
            }

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

    /// returns true on pass of verification
    fn validate_order_params(
        &self,
        market: &Market,
        outcome: &Outcome,
        price: &Amount,
        quantity: &ContractOfOutcomeAmount,
    ) -> bool {
        !(outcome >= &market.outcomes
            || price == &Amount::ZERO
            || price >= &market.contract_price
            || quantity == &ContractOfOutcomeAmount::ZERO
            || quantity > &self.cfg.consensus.max_order_quantity)
    }

    async fn process_new_order(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        order_owner: XOnlyPublicKey,
        market_out_point: OutPoint,
        outcome: Outcome,
        side: Side,
        price: Amount,
        quantity: ContractOfOutcomeAmount,
    ) {
        let mut order = Order {
            market: market_out_point,
            outcome,
            side,
            price,
            original_quantity: quantity,
            time_ordering: PredictionMarkets::get_next_order_time_ordering(dbtx, market_out_point)
                .await,
            quantity_waiting_for_match: quantity,
            contract_of_outcome_balance: ContractOfOutcomeAmount::ZERO,
            bitcoin_balance: Amount::ZERO,
        };

        let market = dbtx
            .get_value(&db::MarketKey(market_out_point.to_owned()))
            .await
            .expect("should always find market");

        while order.quantity_waiting_for_match > ContractOfOutcomeAmount::ZERO {
            let own = Self::get_outcome_side_price_quantity(
                dbtx,
                &market_out_point,
                &outcome,
                &side.opposite(),
            )
            .await;
            let other = Self::get_other_outcome_sides_price_quantity(
                dbtx,
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

            // process own outcome match
            if matches_own {
                let (own_price, own_quantity) = own.expect("should always be some");
                let satisfied_quantity = order.quantity_waiting_for_match.min(own_quantity);

                Self::process_market_quantity(
                    dbtx,
                    &market_out_point,
                    &market,
                    &outcome,
                    &side.opposite(),
                    satisfied_quantity,
                )
                .await;

                order.quantity_waiting_for_match =
                    order.quantity_waiting_for_match - satisfied_quantity;

                match side {
                    Side::Buy => {
                        order.contract_of_outcome_balance =
                            order.contract_of_outcome_balance + satisfied_quantity;

                        order.bitcoin_balance = order.bitcoin_balance
                            + ((order.price - own_price) * satisfied_quantity.0);
                    }
                    Side::Sell => {
                        order.bitcoin_balance =
                            order.bitcoin_balance + (own_price * satisfied_quantity.0)
                    }
                }

            // process other outcome match
            } else if matches_other {
                let (other_price, other_quantity) = other.expect("should always be some");
                let satisfied_quantity = order.quantity_waiting_for_match.min(other_quantity);

                for i in 0..market.outcomes {
                    if i != outcome {
                        Self::process_market_quantity(
                            dbtx,
                            &market_out_point,
                            &market,
                            &i,
                            &side,
                            satisfied_quantity,
                        )
                        .await;
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
                    }
                    Side::Sell => {
                        order.bitcoin_balance = order.bitcoin_balance
                            + (Amount::try_from(other_price).expect("should always convert")
                                * satisfied_quantity.0)
                    }
                }

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
    }

    async fn get_outcome_side_price_quantity(
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        market: &OutPoint,
        outcome: &Outcome,
        side: &Side,
    ) -> Option<(Amount, ContractOfOutcomeAmount)> {
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

        loop {
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
            let order = dbtx
                .get_value(&db::OrderKey(order_owner))
                .await
                .expect("should always produce order");

            price = order.price;
            quantity = quantity + order.quantity_waiting_for_match;
        }

        Some((price, quantity))
    }

    async fn get_other_outcome_sides_price_quantity(
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        market_out_point: &OutPoint,
        market: &Market,
        outcome: &Outcome,
        side: &Side,
    ) -> Option<(SignedAmount, ContractOfOutcomeAmount)> {
        let mut price = SignedAmount::from(market.contract_price);
        let mut quantity = ContractOfOutcomeAmount(u64::MAX);

        for i in 0..market.outcomes {
            if &i != outcome {
                match Self::get_outcome_side_price_quantity(dbtx, market_out_point, &i, &side).await
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
        market_out_point: &OutPoint,
        market: &Market,
        outcome: &Outcome,
        side: &Side,
        mut quantity: ContractOfOutcomeAmount,
    ) {
        while quantity > ContractOfOutcomeAmount::ZERO {
            let (_, order_owner) = dbtx
                .find_by_prefix(&db::OrderPriceTimePriorityPrefix3 {
                    market: market_out_point.clone(),
                    outcome: outcome.clone(),
                    side: side.clone(),
                })
                .await
                .next()
                .await
                .expect("should always produce order");

            let mut order = dbtx
                .get_value(&db::OrderKey(order_owner))
                .await
                .expect("should always produce order");

            let satisfied_quantity = order.quantity_waiting_for_match.min(quantity);

            order.quantity_waiting_for_match =
                order.quantity_waiting_for_match - satisfied_quantity;
            quantity = quantity - satisfied_quantity;

            match side {
                Side::Buy => {
                    order.contract_of_outcome_balance =
                        order.contract_of_outcome_balance + satisfied_quantity
                }
                Side::Sell => {
                    order.bitcoin_balance =
                        order.bitcoin_balance + (order.price * satisfied_quantity.0)
                }
            }

            dbtx.insert_entry(&db::OrderKey(order_owner), &order).await;

            if order.quantity_waiting_for_match == ContractOfOutcomeAmount::ZERO {
                dbtx.remove_entry(&db::OrderPriceTimePriorityKey::from_market_and_order(
                    &market, &order,
                ))
                .await
                .expect("should always find entry to remove");
            }
        }
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
