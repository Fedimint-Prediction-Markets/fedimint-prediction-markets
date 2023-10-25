use std::collections::{BTreeMap, HashMap};

use std::string::ToString;

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
pub use fedimint_dummy_common::config::{
    PredictionMarketsClientConfig, PredictionMarketsConfig, PredictionMarketsConfigConsensus,
    PredictionMarketsConfigLocal, PredictionMarketsConfigPrivate, PredictionMarketsGenParams,
};
use fedimint_dummy_common::{
    ContractAmount, ContractSource, Market, Order, Outcome, Side, SignedAmount, TimePriority,
};
pub use fedimint_dummy_common::{
    PredictionMarketsCommonGen, PredictionMarketsConsensusItem, PredictionMarketsError,
    PredictionMarketsInput, PredictionMarketsModuleTypes, PredictionMarketsOutput,
    PredictionMarketsOutputOutcome, CONSENSUS_VERSION, KIND,
};
use futures::StreamExt;

use secp256k1::XOnlyPublicKey;

use strum::IntoEnumIterator;

use tokio::sync::Notify;

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
                        examples: "test".to_owned(),
                    },
                    private: PredictionMarketsConfigPrivate {
                        example: "test".to_owned(),
                    },
                    consensus: PredictionMarketsConfigConsensus {
                        new_market_fee: params.consensus.new_market_fee,
                        new_order_fee: params.consensus.new_order_fee,
                        max_contract_value: params.consensus.max_contract_value,
                        max_order_quantity: params.consensus.max_order_quantity,
                    },
                };
                (peer, config.to_erased())
            })
            .collect()
    }

    /// Generates configs for all peers in an untrusted manner
    async fn distributed_gen(
        &self,
        _peers: &PeerHandle,
        params: &ConfigGenModuleParams,
    ) -> DkgResult<ServerModuleConfig> {
        let params = self.parse_params(params).unwrap();

        Ok(PredictionMarketsConfig {
            local: PredictionMarketsConfigLocal {
                examples: "test".to_owned(),
            },
            private: PredictionMarketsConfigPrivate {
                example: "test".to_owned(),
            },
            consensus: PredictionMarketsConfigConsensus {
                new_market_fee: params.consensus.new_market_fee,
                new_order_fee: params.consensus.new_order_fee,
                max_contract_value: params.consensus.max_contract_value,
                max_order_quantity: params.consensus.max_order_quantity,
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
            max_contract_value: config.max_contract_value,
            max_order_quantity: config.max_order_quantity,
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
                DbKeyPrefix::NextOrderTimePriority => {
                    push_db_pair_items!(
                        dbtx,
                        db::NextOrderTimePriorityPrefixAll,
                        db::NextOrderTimePriorityKey,
                        TimePriority,
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
            }
        }

        Box::new(items.into_iter())
    }
}

/// Dummy module
#[derive(Debug)]
pub struct PredictionMarkets {
    pub cfg: PredictionMarketsConfig,

    /// Notifies us to propose an epoch
    pub propose_consensus: Notify,
}

/// Implementation of consensus for the server module
#[async_trait]
impl ServerModule for PredictionMarkets {
    /// Define the consensus types
    type Common = PredictionMarketsModuleTypes;
    type Gen = PredictionMarketsGen;
    type VerificationCache = PredictionMarketsCache;

    async fn await_consensus_proposal(&self, dbtx: &mut ModuleDatabaseTransaction<'_>) {
        // Wait until we have a proposal
        if !self.consensus_proposal(dbtx).await.forces_new_epoch() {
            self.propose_consensus.notified().await;
        }
    }

    async fn consensus_proposal(
        &self,
        _dbtx: &mut ModuleDatabaseTransaction<'_>,
    ) -> ConsensusProposal<PredictionMarketsConsensusItem> {
        ConsensusProposal::new_auto_trigger(vec![])
    }

    async fn process_consensus_item<'a, 'b>(
        &'a self,
        _dbtx: &mut ModuleDatabaseTransaction<'b>,
        _consensus_item: PredictionMarketsConsensusItem,
        _peer_id: PeerId,
    ) -> anyhow::Result<()> {
        bail!("currently no consensus items")
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
                let market = match dbtx
                    .get_value(&db::MarketKey(market_out_point.to_owned()))
                    .await
                {
                    Some(m) => m,
                    None => {
                        return Err(PredictionMarketsError::MarketDoesNotExist)
                            .into_module_error_other()
                    }
                };

                if market.payout.is_some() {
                    return Err(PredictionMarketsError::MarketFinished).into_module_error_other();
                }

                // get quantity from sources, verifying public keys of sources
                let quantity;
                (quantity, pub_keys) =
                    match Self::process_contract_sources(dbtx, sources, market_out_point, outcome)
                        .await
                    {
                        Ok(v) => v,
                        Err(_) => {
                            return Err(PredictionMarketsError::OrderValidationFailed)
                                .into_module_error_other()
                        }
                    };

                // verify order params
                if !self.validate_order_params(&market, outcome, price, &quantity) {
                    return Err(PredictionMarketsError::OrderValidationFailed)
                        .into_module_error_other();
                }

                amount = Amount::ZERO;
                fee = self.cfg.consensus.new_order_fee;

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
            PredictionMarketsInput::ConsumeOrderFreeBalance {
                order: order_owner,
                amount: amount_to_free,
            } => {
                let mut order = match dbtx.get_value(&db::OrderKey(order_owner.to_owned())).await {
                    Some(v) => v,
                    None => {
                        return Err(PredictionMarketsError::OrderDoesNotExist)
                            .into_module_error_other()
                    }
                };

                if &order.btc_balance < amount_to_free {
                    return Err(PredictionMarketsError::NotEnoughFunds).into_module_error_other();
                }

                amount = amount_to_free.to_owned();
                fee = Amount::ZERO;
                pub_keys = vec![order_owner.to_owned()];

                order.btc_balance = order.btc_balance - amount_to_free.to_owned();
                dbtx.insert_entry(&db::OrderKey(order_owner.to_owned()), &order)
                    .await;
            }
            PredictionMarketsInput::CancelOrder { order: order_owner } => {
                let mut order = match dbtx.get_value(&db::OrderKey(order_owner.to_owned())).await {
                    Some(v) => v,
                    None => {
                        return Err(PredictionMarketsError::OrderDoesNotExist)
                            .into_module_error_other()
                    }
                };

                if order.quantity_waiting_for_match == ContractAmount::ZERO {
                    return Err(PredictionMarketsError::OrderAlreadyFinished)
                        .into_module_error_other();
                }

                Self::cancel_order(dbtx, order_owner.to_owned(), &mut order).await;

                amount = Amount::ZERO;
                fee = Amount::ZERO;
                pub_keys = vec![order_owner.to_owned()];
            }
            PredictionMarketsInput::PayoutMarket {
                market: market_out_point,
                payout,
            } => {
                // get market
                let mut market = match dbtx
                    .get_value(&db::MarketKey(market_out_point.to_owned()))
                    .await
                {
                    Some(m) => m,
                    None => {
                        return Err(PredictionMarketsError::MarketDoesNotExist)
                            .into_module_error_other()
                    }
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
                    || market.outcomes != payout.outcome_payouts.len() as Outcome
                {
                    return Err(PredictionMarketsError::PayoutValidationFailed)
                        .into_module_error_other();
                }

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

                    if order.quantity_waiting_for_match != ContractAmount::ZERO {
                        Self::cancel_order(dbtx, order_owner, &mut order).await;
                    }

                    let payout_per_quantity = payout
                        .outcome_payouts
                        .get(order.outcome as usize)
                        .expect("should be some");
                    order.btc_balance = order.btc_balance
                        + (payout_per_quantity.to_owned() * order.contract_balance.0);
                    order.contract_balance = ContractAmount::ZERO;

                    dbtx.insert_entry(&db::OrderKey(order_owner), &order).await;
                }

                // save payout to market
                market.payout = Some(payout.to_owned());
                dbtx.insert_new_entry(&db::MarketKey(market_out_point.to_owned()), &market)
                    .await;

                amount = Amount::ZERO;
                fee = Amount::ZERO;
                pub_keys = vec![market.outcome_control];
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
                if contract_price > &self.cfg.consensus.max_contract_value || outcomes < &2 {
                    return Err(PredictionMarketsError::MarketValidationFailed)
                        .into_module_error_other();
                }

                // --- init market ---

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
                    &db::NextOrderTimePriorityKey { market: out_point },
                    &TimePriority(0),
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

                // save outcome
                dbtx.insert_new_entry(
                    &db::OutcomeKey(out_point),
                    &PredictionMarketsOutputOutcome::NewMarket,
                )
                .await;

                amount = Amount::ZERO;
                fee = self.cfg.consensus.new_market_fee;
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
                let market = match dbtx
                    .get_value(&db::MarketKey(market_out_point.to_owned()))
                    .await
                {
                    Some(m) => m,
                    None => {
                        return Err(PredictionMarketsError::MarketDoesNotExist)
                            .into_module_error_other()
                    }
                };

                if market.payout.is_some() {
                    return Err(PredictionMarketsError::MarketFinished).into_module_error_other();
                }

                if !self.validate_order_params(&market, outcome, price, quantity) {
                    return Err(PredictionMarketsError::OrderValidationFailed)
                        .into_module_error_other();
                }

                amount = price.to_owned() * quantity.0;
                fee = self.cfg.consensus.new_order_fee;

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

                // save outcome
                dbtx.insert_new_entry(
                    &db::OutcomeKey(out_point),
                    &PredictionMarketsOutputOutcome::NewBuyOrder,
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
                async |module: &PredictionMarkets, context, market: OutPoint| -> Market {
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
    ) -> Result<Market, ApiError> {
        match dbtx.get_value(&db::MarketKey(market)).await {
            Some(val) => return Ok(val),
            None => return Err(ApiError::not_found("market not found".into())),
        };
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
    async fn get_next_order_time_priority(
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        market: OutPoint,
    ) -> TimePriority {
        let current = dbtx
            .get_value(&db::NextOrderTimePriorityKey { market })
            .await
            .expect("should always produce value");

        // increment
        dbtx.insert_entry(
            &db::NextOrderTimePriorityKey { market },
            &TimePriority(current.0 + 1),
        )
        .await;

        current
    }

    async fn process_contract_sources(
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        sources: &Vec<ContractSource>,
        market: &OutPoint,
        outcome: &Outcome,
    ) -> Result<(ContractAmount, Vec<XOnlyPublicKey>), ()> {
        let mut total_amount = ContractAmount(0);
        let mut order_public_keys = Vec::new();

        let mut duplicate_check = HashMap::new();

        for ContractSource {
            order: order_owner,
            amount,
        } in sources
        {
            // check for duplicate orders in sources
            if let Some(_) = duplicate_check.insert(order_owner, ()) {
                return Err(());
            }

            let Some(mut order) = dbtx.get_value(&db::OrderKey(order_owner.to_owned())).await
            else {
                return Err(());
            };

            if market != &order.market
                || outcome != &order.outcome
                || &order.contract_balance < amount
                || amount <= &ContractAmount::ZERO
            {
                return Err(());
            }

            order.contract_balance = order.contract_balance - amount.to_owned();

            dbtx.insert_entry(&db::OrderKey(order_owner.to_owned()), &order)
                .await;

            total_amount = total_amount + amount.to_owned();
            order_public_keys.push(order_owner.to_owned());
        }

        Ok((total_amount, order_public_keys))
    }

    /// returns true on pass of verification
    fn validate_order_params(
        &self,
        market: &Market,
        outcome: &Outcome,
        price: &Amount,
        quantity: &ContractAmount,
    ) -> bool {
        !(outcome >= &market.outcomes
            || price <= &Amount::ZERO
            || price >= &market.contract_price
            || quantity <= &ContractAmount::ZERO
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
        quantity: ContractAmount,
    ) {
        let mut order = Order {
            market: market_out_point,
            outcome,
            side,
            price,
            original_quantity: quantity,
            time_priority: PredictionMarkets::get_next_order_time_priority(dbtx, market_out_point)
                .await,
            quantity_waiting_for_match: quantity,
            contract_balance: ContractAmount::ZERO,
            btc_balance: Amount::ZERO,
        };

        // get market
        let market = dbtx
            .get_value(&db::MarketKey(market_out_point.to_owned()))
            .await
            .expect("should always find market");

        while order.quantity_waiting_for_match > ContractAmount::ZERO {
            let own = Self::get_outcome_price_quantity(
                dbtx,
                &market_out_point,
                &outcome,
                &side.opposite(),
            )
            .await;
            let other = Self::get_other_outcomes_price_quantity(
                dbtx,
                &market_out_point,
                &market,
                &outcome,
                &side.opposite(),
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
                        order.contract_balance = order.contract_balance + satisfied_quantity
                    }
                    Side::Sell => {
                        order.btc_balance = order.btc_balance + (own_price * satisfied_quantity.0)
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
                        order.contract_balance = order.contract_balance + satisfied_quantity;

                        let unspent_collateral_per_quantity =
                            SignedAmount::from(order.price) - other_price;
                        assert_eq!(unspent_collateral_per_quantity.is_negative(), false);

                        order.btc_balance = order.btc_balance
                            + (unspent_collateral_per_quantity.amount * satisfied_quantity.0);
                    }
                    Side::Sell => {
                        assert_eq!(other_price.is_negative(), false);

                        order.btc_balance =
                            order.btc_balance + (other_price.amount * satisfied_quantity.0)
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
        if order.quantity_waiting_for_match != ContractAmount::ZERO {
            dbtx.insert_new_entry(
                &db::OrderPriceTimePriorityKey::from_order(&order, market.contract_price),
                &order_owner,
            )
            .await
        }
    }

    async fn get_outcome_price_quantity(
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        market: &OutPoint,
        outcome: &Outcome,
        side: &Side,
    ) -> Option<(Amount, ContractAmount)> {
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
        let mut quantity = ContractAmount::ZERO;
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

    async fn get_other_outcomes_price_quantity(
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        market_out_point: &OutPoint,
        market: &Market,
        outcome: &Outcome,
        side: &Side,
    ) -> Option<(SignedAmount, ContractAmount)> {
        let mut price = SignedAmount::from(market.contract_price);
        let mut quantity = ContractAmount(u64::MAX);

        for i in 0..market.outcomes {
            if &i != outcome {
                match Self::get_outcome_price_quantity(dbtx, market_out_point, &i, &side.opposite())
                    .await
                {
                    Some((outcome_price, outcome_quantity)) => {
                        price = price - outcome_price.into();
                        quantity = quantity.min(outcome_quantity);
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
        mut quantity: ContractAmount,
    ) {
        while quantity > ContractAmount::ZERO {
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

            let satisfied = order.quantity_waiting_for_match.min(quantity);

            order.quantity_waiting_for_match = order.quantity_waiting_for_match - satisfied;
            quantity = quantity - satisfied;

            match side {
                Side::Buy => order.contract_balance = order.contract_balance + satisfied,
                Side::Sell => order.btc_balance = order.btc_balance + (order.price * satisfied.0),
            }

            dbtx.insert_entry(&db::OrderKey(order_owner), &order).await;
            if order.quantity_waiting_for_match == ContractAmount::ZERO {
                dbtx.remove_entry(&db::OrderPriceTimePriorityKey::from_order(
                    &order,
                    market.contract_price,
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
        if order.quantity_waiting_for_match != ContractAmount::ZERO {
            let market = dbtx
                .get_value(&db::MarketKey(order.market))
                .await
                .expect("should always find market");

            dbtx.remove_entry(&db::OrderPriceTimePriorityKey::from_order(
                &order,
                market.contract_price,
            ))
            .await
            .expect("should always remove order");
        }

        match order.side {
            Side::Buy => {
                order.btc_balance =
                    order.btc_balance + (order.price * order.quantity_waiting_for_match.0)
            }
            Side::Sell => {
                order.contract_balance = order.contract_balance + order.quantity_waiting_for_match
            }
        }
        order.quantity_waiting_for_match = ContractAmount::ZERO;

        dbtx.insert_entry(&db::OrderKey(order_owner.to_owned()), &order)
            .await;
    }
}

/// An in-memory cache we could use for faster validation
#[derive(Debug, Clone)]
pub struct PredictionMarketsCache;

impl fedimint_core::server::VerificationCache for PredictionMarketsCache {}

impl PredictionMarkets {
    /// Create new module instance
    pub fn new(cfg: PredictionMarketsConfig) -> PredictionMarkets {
        PredictionMarkets {
            cfg,
            propose_consensus: Notify::new(),
        }
    }
}
