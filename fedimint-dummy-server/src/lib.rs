use std::collections::{BTreeMap, HashMap};

use std::string::ToString;

use anyhow::bail;
use async_trait::async_trait;

use db::DbKeyPrefix;
use fedimint_core::config::{
    ConfigGenModuleParams, DkgResult, ServerModuleConfig, ServerModuleConsensusConfig,
    TypedServerModuleConfig, TypedServerModuleConsensusConfig,
};
use fedimint_core::db::{
    AutocommitError, Database, DatabaseVersion, MigrationMap, ModuleDatabaseTransaction,
};

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
    ContractAmount, ContractSource, Market, MarketDescription, Order, OutcomeSize, Payout, Side,
    TimePriority,
};
pub use fedimint_dummy_common::{
    PredictionMarketsCommonGen, PredictionMarketsConsensusItem, PredictionMarketsError,
    PredictionMarketsInput, PredictionMarketsModuleTypes, PredictionMarketsOutput,
    PredictionMarketsOutputOutcome, CONSENSUS_VERSION, KIND,
};
use futures::{future, StreamExt};

use secp256k1::schnorr::Signature;
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
                        OutPoint,
                        items,
                        "OrdersByMarket"
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
        let mut amount = Amount::ZERO;
        let mut fee = Amount::ZERO;
        let mut pub_keys = vec![];

        match input {
            PredictionMarketsInput::NewSellOrder {
                owner,
                market: market_out_point,
                outcome,
                price,
                sources,
            } => {
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

                // get quantity from sources
                let quantity;
                (quantity, pub_keys) = match Self::process_contract_sources(dbtx, sources).await {
                    Ok(v) => v,
                    Err(_) => {
                        return Err(PredictionMarketsError::FailedNewOrderValidation)
                            .into_module_error_other()
                    }
                };

                // verify order params
                if !self.validate_order_params(&market, outcome, price, &quantity) {
                    return Err(PredictionMarketsError::FailedNewOrderValidation)
                        .into_module_error_other();
                }
            }
            PredictionMarketsInput::ConsumeOrderFreeBalance { order } => {}
            PredictionMarketsInput::CancelOrder { order } => {}
            PredictionMarketsInput::PayoutMarket { market, payout } => {}
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
        let mut amount = Amount::ZERO;
        let mut fee = Amount::ZERO;

        match output {
            PredictionMarketsOutput::NewMarket {
                contract_price,
                outcomes,
                outcome_control,
                description,
            } => {
                // verify market params
                if contract_price > &self.cfg.consensus.max_contract_value {
                    return Err(PredictionMarketsError::FailedNewMarketValidation)
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

                // save outcome
                dbtx.insert_new_entry(
                    &db::OutcomeKey(out_point),
                    &PredictionMarketsOutputOutcome::NewMarket,
                )
                .await;

                // set new market fee
                fee = self.cfg.consensus.new_market_fee;
            }
            PredictionMarketsOutput::NewBuyOrder {
                owner,
                market: market_out_point,
                outcome,
                price,
                quantity,
            } => {
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

                if !self.validate_order_params(&market, outcome, price, quantity) {
                    return Err(PredictionMarketsError::FailedNewOrderValidation)
                        .into_module_error_other();
                }

                // create order
                let order = Order {
                    owner: owner.to_owned(),
                    outcome: outcome.to_owned(),
                    side: Side::Buy,
                    price: price.to_owned(),
                    original_quantity: quantity.to_owned(),
                    time_priority: Self::get_next_order_time_priority(
                        dbtx,
                        market_out_point.to_owned(),
                    )
                    .await,
                    quantity_waiting_for_match: quantity.to_owned(),
                    contract_balance: ContractAmount(0),
                    btc_balance: Amount::ZERO,
                };

                // set amount and fee
                amount = order.price * order.quantity_waiting_for_match.0;
                fee = self.cfg.consensus.new_order_fee;

                // save order
                dbtx.insert_new_entry(&db::OrderKey(out_point.to_owned()), &order)
                    .await;

                // process order in orderbook
            }
            PredictionMarketsOutput::PayoutMarket(payout, signature) => {
                // check if payout already exists for market
                if let Some(_) = dbtx
                    .get_value(&OddsMarketsPayoutKey {
                        market: payout.market,
                    })
                    .await
                {
                    return Err(PredictionMarketsError::PayoutAlreadyExists)
                        .into_module_error_other();
                }

                // get market
                let market = match dbtx
                    .get_value(&MarketKey {
                        market: payout.market,
                    })
                    .await
                {
                    Some(val) => val,
                    None => {
                        return Err(PredictionMarketsError::MarketDoesNotExist)
                            .into_module_error_other()
                    }
                };

                // validate payout parameters
                if payout
                    .outcome_payouts
                    .iter()
                    .map(|a| a.to_owned())
                    .sum::<Amount>()
                    != market.contract_price
                {
                    return Err(PredictionMarketsError::FailedPayoutValidation)
                        .into_module_error_other();
                }

                // validate payout signature
                if let Err(_) = payout.verify_schnorr(&market.outcome_control, signature) {
                    return Err(PredictionMarketsError::FailedPayoutValidation)
                        .into_module_error_other();
                }

                // process payout
                // TODO

                // save payout
                dbtx.insert_new_entry(
                    &OddsMarketsPayoutKey {
                        market: payout.market,
                    },
                    &(payout.to_owned(), signature.to_owned()),
                )
                .await;

                // save outcome status
                dbtx.insert_new_entry(
                    &OutputToOutcomeStatusKey(out_point),
                    &PredictionMarketsOutputOutcome::PayoutMarket,
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
        dbtx.get_value(&OutputToOutcomeStatusKey(out_point)).await
    }

    async fn audit(&self, _dbtx: &mut ModuleDatabaseTransaction<'_>, _audit: &mut Audit) {}

    fn api_endpoints(&self) -> Vec<ApiEndpoint<Self>> {
        vec![
            api_endpoint! {
                "get_market",
                async |module: &PredictionMarkets, context, out_point: OutPoint| -> Market {
                    module.handle_get_market(&mut context.dbtx(), out_point).await
                }
            },
            api_endpoint! {
                "get_order",
                async |module: &PredictionMarkets, context, out_point: OutPoint| -> Order {
                    module.handle_get_order(&mut context.dbtx(), out_point).await
                }
            },
        ]
    }
}

impl PredictionMarkets {
    async fn handle_get_market(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        market: OutPoint,
    ) -> Result<Market, ApiError> {
        match dbtx.get_value(&db::MarketKey(market)).await {
            Some(val) => return Ok(val),
            None => return Err(ApiError::not_found("market not found".into())),
        };
    }

    async fn handle_get_order(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        order: OutPoint,
    ) -> Result<Order, ApiError> {
        match dbtx.get_value(&db::OrderKey(order)).await {
            Some(val) => return Ok(val),
            None => return Err(ApiError::not_found("order not found".into())),
        };
    }

    async fn get_next_order_time_priority(
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        market: OutPoint,
    ) -> TimePriority {
        let current = dbtx
            .get_value(&db::NextOrderTimePriorityKey { market })
            .await
            .expect("should always produce value");

        // increment
        dbtx.insert_new_entry(
            &db::NextOrderTimePriorityKey { market },
            &TimePriority(current.0 + 1),
        )
        .await;

        current
    }

    async fn process_contract_sources(
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        sources: &Vec<ContractSource>,
    ) -> Result<(ContractAmount, Vec<XOnlyPublicKey>), ()> {
        let mut total_amount = ContractAmount(0);
        let mut order_public_keys = Vec::new();

        let mut duplicate_check = HashMap::new();

        for ContractSource {
            order: order_out_point,
            amount,
        } in sources
        {
            // check for duplicate orders in sources
            if let Some(_) = duplicate_check.insert(order_out_point, ()) {
                return Err(());
            }

            let mut order = match dbtx
                .get_value(&db::OrderKey(order_out_point.to_owned()))
                .await
            {
                Some(v) => v,
                None => return Err(()),
            };

            if &order.contract_balance < amount || amount <= &ContractAmount::ZERO {
                return Err(());
            }

            order.contract_balance = order.contract_balance - amount.to_owned();

            dbtx.insert_new_entry(&db::OrderKey(order_out_point.to_owned()), &order)
                .await;

            total_amount = total_amount + amount.to_owned();
            order_public_keys.push(order.owner)
        }

        Ok((total_amount, order_public_keys))
    }

    // returns true on success
    pub fn validate_order_params(
        &self,
        market: &Market,
        outcome: &OutcomeSize,
        price: &Amount,
        quantity: &ContractAmount,
    ) -> bool {
        !(outcome >= &market.outcomes
            || price <= &Amount::ZERO
            || price >= &market.contract_price
            || quantity <= &ContractAmount(0)
            || quantity > &self.cfg.consensus.max_order_quantity)
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
