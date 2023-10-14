use std::collections::BTreeMap;

use std::string::ToString;

use anyhow::bail;
use async_trait::async_trait;

use db::{
    DbKeyPrefix, OddsMarketsMarketKey, OddsMarketsMarketPrefix, OddsMarketsNextOrderPriorityKey,
    OddsMarketsNextOrderPriorityPrefix, OddsMarketsOrderKey, OddsMarketsOrderPrefixAll,
    OddsMarketsOrderPrefixMarket, OddsMarketsOutcomeKey, OddsMarketsOutcomePrefix,
    OddsMarketsPayoutKey, OddsMarketsPayoutPrefix,
};
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
    PredictionMarketsClientConfig, PredictionMarketsConfig, PredictionMarketsConfigConsensus, PredictionMarketsConfigLocal,
    PredictionMarketsConfigPrivate, PredictionMarketsGenParams,
};
use fedimint_dummy_common::{Market, MarketDescription, Order, Payout, Side};
pub use fedimint_dummy_common::{
    PredictionMarketsCommonGen, PredictionMarketsConsensusItem, PredictionMarketsError, PredictionMarketsInput,
    PredictionMarketsModuleTypes, PredictionMarketsOutput, PredictionMarketsOutputOutcome, CONSENSUS_VERSION, KIND,
};
use futures::{future, StreamExt};

use rust_decimal::Decimal;
use rust_pie_ob::PieOrderBook;
use secp256k1::schnorr::Signature;
use secp256k1::XOnlyPublicKey;

use strum::IntoEnumIterator;

use tokio::sync::Notify;

mod db;

/// Generates the module
#[derive(Debug, Clone)]
pub struct OddsMarketsGen;

// TODO: Boilerplate-code
impl ExtendsCommonModuleInit for OddsMarketsGen {
    type Common = PredictionMarketsCommonGen;
}

/// Implementation of server module non-consensus functions
#[async_trait]
impl ServerModuleInit for OddsMarketsGen {
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
        Ok(OddsMarkets::new(cfg.to_typed()?).into())
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
                        OddsMarketsOutcomePrefix,
                        OddsMarketsOutPointKey,
                        PredictionMarketsOutputOutcome,
                        items,
                        "Output Outcomes"
                    );
                }
                DbKeyPrefix::Market => {
                    push_db_pair_items!(
                        dbtx,
                        OddsMarketsMarketPrefix,
                        OddsMarketsMarketKey,
                        Market,
                        items,
                        "Markets"
                    );
                }
                DbKeyPrefix::Order => {
                    push_db_pair_items!(
                        dbtx,
                        OddsMarketsOrderPrefixAll,
                        OddsMarketsOrderKey,
                        Order,
                        items,
                        "Markets"
                    );
                }
                DbKeyPrefix::Payout => {
                    push_db_pair_items!(
                        dbtx,
                        OddsMarketsPayoutPrefix,
                        OddsMarketsPayoutKey,
                        (Payout, Signature),
                        items,
                        "Payouts"
                    );
                }
                DbKeyPrefix::NextOrderPriority => {
                    push_db_pair_items!(
                        dbtx,
                        OddsMarketsNextOrderPriorityPrefix,
                        OddsMarketsNextOrderPriorityKey,
                        u64,
                        items,
                        "NextOrderPriority"
                    );
                }
            }
        }

        Box::new(items.into_iter())
    }
}

/// Dummy module
#[derive(Debug)]
pub struct OddsMarkets {
    pub cfg: PredictionMarketsConfig,

    /// Notifies us to propose an epoch
    pub propose_consensus: Notify,
}

/// Implementation of consensus for the server module
#[async_trait]
impl ServerModule for OddsMarkets {
    /// Define the consensus types
    type Common = PredictionMarketsModuleTypes;
    type Gen = OddsMarketsGen;
    type VerificationCache = OddsMarketsCache;

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
        OddsMarketsCache
    }

    async fn process_input<'a, 'b, 'c>(
        &'a self,
        _dbtx: &mut ModuleDatabaseTransaction<'c>,
        _input: &'b PredictionMarketsInput,
        _cache: &Self::VerificationCache,
    ) -> Result<InputMeta, ModuleError> {
        Ok(InputMeta {
            amount: TransactionItemAmount {
                amount: Amount::ZERO,
                fee: Amount::ZERO,
            },
            // IMPORTANT: include the pubkey to validate the user signed this tx
            pub_keys: vec![],
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
            PredictionMarketsOutput::NewMarket(market) => {
                // verify market params
                if market.contract_price > self.cfg.consensus.max_contract_value {
                    return Err(PredictionMarketsError::FailedNewMarketValidation)
                        .into_module_error_other();
                }

                // set new market fee
                fee = self.cfg.consensus.new_market_fee;

                // save market
                dbtx.insert_new_entry(&OddsMarketsMarketKey { market: out_point }, market)
                    .await;

                // save starting next order priority
                dbtx.insert_new_entry(&OddsMarketsNextOrderPriorityKey { market: out_point }, &0)
                    .await;

                // save outcome status
                dbtx.insert_new_entry(
                    &OddsMarketsOutcomeKey(out_point),
                    &PredictionMarketsOutputOutcome::NewMarket,
                )
                .await;
            }
            PredictionMarketsOutput::NewOrder {
                owner,
                market_outpoint,
                outcome,
                side,
                price,
                quantity,
            } => {
                // get market
                let market = match dbtx
                    .get_value(&OddsMarketsMarketKey {
                        market: market_outpoint.to_owned(),
                    })
                    .await
                {
                    Some(m) => m,
                    None => {
                        return Err(PredictionMarketsError::MarketDoesNotExist).into_module_error_other()
                    }
                };

                // verify order params
                if outcome >= &market.outcomes
                    || price <= &Amount::ZERO
                    || price >= &market.contract_price
                    || quantity <= &self.cfg.consensus.max_order_quantity
                {
                    return Err(PredictionMarketsError::FailedNewOrderValidation)
                        .into_module_error_other();
                }

                // generate rust_pie_ob from market orders
                let mut pie_order_book: PieOrderBook<OutPoint> = PieOrderBook::new(
                    Decimal::from(market.contract_price.msats),
                    market.outcomes.into(),
                );

                let mut market_existing_orders: Vec<_> = dbtx
                    .find_by_prefix(&OddsMarketsOrderPrefixMarket {
                        market: market_outpoint.to_owned(),
                    })
                    .await
                    .filter(|order_entry| future::ready(order_entry.1.quantity_remaining != 0))
                    .collect()
                    .await;

                market_existing_orders.sort_by(|order_entry1, order_entry2| {
                    order_entry1
                        .1
                        .time_priority
                        .cmp(&order_entry2.1.time_priority)
                });

                for (key, entry) in market_existing_orders {
                    assert_eq!(
                        pie_order_book
                            .process_limit_order(
                                key.order,
                                usize::from(entry.outcome),
                                side.into(),
                                Decimal::from(entry.price.msats),
                                Decimal::from(entry.quantity_remaining)
                            )
                            .unwrap()
                            .len(),
                        0
                    );
                }

                // create order
                let time_priority = {
                    let p = dbtx
                        .get_value(&OddsMarketsNextOrderPriorityKey {
                            market: market_outpoint.to_owned(),
                        })
                        .await
                        .expect("should always be Some");
                    dbtx.insert_new_entry(
                        &OddsMarketsNextOrderPriorityKey {
                            market: market_outpoint.to_owned(),
                        },
                        &(p + 1),
                    )
                    .await;
                    p
                };

                let order = Order {
                    owner: owner.to_owned(),
                    outcome: outcome.to_owned(),
                    side: side.to_owned(),
                    price: price.to_owned(),
                    time_priority,
                    quantity_remaining: quantity.to_owned(),
                    quantity_balance: 0,
                    btc_balance: Amount::ZERO,
                };

                // set amount and fee
                if let Side::Buy = side {
                    amount = order.price * order.quantity_remaining
                }
                fee = self.cfg.consensus.new_order_fee;

                // save order
                dbtx.insert_new_entry(
                    &OddsMarketsOrderKey {
                        market: market_outpoint.to_owned(),
                        order: out_point.to_owned(),
                    },
                    &order,
                )
                .await;

                // process order in orderbook
                let order_match_vec = pie_order_book
                    .process_limit_order(
                        out_point,
                        usize::from(order.outcome),
                        (&order.side).into(),
                        Decimal::from(order.price.msats),
                        Decimal::from(order.quantity_remaining),
                    )
                    .expect("should never panic");

                for order_match in order_match_vec {
                    let mut order = dbtx
                        .get_value(&OddsMarketsOrderKey {
                            market: market_outpoint.to_owned(),
                            order: order_match.order,
                        })
                        .await
                        .expect("should always find order");

                    let order_match_quantity: u64 =
                        order_match.quantity.try_into().expect("should never fail");
                    order.quantity_remaining -= order_match_quantity;
                    if let Side::Buy = side {
                        order.quantity_balance += order_match_quantity;
                    }

                    if let Side::Buy = side {
                        order.btc_balance += order.price
                            * order_match.quantity.try_into().expect("should never fail");
                    }
                    if order_match.cost.is_sign_positive() {
                        order.btc_balance -= Amount::from_msats(
                            order_match.cost.try_into().expect("should never fail"),
                        )
                    }
                    if order_match.cost.is_sign_negative() {
                        order.btc_balance += Amount::from_msats(
                            (-order_match.cost).try_into().expect("should never fail"),
                        )
                    }
                }
            }
            PredictionMarketsOutput::PayoutMarket(payout, signature) => {
                // check if payout already exists for market
                if let Some(_) = dbtx
                    .get_value(&OddsMarketsPayoutKey {
                        market: payout.market,
                    })
                    .await
                {
                    return Err(PredictionMarketsError::PayoutAlreadyExists).into_module_error_other();
                }

                // get market
                let market = match dbtx
                    .get_value(&OddsMarketsMarketKey {
                        market: payout.market,
                    })
                    .await
                {
                    Some(val) => val,
                    None => {
                        return Err(PredictionMarketsError::MarketDoesNotExist).into_module_error_other()
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
                    return Err(PredictionMarketsError::FailedPayoutValidation).into_module_error_other();
                }

                // validate payout signature
                if let Err(_) = payout.verify_schnorr(&market.outcome_control, signature) {
                    return Err(PredictionMarketsError::FailedPayoutValidation).into_module_error_other();
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
                    &OddsMarketsOutcomeKey(out_point),
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
        dbtx.get_value(&OddsMarketsOutcomeKey(out_point)).await
    }

    async fn audit(&self, _dbtx: &mut ModuleDatabaseTransaction<'_>, _audit: &mut Audit) {}

    fn api_endpoints(&self) -> Vec<ApiEndpoint<Self>> {
        vec![api_endpoint! {
            "get_market",
            async |module: &OddsMarkets, context, out_point: OutPoint| -> Market {
                module.handle_get_market(&mut context.dbtx(), out_point).await
            }
        }]
    }
}

impl OddsMarkets {
    async fn handle_get_market(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        out_point: OutPoint,
    ) -> Result<Market, ApiError> {
        match dbtx
            .get_value(&OddsMarketsMarketKey { market: out_point })
            .await
        {
            Some(val) => return Ok(val),
            None => return Err(ApiError::not_found("market not found".into())),
        };
    }

    async fn handle_get_order(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        market: OutPoint,
        order: OutPoint,
    ) -> Result<Order, ApiError> {
        match dbtx.get_value(&OddsMarketsOrderKey { market, order }).await {
            Some(val) => return Ok(val),
            None => return Err(ApiError::not_found("order not found".into())),
        };
    }
}

/// An in-memory cache we could use for faster validation
#[derive(Debug, Clone)]
pub struct OddsMarketsCache;

impl fedimint_core::server::VerificationCache for OddsMarketsCache {}

impl OddsMarkets {
    /// Create new module instance
    pub fn new(cfg: PredictionMarketsConfig) -> OddsMarkets {
        OddsMarkets {
            cfg,
            propose_consensus: Notify::new(),
        }
    }
}
