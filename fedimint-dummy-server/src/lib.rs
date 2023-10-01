use std::collections::BTreeMap;

use std::string::ToString;

use anyhow::bail;
use async_trait::async_trait;

use db::{
    DbKeyPrefix, OddsMarketsMarketKey, OddsMarketsMarketPrefix, OddsMarketsOutPointKey,
    OddsMarketsOutPointPrefix, OddsMarketsPayoutKey, OddsMarketsPayoutPrefix,
};
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
    OddsMarketsClientConfig, OddsMarketsConfig, OddsMarketsConfigConsensus, OddsMarketsConfigLocal,
    OddsMarketsConfigPrivate, OddsMarketsGenParams,
};
use fedimint_dummy_common::{Market, Payout};
pub use fedimint_dummy_common::{
    OddsMarketsCommonGen, OddsMarketsConsensusItem, OddsMarketsError, OddsMarketsInput,
    OddsMarketsModuleTypes, OddsMarketsOutput, OddsMarketsOutputOutcome, CONSENSUS_VERSION, KIND,
};
use futures::StreamExt;

use secp256k1::schnorr::Signature;

use strum::IntoEnumIterator;

use tokio::sync::Notify;

mod db;

/// Generates the module
#[derive(Debug, Clone)]
pub struct OddsMarketsGen;

// TODO: Boilerplate-code
impl ExtendsCommonModuleInit for OddsMarketsGen {
    type Common = OddsMarketsCommonGen;
}

/// Implementation of server module non-consensus functions
#[async_trait]
impl ServerModuleInit for OddsMarketsGen {
    type Params = OddsMarketsGenParams;
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
                let config = OddsMarketsConfig {
                    local: OddsMarketsConfigLocal {
                        examples: "test".to_owned(),
                    },
                    private: OddsMarketsConfigPrivate {
                        example: "test".to_owned(),
                    },
                    consensus: OddsMarketsConfigConsensus {
                        new_market_fee: params.consensus.new_market_fee,
                        max_contract_value: params.consensus.max_contract_value,
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

        Ok(OddsMarketsConfig {
            local: OddsMarketsConfigLocal {
                examples: "test".to_owned(),
            },
            private: OddsMarketsConfigPrivate {
                example: "test".to_owned(),
            },
            consensus: OddsMarketsConfigConsensus {
                new_market_fee: params.consensus.new_market_fee,
                max_contract_value: params.consensus.max_contract_value,
            },
        }
        .to_erased())
    }

    /// Converts the consensus config into the client config
    fn get_client_config(
        &self,
        config: &ServerModuleConsensusConfig,
    ) -> anyhow::Result<OddsMarketsClientConfig> {
        let config = OddsMarketsConfigConsensus::from_erased(config)?;
        Ok(OddsMarketsClientConfig {
            new_market_fee: config.new_market_fee,
        })
    }

    /// Validates the private/public key of configs
    fn validate_config(
        &self,
        _identity: &PeerId,
        config: ServerModuleConfig,
    ) -> anyhow::Result<()> {
        let _config = config.to_typed::<OddsMarketsConfig>()?;

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
                DbKeyPrefix::OutPoint => {
                    push_db_pair_items!(
                        dbtx,
                        OddsMarketsOutPointPrefix,
                        OddsMarketsOutPointKey,
                        OddsMarketsOutputOutcome,
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
            }
        }

        Box::new(items.into_iter())
    }
}

/// Dummy module
#[derive(Debug)]
pub struct OddsMarkets {
    pub cfg: OddsMarketsConfig,
    /// Notifies us to propose an epoch
    pub propose_consensus: Notify,
}

/// Implementation of consensus for the server module
#[async_trait]
impl ServerModule for OddsMarkets {
    /// Define the consensus types
    type Common = OddsMarketsModuleTypes;
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
    ) -> ConsensusProposal<OddsMarketsConsensusItem> {
        ConsensusProposal::new_auto_trigger(vec![])
    }

    async fn process_consensus_item<'a, 'b>(
        &'a self,
        _dbtx: &mut ModuleDatabaseTransaction<'b>,
        _consensus_item: OddsMarketsConsensusItem,
        _peer_id: PeerId,
    ) -> anyhow::Result<()> {
        bail!("currently no consensus items")
    }

    fn build_verification_cache<'a>(
        &'a self,
        _inputs: impl Iterator<Item = &'a OddsMarketsInput> + Send,
    ) -> Self::VerificationCache {
        OddsMarketsCache
    }

    async fn process_input<'a, 'b, 'c>(
        &'a self,
        _dbtx: &mut ModuleDatabaseTransaction<'c>,
        _input: &'b OddsMarketsInput,
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
        output: &'a OddsMarketsOutput,
        out_point: OutPoint,
    ) -> Result<TransactionItemAmount, ModuleError> {
        let amount = Amount::ZERO;
        let mut fee = Amount::ZERO;

        match output {
            OddsMarketsOutput::NewMarket(market) => {
                // verify market params
                if market.contract_value > self.cfg.consensus.max_contract_value {
                    return Err(OddsMarketsError::FailedNewMarketValidation)
                        .into_module_error_other();
                }

                // set new market fee
                fee = self.cfg.consensus.new_market_fee;

                // save market
                dbtx.insert_new_entry(&OddsMarketsMarketKey { market: out_point }, market)
                    .await;

                // save outcome status
                dbtx.insert_new_entry(
                    &OddsMarketsOutPointKey(out_point),
                    &OddsMarketsOutputOutcome::NewMarket,
                )
                .await;
            }
            OddsMarketsOutput::PayoutMarket(payout, signature) => {
                // check if payout already exists for market
                if let Some(_) = dbtx
                    .get_value(&OddsMarketsPayoutKey {
                        market: payout.market,
                    })
                    .await
                {
                    return Err(OddsMarketsError::PayoutAlreadyExists).into_module_error_other();
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
                        return Err(OddsMarketsError::MarketDoesNotExist).into_module_error_other()
                    }
                };

                // validate payout parameters
                if payout.positive_contract_payout < Amount::ZERO
                    || payout.positive_contract_payout > market.contract_value
                {
                    return Err(OddsMarketsError::FailedPayoutValidation).into_module_error_other();
                }

                // validate payout signature
                if let Err(_) = payout.verify_schnorr(&market.outcome_control, signature) {
                    return Err(OddsMarketsError::FailedPayoutValidation).into_module_error_other();
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
                .await
            }
            OddsMarketsOutput::NewOrder {} => {}
        }

        Ok(TransactionItemAmount { amount, fee })
    }

    async fn output_status(
        &self,
        dbtx: &mut ModuleDatabaseTransaction<'_>,
        out_point: OutPoint,
    ) -> Option<OddsMarketsOutputOutcome> {
        dbtx.get_value(&OddsMarketsOutPointKey(out_point)).await
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
}

/// An in-memory cache we could use for faster validation
#[derive(Debug, Clone)]
pub struct OddsMarketsCache;

impl fedimint_core::server::VerificationCache for OddsMarketsCache {}

impl OddsMarkets {
    /// Create new module instance
    pub fn new(cfg: OddsMarketsConfig) -> OddsMarkets {
        OddsMarkets {
            cfg,
            propose_consensus: Notify::new(),
        }
    }
}
