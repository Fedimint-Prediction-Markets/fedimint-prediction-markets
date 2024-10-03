use std::collections::{BTreeMap, HashMap, HashSet};
use std::string::ToString;

use anyhow::bail;
use async_trait::async_trait;
use candlestick_data_creator::CandlestickDataCreator;
use db::DbKeyPrefix;
use fedimint_core::config::{
    ConfigGenModuleParams, DkgResult, ServerModuleConfig, ServerModuleConsensusConfig,
    TypedServerModuleConfig, TypedServerModuleConsensusConfig,
};
use fedimint_core::core::ModuleInstanceId;
use fedimint_core::db::{
    Committable, Database, DatabaseTransaction, DatabaseVersion, IDatabaseTransactionOpsCoreTyped,
    ServerMigrationFn,
};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::module::audit::Audit;
use fedimint_core::module::{
    api_endpoint, ApiEndpoint, ApiEndpointContext, ApiError, ApiVersion, CoreConsensusVersion,
    InputMeta, ModuleConsensusVersion, ModuleInit, MultiApiVersion, PeerHandle, ServerModuleInit,
    ServerModuleInitArgs, SupportedModuleApiVersions, TransactionItemAmount,
};
use fedimint_core::server::DynServerModule;
use fedimint_core::{push_db_pair_items, Amount, OutPoint, PeerId, ServerModule};
use fedimint_prediction_markets_common::config::GeneralConsensus;
use fedimint_prediction_markets_common::{
    api, config, Candlestick, ContractAmount, ContractOfOutcomeAmount, Market, MarketDynamic,
    MarketStatic, Order, Outcome, Payout, PredictionMarketsCommonInit,
    PredictionMarketsConsensusItem, PredictionMarketsInput, PredictionMarketsInputError,
    PredictionMarketsModuleTypes, PredictionMarketsOutput, PredictionMarketsOutputError,
    PredictionMarketsOutputOutcome, Side, SignedAmount, TimeOrdering, UnixTimestamp,
    WeightRequiredForPayout, MODULE_CONSENSUS_VERSION,
};
use fedimint_server::config::CORE_CONSENSUS_VERSION;
use futures::{future, StreamExt};
use highest_priority_order_cache::HighestPriorityOrderCache;
use order_cache::OrderCache;
use prediction_market_event::nostr_event_types::NostrEventUtils;
use prediction_market_event::Event;
use secp256k1::PublicKey;
use serde::Serialize;
use strum::IntoEnumIterator;

mod candlestick_data_creator;
mod db;
mod highest_priority_order_cache;
mod order_cache;

/// Generates the module
#[derive(Debug, Clone)]
pub struct PredictionMarketsInit;

#[async_trait]
impl ModuleInit for PredictionMarketsInit {
    type Common = PredictionMarketsCommonInit;
    const DATABASE_VERSION: DatabaseVersion = DatabaseVersion(0);

    /// Dumps all database items for debugging
    async fn dump_database(
        &self,
        dbtx: &mut DatabaseTransaction<'_>,
        prefix_names: Vec<String>,
    ) -> Box<dyn Iterator<Item = (String, Box<dyn erased_serde::Serialize + Send>)> + '_> {
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
                        "Outcome"
                    );
                }
                DbKeyPrefix::MarketStatic => {
                    push_db_pair_items!(
                        dbtx,
                        db::MarketStaticPrefixAll,
                        db::MarketStaticKey,
                        MarketStatic,
                        items,
                        "MarketStatic"
                    );
                }
                DbKeyPrefix::MarketDynamic => {
                    push_db_pair_items!(
                        dbtx,
                        db::MarketDynamicPrefixAll,
                        db::MarketDynamicKey,
                        MarketDynamic,
                        items,
                        "MarketDynamic"
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
                DbKeyPrefix::MarketSpecificationsNeededForNewOrders => {
                    push_db_pair_items!(
                        dbtx,
                        db::MarketSpecificationsNeededForNewOrdersPrefixAll,
                        db::MarketSpecificationsNeededForNewOrdersKey,
                        MarketSpecificationsNeededForNewOrders,
                        items,
                        "MarketSpecificationsNeededForNewOrders"
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
                        PublicKey,
                        items,
                        "OrderPriceTimePriority"
                    );
                }
                DbKeyPrefix::EventPayoutAttestationsUsedToPermitPayout => {
                    push_db_pair_items!(
                        dbtx,
                        db::EventPayoutAttestationsUsedToPermitPayoutPrefixAll,
                        db::EventPayoutAttestationsUsedToPermitPayoutKey,
                        Vec<String>,
                        items,
                        "EventPayoutAttestationsUsedToPermitPayout"
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
                DbKeyPrefix::MarketOutcomeNewestCandlestickVolume => {
                    push_db_pair_items!(
                        dbtx,
                        db::MarketOutcomeNewestCandlestickVolumePrefixAll,
                        db::MarketOutcomeNewestCandlestickKey,
                        (UnixTimestamp, ContractOfOutcomeAmount),
                        items,
                        "MarketOutcomeNewestCandlestick"
                    );
                }
                DbKeyPrefix::PeersProposedTimestamp => {
                    push_db_pair_items!(
                        dbtx,
                        db::PeersProposedTimestampPrefixAll,
                        db::PeersProposedTimestampKey,
                        UnixTimestamp,
                        items,
                        "PeersProposedTimestamp"
                    );
                }
            }
        }

        Box::new(items.into_iter())
    }
}

/// Implementation of server module non-consensus functions
#[async_trait]
impl ServerModuleInit for PredictionMarketsInit {
    type Params = config::PredictionMarketsGenParams;

    /// Returns the version of this module
    fn versions(&self, _core: CoreConsensusVersion) -> &[ModuleConsensusVersion] {
        &[MODULE_CONSENSUS_VERSION]
    }

    fn supported_api_versions(&self) -> SupportedModuleApiVersions {
        SupportedModuleApiVersions {
            core_consensus: CORE_CONSENSUS_VERSION,
            module_consensus: MODULE_CONSENSUS_VERSION,
            api: MultiApiVersion::try_from_iter([ApiVersion::new(0, 0)])
                .expect("no version conflicts"),
        }
    }

    /// Initialize the module
    async fn init(&self, args: &ServerModuleInitArgs<Self>) -> anyhow::Result<DynServerModule> {
        Ok(PredictionMarkets::new(args.cfg().to_typed()?, args.db().to_owned()).into())
    }

    /// DB migrations to move from old to newer versions
    fn get_database_migrations(&self) -> BTreeMap<DatabaseVersion, ServerMigrationFn> {
        let migrations: BTreeMap<DatabaseVersion, ServerMigrationFn> = BTreeMap::new();
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
                let config = config::PredictionMarketsConfig {
                    local: config::PredictionMarketsConfigLocal {
                        peer_count: peers.len().try_into().unwrap(),
                    },
                    private: config::PredictionMarketsConfigPrivate {
                        example: "test".into(),
                    },
                    consensus: config::PredictionMarketsConfigConsensus {
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

        Ok(config::PredictionMarketsConfig {
            local: config::PredictionMarketsConfigLocal {
                peer_count: peers.peer_ids().len().try_into().unwrap(),
            },
            private: config::PredictionMarketsConfigPrivate {
                example: "test".into(),
            },
            consensus: config::PredictionMarketsConfigConsensus {
                gc: params.consensus.gc,
            },
        }
        .to_erased())
    }

    /// Converts the consensus config into the client config
    fn get_client_config(
        &self,
        config: &ServerModuleConsensusConfig,
    ) -> anyhow::Result<config::PredictionMarketsClientConfig> {
        let config = config::PredictionMarketsConfigConsensus::from_erased(config)?;
        Ok(config::PredictionMarketsClientConfig { gc: config.gc })
    }

    /// Validates the private/public key of configs
    fn validate_config(
        &self,
        _identity: &PeerId,
        config: ServerModuleConfig,
    ) -> anyhow::Result<()> {
        let _config = config.to_typed::<config::PredictionMarketsConfig>()?;

        Ok(())
    }
}

/// Prediction Markets Module
#[derive(Debug)]
pub struct PredictionMarkets {
    pub cfg: config::PredictionMarketsConfig,
    pub db: Database,
}

impl PredictionMarkets {
    /// Create new module instance
    pub fn new(cfg: config::PredictionMarketsConfig, db: Database) -> PredictionMarkets {
        PredictionMarkets { cfg, db }
    }
}

/// Implementation of consensus for the server module
#[async_trait]
impl ServerModule for PredictionMarkets {
    /// Define the consensus types
    type Common = PredictionMarketsModuleTypes;
    type Init = PredictionMarketsInit;

    async fn consensus_proposal(
        &self,
        _dbtx: &mut DatabaseTransaction<'_>,
    ) -> Vec<PredictionMarketsConsensusItem> {
        let timestamp_to_propose =
            UnixTimestamp::now().round_down(self.cfg.consensus.gc.timestamp_interval);
        let timestamp_proposal =
            PredictionMarketsConsensusItem::TimestampProposal(timestamp_to_propose);

        vec![timestamp_proposal]
    }

    async fn process_consensus_item<'a, 'b>(
        &'a self,
        dbtx: &mut DatabaseTransaction<'b>,
        consensus_item: PredictionMarketsConsensusItem,
        peer_id: PeerId,
    ) -> anyhow::Result<()> {
        match consensus_item {
            PredictionMarketsConsensusItem::TimestampProposal(timestamp_proposed) => {
                // checks
                if !timestamp_proposed.divisible(self.cfg.consensus.gc.timestamp_interval) {
                    bail!("new unix timestamp is not divisible by timestamp interval");
                }
                let current_peer_timestamp = dbtx
                    .get_value(&db::PeersProposedTimestampKey { peer_id })
                    .await
                    .unwrap_or(UnixTimestamp::ZERO);
                if current_peer_timestamp >= timestamp_proposed {
                    bail!("proposed timestamp is not after current timestamp")
                }

                // insert
                dbtx.insert_entry(
                    &db::PeersProposedTimestampKey { peer_id },
                    &timestamp_proposed,
                )
                .await;
                Ok(())
            }
        }
    }

    async fn process_input<'a, 'b, 'c>(
        &'a self,
        dbtx: &mut DatabaseTransaction<'c>,
        input: &'b PredictionMarketsInput,
    ) -> Result<InputMeta, PredictionMarketsInputError> {
        let amount;
        let fee;
        let pub_key;

        match input {
            PredictionMarketsInput::NewSellOrder {
                owner,
                market,
                outcome,
                price,
                sources,
            } => {
                // check that order does not already exists for owner
                if let Some(_) = dbtx.get_value(&db::OrderKey(*owner)).await {
                    return Err(PredictionMarketsInputError::OrderAlreadyExists);
                }

                // get market dynamic
                let Some(market_dynamic) = dbtx.get_value(&db::MarketDynamicKey(*market)).await
                else {
                    return Err(PredictionMarketsInputError::MarketDoesNotExist);
                };

                // check if payout has already occurred
                if market_dynamic.payout.is_some() {
                    return Err(PredictionMarketsInputError::MarketFinished);
                }

                // get quantity from sources, verifying public keys of sources
                let Ok((quantity, source_order_public_keys_combined)) =
                    Self::verify_and_process_contract_of_outcome_sources(
                        dbtx,
                        &self.cfg.consensus.gc,
                        sources,
                        market,
                        *outcome,
                    )
                    .await
                else {
                    return Err(PredictionMarketsInputError::OrderValidationFailed);
                };

                // get MarketSpecificationsNeededForNewOrders
                let market_specifications = dbtx
                    .get_value(&db::MarketSpecificationsNeededForNewOrdersKey(*market))
                    .await
                    .unwrap();

                // verify order params
                if let Err(()) = Order::validate_order_params(
                    &self.cfg.consensus.gc,
                    &market_specifications.outcome_count,
                    &market_specifications.contract_price,
                    outcome,
                    price,
                    &quantity,
                ) {
                    return Err(PredictionMarketsInputError::OrderValidationFailed);
                }

                // set input meta
                amount = Amount::ZERO;
                fee = self.cfg.consensus.gc.new_order_fee;
                pub_key = source_order_public_keys_combined;

                // process order
                self.process_new_order(
                    dbtx,
                    *market,
                    market_dynamic,
                    market_specifications,
                    *owner,
                    *outcome,
                    Side::Sell,
                    *price,
                    quantity,
                )
                .await;
            }
            PredictionMarketsInput::ConsumeOrderBitcoinBalance {
                order: order_owner,
                amount: amount_to_consume,
            } => {
                // get order
                let Some(mut order) = dbtx.get_value(&db::OrderKey(*order_owner)).await else {
                    return Err(PredictionMarketsInputError::OrderDoesNotExist);
                };

                // check if order has sufficent balance
                if &order.bitcoin_balance < amount_to_consume {
                    return Err(PredictionMarketsInputError::NotEnoughFunds);
                }

                // set input meta
                amount = *amount_to_consume;
                fee = self.cfg.consensus.gc.consume_order_bitcoin_balance_fee;
                pub_key = *order_owner;

                // update order's bitcoin balance
                order.bitcoin_balance -= *amount_to_consume;
                dbtx.insert_entry(&db::OrderKey(*order_owner), &order).await;
            }
            PredictionMarketsInput::CancelOrder { order: order_owner } => {
                // get order
                let Some(mut order) = dbtx.get_value(&db::OrderKey(*order_owner)).await else {
                    return Err(PredictionMarketsInputError::OrderDoesNotExist);
                };

                // check if order already finished
                if order.quantity_waiting_for_match == ContractOfOutcomeAmount::ZERO {
                    return Err(PredictionMarketsInputError::OrderAlreadyFinished);
                }

                // set input meta
                amount = Amount::ZERO;
                fee = Amount::ZERO;
                pub_key = order_owner.to_owned();

                // cancel order
                Self::cancel_order(dbtx, order_owner, &mut order).await;
            }
        }

        Ok(InputMeta {
            amount: TransactionItemAmount { amount, fee },
            // IMPORTANT: include the pubkey to validate the user signed this tx
            pub_key,
        })
    }

    async fn process_output<'a, 'b>(
        &'a self,
        dbtx: &mut DatabaseTransaction<'b>,
        output: &'a PredictionMarketsOutput,
        out_point: OutPoint,
    ) -> Result<TransactionItemAmount, PredictionMarketsOutputError> {
        let amount;
        let fee;

        match output {
            PredictionMarketsOutput::NewMarket {
                event_json,
                contract_price,
                payout_control_weight_map,
                weight_required_for_payout,
            } => {
                let event = Event::try_from_json_str(event_json)
                    .map_err(|_| PredictionMarketsOutputError::MarketValidationFailed)?;

                // verify market params
                if let Err(()) = Market::validate_market_params(
                    &self.cfg.consensus.gc,
                    &event,
                    contract_price,
                    payout_control_weight_map,
                    weight_required_for_payout,
                ) {
                    return Err(PredictionMarketsOutputError::MarketValidationFailed);
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

                // save market static
                let event_json = ensure_compact_json(event_json)
                    .map_err(|e| PredictionMarketsOutputError::Other(e.to_string()))?;
                let consensus_timestamp = self.get_consensus_timestamp(dbtx).await;

                dbtx.insert_new_entry(
                    &db::MarketStaticKey(out_point),
                    &MarketStatic {
                        event_json,
                        contract_price: contract_price.to_owned(),
                        payout_control_weight_map: payout_control_weight_map.to_owned(),
                        weight_required_for_payout: weight_required_for_payout.to_owned(),
                        created_consensus_timestamp: consensus_timestamp,
                    },
                )
                .await;

                // save market dynamic
                dbtx.insert_new_entry(
                    &db::MarketDynamicKey(out_point),
                    &MarketDynamic {
                        open_contracts: ContractAmount::ZERO,
                        payout: None,
                    },
                )
                .await;

                // save MarketSpecificationsNeededForNewOrders
                dbtx.insert_new_entry(
                    &db::MarketSpecificationsNeededForNewOrdersKey(out_point),
                    &MarketSpecificationsNeededForNewOrders {
                        outcome_count: event.outcome_count,
                        contract_price: contract_price.to_owned(),
                        next_time_ordering: 0,
                    },
                )
                .await;
            }
            PredictionMarketsOutput::NewBuyOrder {
                owner,
                market,
                outcome,
                price,
                quantity,
            } => {
                // check that order does not already exists for owner
                if let Some(_) = dbtx.get_value(&db::OrderKey(*owner)).await {
                    return Err(PredictionMarketsOutputError::OrderAlreadyExists);
                }

                // get market dynamic
                let Some(market_dynamic) = dbtx.get_value(&db::MarketDynamicKey(*market)).await
                else {
                    return Err(PredictionMarketsOutputError::MarketDoesNotExist);
                };

                // check if payout has already occurred
                if market_dynamic.payout.is_some() {
                    return Err(PredictionMarketsOutputError::MarketFinished);
                }

                // get MarketSpecificationsNeededForNewOrders
                let market_specifications = dbtx
                    .get_value(&db::MarketSpecificationsNeededForNewOrdersKey(*market))
                    .await
                    .unwrap();

                // verify order params
                if let Err(_) = Order::validate_order_params(
                    &self.cfg.consensus.gc,
                    &market_specifications.outcome_count,
                    &market_specifications.contract_price,
                    outcome,
                    price,
                    quantity,
                ) {
                    return Err(PredictionMarketsOutputError::OrderValidationFailed);
                }

                // set output meta
                amount = *price * quantity.0;
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
                    *market,
                    market_dynamic,
                    market_specifications,
                    *owner,
                    *outcome,
                    Side::Buy,
                    *price,
                    *quantity,
                )
                .await;
            }
            PredictionMarketsOutput::PayoutMarket {
                market,
                event_payout_attestations_json,
            } => {
                // get market static and market dynamic
                let Some(market_static) = dbtx.get_value(&db::MarketStaticKey(*market)).await
                else {
                    return Err(PredictionMarketsOutputError::MarketDoesNotExist);
                };
                let mut market_dynamic = dbtx
                    .get_value(&db::MarketDynamicKey(*market))
                    .await
                    .unwrap();

                // check if payout already exists
                if market_dynamic.payout.is_some() {
                    return Err(PredictionMarketsOutputError::PayoutAlreadyExists);
                }

                // validate payout
                let event = market_static.event().unwrap();
                let mut sum_weight: WeightRequiredForPayout = 0;
                let mut used_keys_set: HashSet<
                    prediction_market_event::nostr_event_types::NostrPublicKeyHex,
                > = HashSet::new();
                let mut event_payout: Option<prediction_market_event::EventPayout> = None;
                let mut event_payout_attestations_json_ensured_compact: Vec<String> = Vec::new();

                for event_json in event_payout_attestations_json {
                    let Ok((loop_nostr_public_key_hex, loop_event_payout)) =
                        prediction_market_event::nostr_event_types::EventPayoutAttestation::interpret_nostr_event_json(event_json)
                    else {
                        return Err(PredictionMarketsOutputError::PayoutValidationFailed)
                    };

                    if !used_keys_set.insert(loop_nostr_public_key_hex.to_owned()) {
                        return Err(PredictionMarketsOutputError::PayoutValidationFailed);
                    }

                    let Some(nostr_key_weight) = market_static
                        .payout_control_weight_map
                        .get(&loop_nostr_public_key_hex.0)
                    else {
                        return Err(PredictionMarketsOutputError::PayoutValidationFailed);
                    };
                    sum_weight += WeightRequiredForPayout::from(*nostr_key_weight);

                    match event_payout.as_mut() {
                        None => {
                            let Ok(()) = loop_event_payout.validate(&event) else {
                                return Err(PredictionMarketsOutputError::PayoutValidationFailed);
                            };
                            event_payout = Some(loop_event_payout);
                        }
                        Some(ep) => {
                            if ep != &loop_event_payout {
                                return Err(PredictionMarketsOutputError::PayoutValidationFailed);
                            }
                        }
                    };

                    let event_json_ensured_compact = ensure_compact_json(event_json)
                        .map_err(|e| PredictionMarketsOutputError::Other(e.to_string()))?;
                    event_payout_attestations_json_ensured_compact.push(event_json_ensured_compact);
                }

                if sum_weight < market_static.weight_required_for_payout {
                    return Err(PredictionMarketsOutputError::PayoutValidationFailed);
                }

                // set input meta
                amount = Amount::ZERO;
                fee = Amount::ZERO;

                let mut assert_test_total_orders_payout = Amount::ZERO;
                let event_payout = event_payout.unwrap();
                let payout_scaling_factor =
                    market_static.contract_price.msats / u64::from(event.units_to_payout);
                let payout_amount_per_outcome = event_payout
                    .units_per_outcome
                    .iter()
                    .map(|u| {
                        let msats = u64::from(*u) * payout_scaling_factor;
                        Amount::from_msats(msats)
                    })
                    .collect::<Vec<_>>();

                let market_orders: Vec<_> = dbtx
                    .find_by_prefix(&db::OrdersByMarketPrefix1 { market: *market })
                    .await
                    .map(|(key, _)| key.order)
                    .collect()
                    .await;

                for order_owner in market_orders {
                    let mut order = dbtx.get_value(&db::OrderKey(order_owner)).await.unwrap();

                    Self::cancel_order(dbtx, &order_owner, &mut order).await;

                    let payout_per_contract_of_outcome = payout_amount_per_outcome
                        .get(usize::from(order.outcome))
                        .unwrap();
                    let payout =
                        *payout_per_contract_of_outcome * order.contract_of_outcome_balance.0;

                    order.contract_of_outcome_balance = ContractOfOutcomeAmount::ZERO;
                    order.bitcoin_balance += payout;
                    order.bitcoin_acquired_from_payout = payout;

                    dbtx.insert_entry(&db::OrderKey(order_owner), &order).await;

                    assert_test_total_orders_payout += payout;
                }

                // payout total assert
                assert_eq!(
                    market_static.contract_price * market_dynamic.open_contracts.0,
                    assert_test_total_orders_payout
                );

                // save payout to market
                market_dynamic.open_contracts = ContractAmount::ZERO;
                market_dynamic.payout = Some(Payout {
                    amount_per_outcome: payout_amount_per_outcome,
                    occurred_consensus_timestamp: self.get_consensus_timestamp(dbtx).await,
                });
                dbtx.insert_entry(&db::MarketDynamicKey(*market), &market_dynamic)
                    .await;
                dbtx.insert_new_entry(
                    &db::EventPayoutAttestationsUsedToPermitPayoutKey(*market),
                    &event_payout_attestations_json_ensured_compact,
                )
                .await;
            }
        }

        Ok(TransactionItemAmount { amount, fee })
    }

    async fn output_status(
        &self,
        dbtx: &mut DatabaseTransaction<'_>,
        out_point: OutPoint,
    ) -> Option<PredictionMarketsOutputOutcome> {
        dbtx.get_value(&db::OutcomeKey(out_point)).await
    }

    async fn audit(
        &self,
        dbtx: &mut DatabaseTransaction<'_>,
        audit: &mut Audit,
        module_instance_id: ModuleInstanceId,
    ) {
        // bitcoin owed for open contracts across markets
        let market_open_contracts_map: HashMap<OutPoint, ContractAmount> = dbtx
            .find_by_prefix(&db::MarketDynamicPrefixAll)
            .await
            .map(|(db::MarketDynamicKey(outpoint), market_dynamic)| {
                (outpoint, market_dynamic.open_contracts)
            })
            .collect()
            .await;
        audit
            .add_items(
                dbtx,
                module_instance_id,
                &db::MarketStaticPrefixAll,
                |db::MarketStaticKey(outpoint), market_static| {
                    let market_open_contracts = market_open_contracts_map.get(&outpoint).unwrap();
                    let milli_sat =
                        -((market_static.contract_price * market_open_contracts.0).msats as i64);

                    milli_sat
                },
            )
            .await;

        // bitcoin owed for collateral held for buy orders and in field bitcoin_balance
        // on orders
        audit
            .add_items(dbtx, module_instance_id, &db::OrderPrefixAll, |_, order| {
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
                api::GET_MARKET_ENDPOINT,
                ApiVersion::new(0, 0),
                async |module: &PredictionMarkets, context, params: api::GetMarketParams| -> api::GetMarketResult {
                    module.api_get_market(&mut context.dbtx(), params).await
                }
            },
            api_endpoint! {
                api::GET_MARKET_DYNAMIC_ENDPOINT,
                ApiVersion::new(0, 0),
                async |module: &PredictionMarkets, context, params: api::GetMarketDynamicParams| -> api::GetMarketDynamicResult {
                    module.api_get_market_dynamic(&mut context.dbtx(), params).await
                }
            },
            api_endpoint! {
                api::GET_EVENT_PAYOUT_ATTESTATIONS_USED_TO_PERMIT_PAYOUT_ENDPOINT,
                ApiVersion::new(0, 0),
                async |module: &PredictionMarkets, context, params: api::GetEventPayoutAttestationsUsedToPermitPayoutParams| -> api::GetEventPayoutAttestationsUsedToPermitPayoutResult {
                    module.api_get_event_payout_attestations_used_to_permit_payout(&mut context.dbtx(), params).await
                }
            },
            api_endpoint! {
                api::GET_ORDER_ENDPOINT,
                ApiVersion::new(0, 0),
                async |module: &PredictionMarkets, context, params: api::GetOrderParams| -> api::GetOrderResult {
                    module.api_get_order(&mut context.dbtx(), params).await
                }
            },
            api_endpoint! {
                api::GET_MARKET_OUTCOME_CANDLESTICKS_ENDPOINT,
                ApiVersion::new(0, 0),
                async |module: &PredictionMarkets, context, params: api::GetMarketOutcomeCandlesticksParams| -> api::GetMarketOutcomeCandlesticksResult {
                    module.api_get_market_outcome_candlesticks(&mut context.dbtx(), params).await
                }
            },
            api_endpoint! {
                api::WAIT_MARKET_OUTCOME_CANDLESTICKS_ENDPOINT,
                ApiVersion::new(0, 0),
                async |module: &PredictionMarkets, context, params: api::WaitMarketOutcomeCandlesticksParams| -> api::WaitMarketOutcomeCandlesticksResult {
                    module.api_wait_market_outcome_candlesticks(context, params).await
                }
            },
        ]
    }
}

//
// api endpoints
//
impl PredictionMarkets {
    async fn api_get_market(
        &self,
        dbtx: &mut DatabaseTransaction<'_, Committable>,
        params: api::GetMarketParams,
    ) -> Result<api::GetMarketResult, ApiError> {
        let Some(market_static) = dbtx.get_value(&db::MarketStaticKey(params.market)).await else {
            return Ok(api::GetMarketResult { market: None });
        };
        let market_dynamic = dbtx
            .get_value(&db::MarketDynamicKey(params.market))
            .await
            .unwrap();

        Ok(api::GetMarketResult {
            market: Some(Market(market_static, market_dynamic)),
        })
    }

    async fn api_get_market_dynamic(
        &self,
        dbtx: &mut DatabaseTransaction<'_, Committable>,
        params: api::GetMarketDynamicParams,
    ) -> Result<api::GetMarketDynamicResult, ApiError> {
        Ok(api::GetMarketDynamicResult {
            market_dynamic: dbtx.get_value(&db::MarketDynamicKey(params.market)).await,
        })
    }

    async fn api_get_event_payout_attestations_used_to_permit_payout(
        &self,
        dbtx: &mut DatabaseTransaction<'_, Committable>,
        params: api::GetEventPayoutAttestationsUsedToPermitPayoutParams,
    ) -> Result<api::GetEventPayoutAttestationsUsedToPermitPayoutResult, ApiError> {
        Ok(api::GetEventPayoutAttestationsUsedToPermitPayoutResult {
            event_payout_attestations: dbtx
                .get_value(&db::EventPayoutAttestationsUsedToPermitPayoutKey(
                    params.market,
                ))
                .await,
        })
    }

    async fn api_get_order(
        &self,
        dbtx: &mut DatabaseTransaction<'_, Committable>,
        params: api::GetOrderParams,
    ) -> Result<api::GetOrderResult, ApiError> {
        Ok(api::GetOrderResult {
            order: dbtx.get_value(&db::OrderKey(params.order)).await,
        })
    }

    async fn api_get_market_outcome_candlesticks(
        &self,
        dbtx: &mut DatabaseTransaction<'_, Committable>,
        params: api::GetMarketOutcomeCandlesticksParams,
    ) -> Result<api::GetMarketOutcomeCandlesticksResult, ApiError> {
        let candlesticks = dbtx
            .find_by_prefix_sorted_descending(&db::MarketOutcomeCandlesticksPrefix3 {
                market: params.market,
                outcome: params.outcome,
                candlestick_interval: params.candlestick_interval,
            })
            .await
            .take_while(|(k, _)| {
                future::ready(k.candlestick_timestamp >= params.min_candlestick_timestamp)
            })
            .map(|(k, v)| (k.candlestick_timestamp, v))
            .collect::<Vec<(UnixTimestamp, Candlestick)>>()
            .await;

        Ok(api::GetMarketOutcomeCandlesticksResult { candlesticks })
    }

    async fn api_wait_market_outcome_candlesticks(
        &self,
        context: &mut ApiEndpointContext<'_>,
        params: api::WaitMarketOutcomeCandlesticksParams,
    ) -> Result<api::WaitMarketOutcomeCandlesticksResult, ApiError> {
        let future = context.wait_value_matches(
            db::MarketOutcomeNewestCandlestickVolumeKey {
                market: params.market,
                outcome: params.outcome,
                candlestick_interval: params.candlestick_interval,
            },
            |(current_timestamp, current_volume)| {
                current_volume != &params.candlestick_volume
                    || current_timestamp != &params.candlestick_timestamp
            },
        );
        _ = future.await;

        let mut dbtx = self.db.begin_transaction_nc().await;
        let candlesticks = dbtx
            .find_by_prefix_sorted_descending(&db::MarketOutcomeCandlesticksPrefix3 {
                market: params.market,
                outcome: params.outcome,
                candlestick_interval: params.candlestick_interval,
            })
            .await
            .take_while(|(k, _)| {
                future::ready(k.candlestick_timestamp >= params.candlestick_timestamp)
            })
            .map(|(k, v)| (k.candlestick_timestamp, v))
            .collect::<Vec<(UnixTimestamp, Candlestick)>>()
            .await;

        Ok(api::WaitMarketOutcomeCandlesticksResult { candlesticks })
    }
}

//
// market operations
//
impl PredictionMarkets {
    async fn verify_and_process_contract_of_outcome_sources(
        dbtx: &mut DatabaseTransaction<'_>,
        gc: &GeneralConsensus,
        sources: &BTreeMap<PublicKey, ContractOfOutcomeAmount>,
        market: &OutPoint,
        outcome: Outcome,
    ) -> Result<(ContractOfOutcomeAmount, PublicKey), ()> {
        // check that sources is not empty or above max
        if sources.len() == 0 || sources.len() > usize::from(gc.max_sell_order_sources) {
            return Err(());
        }

        let mut total_contracts_sourced = ContractOfOutcomeAmount::ZERO;
        let mut source_order_public_keys_combined: Option<PublicKey> = None;

        for (order_owner, contracts_to_source_from_order) in sources {
            let Some(mut order) = dbtx.get_value(&db::OrderKey(order_owner.to_owned())).await
            else {
                return Err(());
            };

            if market != &order.market
                || outcome != order.outcome
                || contracts_to_source_from_order == &ContractOfOutcomeAmount::ZERO
                || contracts_to_source_from_order > &order.contract_of_outcome_balance
            {
                return Err(());
            }

            order.contract_of_outcome_balance -= contracts_to_source_from_order.to_owned();
            dbtx.insert_entry(&db::OrderKey(order_owner.to_owned()), &order)
                .await;
            total_contracts_sourced += contracts_to_source_from_order.to_owned();

            if let Some(p1) = source_order_public_keys_combined.as_mut() {
                let Ok(p2) = p1.combine(order_owner) else {
                    return Err(());
                };

                *p1 = p2;
            } else {
                source_order_public_keys_combined = Some(order_owner.to_owned());
            }
        }

        Ok((
            total_contracts_sourced,
            source_order_public_keys_combined.unwrap(),
        ))
    }

    async fn process_new_order(
        &self,
        dbtx: &mut DatabaseTransaction<'_>,
        market: OutPoint,
        mut market_dynamic: MarketDynamic,
        mut market_specifications: MarketSpecificationsNeededForNewOrders,
        order_owner: PublicKey,
        outcome: Outcome,
        side: Side,
        price: Amount,
        quantity: ContractOfOutcomeAmount,
    ) {
        let consensus_timestamp = self.get_consensus_timestamp(dbtx).await;
        let beginning_market_open_contracts = market_dynamic.open_contracts;

        let mut order_cache = OrderCache::new();
        let mut highest_priority_order_cache =
            HighestPriorityOrderCache::new(&market_specifications);
        let mut candlestick_data_creator = CandlestickDataCreator::new(
            &self.cfg.consensus.gc,
            consensus_timestamp,
            market,
            &market_specifications,
        );

        let time_ordering = {
            let n = market_specifications.next_time_ordering;
            market_specifications.next_time_ordering += 1;
            dbtx.insert_entry(
                &db::MarketSpecificationsNeededForNewOrdersKey(market),
                &market_specifications,
            )
            .await
            .unwrap();
            n
        };

        let mut order = Order {
            market,
            outcome,
            side,
            price,
            original_quantity: quantity,
            time_ordering,
            created_consensus_timestamp: consensus_timestamp,

            quantity_waiting_for_match: quantity,
            contract_of_outcome_balance: ContractOfOutcomeAmount::ZERO,
            bitcoin_balance: Amount::ZERO,

            quantity_fulfilled: ContractOfOutcomeAmount::ZERO,
            bitcoin_acquired_from_order_matches: SignedAmount::ZERO,
            bitcoin_acquired_from_payout: Amount::ZERO,
        };

        while order.quantity_waiting_for_match > ContractOfOutcomeAmount::ZERO {
            let own = Self::get_own_outcome_price_quantity(
                dbtx,
                &mut order_cache,
                &mut highest_priority_order_cache,
                &market,
                order.outcome,
                order.side,
            )
            .await;
            let other = Self::get_other_outcomes_price_quantity(
                dbtx,
                &mut order_cache,
                &mut highest_priority_order_cache,
                &market,
                &market_specifications,
                order.outcome,
                order.side,
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
                let (own_price, own_quantity) = own.unwrap();
                let satisfied_quantity = order.quantity_waiting_for_match.min(own_quantity);

                Self::process_quantity_on_order_in_highest_priority_order_cache(
                    dbtx,
                    &mut order_cache,
                    &mut highest_priority_order_cache,
                    &mut candlestick_data_creator,
                    order.outcome,
                    satisfied_quantity,
                )
                .await;

                order.quantity_waiting_for_match -= satisfied_quantity;

                order.quantity_fulfilled = order.quantity_fulfilled + satisfied_quantity;

                match side {
                    Side::Buy => {
                        order.contract_of_outcome_balance += satisfied_quantity;

                        order.bitcoin_balance += (order.price - own_price) * satisfied_quantity.0;

                        order.bitcoin_acquired_from_order_matches -=
                            SignedAmount::from(own_price * satisfied_quantity.0);
                    }
                    Side::Sell => {
                        order.bitcoin_balance += own_price * satisfied_quantity.0;

                        order.bitcoin_acquired_from_order_matches +=
                            SignedAmount::from(own_price * satisfied_quantity.0);
                    }
                }

                candlestick_data_creator
                    .add(dbtx, outcome, own_price, satisfied_quantity)
                    .await;

            // process other outcome match (contract creation/destruction)
            } else if matches_other {
                let (other_price, other_quantity) = other.unwrap();
                let satisfied_quantity = order.quantity_waiting_for_match.min(other_quantity);

                for outcome in 0..market_specifications.outcome_count {
                    if outcome == order.outcome {
                        continue;
                    }

                    Self::process_quantity_on_order_in_highest_priority_order_cache(
                        dbtx,
                        &mut order_cache,
                        &mut highest_priority_order_cache,
                        &mut candlestick_data_creator,
                        outcome,
                        satisfied_quantity,
                    )
                    .await;
                }

                order.quantity_waiting_for_match -= satisfied_quantity;

                order.quantity_fulfilled += satisfied_quantity;

                match side {
                    Side::Buy => {
                        order.contract_of_outcome_balance += satisfied_quantity;

                        order.bitcoin_balance +=
                            Amount::try_from(SignedAmount::from(order.price) - other_price)
                                .unwrap()
                                * satisfied_quantity.0;

                        order.bitcoin_acquired_from_order_matches -=
                            other_price * satisfied_quantity.0;

                        market_dynamic.open_contracts += ContractAmount(satisfied_quantity.0);
                    }
                    Side::Sell => {
                        order.bitcoin_balance +=
                            Amount::try_from(other_price).unwrap() * satisfied_quantity.0;

                        order.bitcoin_acquired_from_order_matches +=
                            other_price * satisfied_quantity.0;

                        market_dynamic.open_contracts -= ContractAmount(satisfied_quantity.0);
                    }
                }

                candlestick_data_creator
                    .add(
                        dbtx,
                        order.outcome,
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
                market,
                order: order_owner,
            },
            &(),
        )
        .await;
        if order.quantity_waiting_for_match != ContractOfOutcomeAmount::ZERO {
            dbtx.insert_new_entry(
                &db::OrderPriceTimePriorityKey::from_order(&order),
                &order_owner,
            )
            .await
        }

        // save market if changed
        if market_dynamic.open_contracts != beginning_market_open_contracts {
            dbtx.insert_entry(&db::MarketDynamicKey(market), &market_dynamic)
                .await;
        }

        // save order cache
        order_cache.save(dbtx).await;

        // save candlesticks if order matched with anything
        if order.original_quantity != order.quantity_waiting_for_match {
            candlestick_data_creator.save(dbtx).await;
        }
    }

    async fn get_outcome_side_highest_priority_order_price_quantity(
        dbtx: &mut DatabaseTransaction<'_>,
        order_cache: &mut OrderCache,
        highest_priority_order_cache: &mut HighestPriorityOrderCache,
        market: &OutPoint,
        outcome: Outcome,
        side: Side,
    ) -> Option<(Amount, ContractOfOutcomeAmount)> {
        if let Some(order_owner) = highest_priority_order_cache.get(outcome) {
            let order = order_cache.get(dbtx, order_owner).await;
            assert_ne!(
                order.quantity_waiting_for_match,
                ContractOfOutcomeAmount::ZERO
            );
            return Some((order.price, order.quantity_waiting_for_match));
        }

        let Some(highest_priority_order_owner) = dbtx
            .find_by_prefix(&db::OrderPriceTimePriorityPrefix3 {
                market: *market,
                outcome: outcome,
                side: side,
            })
            .await
            .next()
            .await
            .map(|(_, v)| v)
        else {
            return None;
        };
        let highest_priority_order = order_cache.get(dbtx, &highest_priority_order_owner).await;

        highest_priority_order_cache.set(outcome, Some(highest_priority_order_owner));

        Some((
            highest_priority_order.price,
            highest_priority_order.quantity_waiting_for_match,
        ))
    }

    async fn get_own_outcome_price_quantity(
        dbtx: &mut DatabaseTransaction<'_>,
        order_cache: &mut OrderCache,
        highest_priority_order_cache: &mut HighestPriorityOrderCache,
        market: &OutPoint,
        outcome: Outcome,
        side: Side,
    ) -> Option<(Amount, ContractOfOutcomeAmount)> {
        Self::get_outcome_side_highest_priority_order_price_quantity(
            dbtx,
            order_cache,
            highest_priority_order_cache,
            market,
            outcome,
            side.opposite(),
        )
        .await
    }

    async fn get_other_outcomes_price_quantity(
        dbtx: &mut DatabaseTransaction<'_>,
        order_cache: &mut OrderCache,
        highest_priority_order_cache: &mut HighestPriorityOrderCache,
        market: &OutPoint,
        market_specifications: &MarketSpecificationsNeededForNewOrders,
        outcome: Outcome,
        side: Side,
    ) -> Option<(SignedAmount, ContractOfOutcomeAmount)> {
        let mut price = SignedAmount::from(market_specifications.contract_price);
        let mut quantity = ContractOfOutcomeAmount(u64::MAX);

        for i in 0..market_specifications.outcome_count {
            if i == outcome {
                continue;
            }

            let (outcome_side_price, outcome_side_quantity) =
                Self::get_outcome_side_highest_priority_order_price_quantity(
                    dbtx,
                    order_cache,
                    highest_priority_order_cache,
                    market,
                    i,
                    side,
                )
                .await?;

            price -= outcome_side_price.into();
            quantity = quantity.min(outcome_side_quantity);
        }

        Some((price, quantity))
    }

    /// uses highest_priority_order_cache to find the order that quantity will
    /// be processed on.
    async fn process_quantity_on_order_in_highest_priority_order_cache(
        dbtx: &mut DatabaseTransaction<'_>,
        order_cache: &mut OrderCache,
        highest_priority_order_cache: &mut HighestPriorityOrderCache,
        candlestick_data_creator: &mut CandlestickDataCreator,
        outcome: Outcome,
        satisfied_quantity: ContractOfOutcomeAmount,
    ) {
        let order_owner = highest_priority_order_cache
            .get(outcome)
            .expect("order should always exist for outcome before process market quantity is ran");
        let order = order_cache.get_mut(dbtx, &order_owner).await;

        order.quantity_waiting_for_match -= satisfied_quantity;
        order.quantity_fulfilled += satisfied_quantity;

        match order.side {
            Side::Buy => {
                order.contract_of_outcome_balance += satisfied_quantity;

                order.bitcoin_acquired_from_order_matches -=
                    SignedAmount::from(order.price * satisfied_quantity.0);
            }
            Side::Sell => {
                order.bitcoin_balance += order.price * satisfied_quantity.0;

                order.bitcoin_acquired_from_order_matches +=
                    SignedAmount::from(order.price * satisfied_quantity.0);
            }
        }

        if order.quantity_waiting_for_match == ContractOfOutcomeAmount::ZERO {
            highest_priority_order_cache.set(outcome, None);

            dbtx.remove_entry(&db::OrderPriceTimePriorityKey::from_order(&order))
                .await
                .unwrap();
        }

        candlestick_data_creator
            .add(dbtx, order.outcome, order.price, satisfied_quantity)
            .await;
    }

    async fn cancel_order(
        dbtx: &mut DatabaseTransaction<'_>,
        order_owner: &PublicKey,
        order: &mut Order,
    ) {
        if order.quantity_waiting_for_match != ContractOfOutcomeAmount::ZERO {
            // move quantity waiting for match based on side
            match order.side {
                Side::Buy => {
                    order.bitcoin_balance += order.price * order.quantity_waiting_for_match.0
                }
                Side::Sell => order.contract_of_outcome_balance += order.quantity_waiting_for_match,
            }
            order.quantity_waiting_for_match = ContractOfOutcomeAmount::ZERO;

            dbtx.insert_entry(&db::OrderKey(*order_owner), &order).await;
            dbtx.remove_entry(&db::OrderPriceTimePriorityKey::from_order(order))
                .await
                .unwrap();
        }
    }

    async fn get_consensus_timestamp(&self, dbtx: &mut DatabaseTransaction<'_>) -> UnixTimestamp {
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

        *peers_proposed_unix_timestamps.get(i).unwrap()
    }
}

#[derive(Debug, Clone, Encodable, Decodable, Eq, PartialEq, Hash, Serialize)]
pub struct MarketSpecificationsNeededForNewOrders {
    outcome_count: Outcome,
    contract_price: Amount,
    next_time_ordering: TimeOrdering,
}

pub(crate) fn ensure_compact_json(json: &str) -> Result<String, serde_json::Error> {
    let d: serde_json::Value = serde_json::from_str(json)?;
    serde_json::to_string(&d)
}
