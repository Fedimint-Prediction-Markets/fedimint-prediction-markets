use std::collections::BTreeMap;
use std::iter;

use fedimint_client::sm::{DynState, State, StateTransition};
use fedimint_client::DynGlobalClientContext;
use fedimint_core::core::{IntoDynInstance, ModuleInstanceId, OperationId};
use fedimint_core::db::IDatabaseTransactionOpsCoreTyped;
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::TransactionId;
use fedimint_prediction_markets_common::UnixTimestamp;
use secp256k1::PublicKey;
use state_transitions::{await_tx_accepted, do_nothing, sync_market, sync_orders};

// use serde::{Deserialize, Serialize};
// use thiserror::Error;
use crate::PredictionMarketsClientContext;
use crate::{db, market_outpoint_from_tx_id, OrderId};

pub mod state_transitions;
pub mod triggers;

/// Tracks a transaction.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Decodable, Encodable)]
pub struct PredictionMarketsStateMachine {
    pub operation_id: OperationId,
    pub state: PredictionMarketState,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Decodable, Encodable)]
pub enum PredictionMarketState {
    NewMarket(NewMarketState),
    NewOrder(NewOrderState),
    CancelOrder(CancelOrderState),
    ConsumeOrderBitcoinBalance(ConsumeOrderBitcoinBalanceState),
    PayoutMarket(PayoutMarketState),
}

impl State for PredictionMarketsStateMachine {
    type ModuleContext = PredictionMarketsClientContext;

    fn transitions(
        &self,
        context: &Self::ModuleContext,
        global_context: &DynGlobalClientContext,
    ) -> Vec<StateTransition<Self>> {
        let operation_id = self.operation_id;

        match self.state.clone() {
            PredictionMarketState::NewMarket(s) => {
                s.transitions(operation_id, context, global_context)
            }
            PredictionMarketState::NewOrder(s) => {
                s.transitions(operation_id, context, global_context)
            }
            PredictionMarketState::CancelOrder(s) => {
                s.transitions(operation_id, context, global_context)
            }
            PredictionMarketState::ConsumeOrderBitcoinBalance(s) => {
                s.transitions(operation_id, context, global_context)
            }
            PredictionMarketState::PayoutMarket(s) => {
                s.transitions(operation_id, context, global_context)
            }
        }
    }

    fn operation_id(&self) -> OperationId {
        self.operation_id
    }
}

impl IntoDynInstance for PredictionMarketsStateMachine {
    type DynType = DynState;

    fn into_dyn(self, instance_id: ModuleInstanceId) -> Self::DynType {
        DynState::from_typed(instance_id, self)
    }
}

trait StateCategoryTrait: Into<PredictionMarketState> {
    fn transitions(
        self,
        operation_id: OperationId,
        context: &PredictionMarketsClientContext,
        global_context: &DynGlobalClientContext,
    ) -> Vec<StateTransition<PredictionMarketsStateMachine>>;
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Decodable, Encodable)]
pub enum NewMarketState {
    Pending { tx_id: TransactionId },
    Accepted { tx_id: TransactionId },
    Complete,
}

impl Into<PredictionMarketState> for NewMarketState {
    fn into(self) -> PredictionMarketState {
        PredictionMarketState::NewMarket(self)
    }
}
impl StateCategoryTrait for NewMarketState {
    fn transitions(
        self,
        operation_id: OperationId,
        _context: &PredictionMarketsClientContext,
        global_context: &DynGlobalClientContext,
    ) -> Vec<StateTransition<PredictionMarketsStateMachine>> {
        match self {
            NewMarketState::Pending { tx_id } => {
                vec![await_tx_accepted(
                    operation_id,
                    global_context,
                    tx_id,
                    Self::Accepted { tx_id },
                    Self::Complete,
                )]
            }
            NewMarketState::Accepted { tx_id } => {
                vec![
                    StateTransition::new(async {}, move |dbtx, _, _| {
                        Box::pin(async move {
                            dbtx.module_tx()
                                .insert_entry(
                                    &db::ClientSavedMarketsKey {
                                        market: market_outpoint_from_tx_id(tx_id),
                                    },
                                    &UnixTimestamp::now(),
                                )
                                .await;
                            PredictionMarketsStateMachine {
                                operation_id,
                                state: Self::Complete.into(),
                            }
                        })
                    }),
                    sync_market(
                        operation_id,
                        global_context,
                        market_outpoint_from_tx_id(tx_id),
                        NewMarketState::Complete,
                    ),
                ]
            }
            NewMarketState::Complete => vec![],
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Decodable, Encodable)]
pub enum NewOrderState {
    Pending {
        tx_id: TransactionId,
        order_id: OrderId,
        orders_to_sync_on_accepted: BTreeMap<OrderId, PublicKey>,
    },
    Rejected {
        order_id: OrderId,
    },
    Accepted {
        order_id: OrderId,
        orders_to_sync_on_accepted: BTreeMap<OrderId, PublicKey>,
    },
    SyncDone {
        order_id: OrderId,
    },
    Complete,
}

impl Into<PredictionMarketState> for NewOrderState {
    fn into(self) -> PredictionMarketState {
        PredictionMarketState::NewOrder(self)
    }
}
impl StateCategoryTrait for NewOrderState {
    fn transitions(
        self,
        operation_id: OperationId,
        context: &PredictionMarketsClientContext,
        global_context: &DynGlobalClientContext,
    ) -> Vec<StateTransition<PredictionMarketsStateMachine>> {
        match self {
            NewOrderState::Pending {
                tx_id,
                order_id,
                orders_to_sync_on_accepted,
            } => vec![await_tx_accepted(
                operation_id,
                global_context,
                tx_id,
                Self::Accepted {
                    order_id,
                    orders_to_sync_on_accepted,
                },
                Self::Rejected { order_id },
            )],
            NewOrderState::Rejected { order_id } => {
                vec![StateTransition::new(async {}, move |dbtx, _, _| {
                    Box::pin(async move {
                        dbtx.module_tx().remove_entry(&db::OrderKey(order_id)).await;
                        PredictionMarketsStateMachine {
                            operation_id,
                            state: Self::Complete.into(),
                        }
                    })
                })]
            }
            NewOrderState::Accepted {
                order_id,
                orders_to_sync_on_accepted,
            } => vec![sync_orders(
                operation_id,
                global_context,
                orders_to_sync_on_accepted,
                Self::SyncDone { order_id },
            )],
            NewOrderState::SyncDone { order_id } => {
                let new_order_broadcast_sender = context.new_order_broadcast_sender.clone();
                vec![StateTransition::new(async {}, move |_, _, _| {
                    let new_order_broadcast_sender = new_order_broadcast_sender.clone();
                    Box::pin(async move {
                        _ = new_order_broadcast_sender.send(order_id);

                        PredictionMarketsStateMachine {
                            operation_id,
                            state: Self::Complete.into(),
                        }
                    })
                })]
            }
            NewOrderState::Complete => vec![],
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Decodable, Encodable)]
pub enum CancelOrderState {
    Pending {
        tx_id: TransactionId,
        order_to_sync_on_accepted: (OrderId, PublicKey),
    },
    Rejected,
    Accepted {
        order_to_sync_on_accepted: (OrderId, PublicKey),
    },
    Complete,
}

impl Into<PredictionMarketState> for CancelOrderState {
    fn into(self) -> PredictionMarketState {
        PredictionMarketState::CancelOrder(self)
    }
}
impl StateCategoryTrait for CancelOrderState {
    fn transitions(
        self,
        operation_id: OperationId,
        _context: &PredictionMarketsClientContext,
        global_context: &DynGlobalClientContext,
    ) -> Vec<StateTransition<PredictionMarketsStateMachine>> {
        match self {
            CancelOrderState::Pending {
                tx_id,
                order_to_sync_on_accepted,
            } => vec![await_tx_accepted(
                operation_id,
                global_context,
                tx_id,
                Self::Accepted {
                    order_to_sync_on_accepted,
                },
                Self::Rejected,
            )],
            CancelOrderState::Rejected => vec![do_nothing(operation_id, Self::Complete)],
            CancelOrderState::Accepted {
                order_to_sync_on_accepted,
            } => vec![sync_orders(
                operation_id,
                global_context,
                iter::once(order_to_sync_on_accepted).collect(),
                Self::Complete,
            )],
            CancelOrderState::Complete => vec![],
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Decodable, Encodable)]
pub enum ConsumeOrderBitcoinBalanceState {
    Pending {
        tx_id: TransactionId,
        order_to_sync_on_accepted: (OrderId, PublicKey),
    },
    Rejected,
    Accepted {
        order_to_sync_on_accepted: (OrderId, PublicKey),
    },
    Complete,
}

impl Into<PredictionMarketState> for ConsumeOrderBitcoinBalanceState {
    fn into(self) -> PredictionMarketState {
        PredictionMarketState::ConsumeOrderBitcoinBalance(self)
    }
}
impl StateCategoryTrait for ConsumeOrderBitcoinBalanceState {
    fn transitions(
        self,
        operation_id: OperationId,
        _context: &PredictionMarketsClientContext,
        global_context: &DynGlobalClientContext,
    ) -> Vec<StateTransition<PredictionMarketsStateMachine>> {
        match self {
            ConsumeOrderBitcoinBalanceState::Pending {
                tx_id,
                order_to_sync_on_accepted,
            } => vec![await_tx_accepted(
                operation_id,
                global_context,
                tx_id,
                Self::Accepted {
                    order_to_sync_on_accepted,
                },
                Self::Rejected,
            )],
            ConsumeOrderBitcoinBalanceState::Rejected => {
                vec![do_nothing(operation_id, Self::Complete)]
            }
            ConsumeOrderBitcoinBalanceState::Accepted {
                order_to_sync_on_accepted,
            } => vec![sync_orders(
                operation_id,
                global_context,
                iter::once(order_to_sync_on_accepted).collect(),
                Self::Complete,
            )],
            ConsumeOrderBitcoinBalanceState::Complete => vec![],
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Decodable, Encodable)]
pub enum PayoutMarketState {
    Pending { tx_id: TransactionId },
    Rejected,
    Accepted { tx_id: TransactionId },
    Complete,
}

impl Into<PredictionMarketState> for PayoutMarketState {
    fn into(self) -> PredictionMarketState {
        PredictionMarketState::PayoutMarket(self)
    }
}
impl StateCategoryTrait for PayoutMarketState {
    fn transitions(
        self,
        operation_id: OperationId,
        _context: &PredictionMarketsClientContext,
        global_context: &DynGlobalClientContext,
    ) -> Vec<StateTransition<PredictionMarketsStateMachine>> {
        match self {
            PayoutMarketState::Pending { tx_id } => vec![await_tx_accepted(
                operation_id,
                global_context,
                tx_id,
                Self::Accepted { tx_id },
                Self::Rejected,
            )],
            PayoutMarketState::Rejected => vec![do_nothing(operation_id, Self::Complete)],
            PayoutMarketState::Accepted { tx_id } => {
                vec![sync_market(
                    operation_id,
                    global_context,
                    market_outpoint_from_tx_id(tx_id),
                    Self::Complete,
                )]
            }
            PayoutMarketState::Complete => vec![],
        }
    }
}

// #[derive(Debug, Clone, Eq, PartialEq, Hash, Decodable, Encodable)]
// pub enum FILLState {
//
// }

// impl Into<PredictionMarketState> for FILLState {
//     fn into(self) -> PredictionMarketState {
//         PredictionMarketState::FILL(self)
//     }
// }
// impl StateCategoryTrait for FILLState {
//     fn transitions(
//         self,
//         operation_id: OperationId,
//         context: &PredictionMarketsClientContext,
//         global_context: &DynGlobalClientContext,
//     ) -> Vec<StateTransition<PredictionMarketsStateMachine>> {
//         match self {}
//     }
// }
