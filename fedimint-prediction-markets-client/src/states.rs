use fedimint_client::sm::{DynState, State, StateTransition};
use fedimint_client::DynGlobalClientContext;
use fedimint_core::core::{IntoDynInstance, ModuleInstanceId, OperationId};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::TransactionId;

use crate::OrderId;
// use serde::{Deserialize, Serialize};
// use thiserror::Error;
use crate::{PredictionMarketsClientContext, PredictionMarketsClientModule};

/// Tracks a transaction.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Decodable, Encodable)]
pub struct PredictionMarketsStateMachine {
    pub operation_id: OperationId,
    pub state: PredictionMarketState,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Decodable, Encodable)]
pub enum PredictionMarketState {
    NewMarket {
        tx_id: TransactionId,
    },
    NewMarketAccepted,
    NewMarketFailed,

    PayoutMarket {
        tx_id: TransactionId,
    },
    PayoutMarketAccepted,
    PayoutMarketFailed,

    NewOrder {
        tx_id: TransactionId,
        order: OrderId,
        sources: Vec<OrderId>,
    },
    NewOrderAccepted,
    NewOrderFailed,

    CancelOrder {
        tx_id: TransactionId,
        order: OrderId,
    },
    CancelOrderAccepted,
    CancelOrderFailed,

    ConsumeOrderBitcoinBalance {
        tx_id: TransactionId,
        order: OrderId,
    },
    ConsumeOrderBitcoinBalanceAccepted,
    ConsumeOrderBitcoinBalanceFailed,
}

impl State for PredictionMarketsStateMachine {
    type ModuleContext = PredictionMarketsClientContext;

    fn transitions(
        &self,
        _context: &Self::ModuleContext,
        global_context: &DynGlobalClientContext,
    ) -> Vec<StateTransition<Self>> {
        let operation_id = self.operation_id;

        match self.state.clone() {
            PredictionMarketState::NewMarket { tx_id } => {
                vec![StateTransition::new(
                    await_tx_accepted(global_context.clone(), operation_id, tx_id),
                    move |_dbtx, res, _state_machine: Self| match res {
                        // tx accepted
                        Ok(_) => Box::pin(async move {
                            Self {
                                operation_id,
                                state: PredictionMarketState::NewMarketAccepted,
                            }
                        }),
                        // tx rejected
                        Err(_) => Box::pin(async move {
                            Self {
                                operation_id,
                                state: PredictionMarketState::NewMarketFailed,
                            }
                        }),
                    },
                )]
            }
            PredictionMarketState::NewMarketAccepted => vec![],
            PredictionMarketState::NewMarketFailed => vec![],

            PredictionMarketState::PayoutMarket { tx_id } => {
                vec![StateTransition::new(
                    await_tx_accepted(global_context.clone(), operation_id, tx_id),
                    move |_dbtx, res, _state: Self| match res {
                        // tx accepted
                        Ok(_) => Box::pin(async move {
                            Self {
                                operation_id,
                                state: PredictionMarketState::PayoutMarketAccepted,
                            }
                        }),
                        // tx rejected
                        Err(_) => Box::pin(async move {
                            Self {
                                operation_id,
                                state: PredictionMarketState::PayoutMarketFailed,
                            }
                        }),
                    },
                )]
            }
            PredictionMarketState::PayoutMarketAccepted => vec![],
            PredictionMarketState::PayoutMarketFailed => vec![],

            PredictionMarketState::NewOrder {
                tx_id,
                order,
                sources,
            } => {
                vec![StateTransition::new(
                    await_tx_accepted(global_context.clone(), operation_id, tx_id),
                    move |dbtx, res, _state: Self| match res {
                        // tx accepted
                        Ok(_) => {
                            let mut changed_orders = Vec::new();
                            changed_orders.push(order);
                            changed_orders.append(&mut sources.clone());

                            Box::pin(async move {
                                PredictionMarketsClientModule::set_order_needs_update(
                                    dbtx.module_tx(),
                                    changed_orders,
                                )
                                .await;
                                Self {
                                    operation_id,
                                    state: PredictionMarketState::NewOrderAccepted,
                                }
                            })
                        }
                        // tx rejected
                        Err(_) => Box::pin(async move {
                            PredictionMarketsClientModule::unreserve_order_id_slot(
                                dbtx.module_tx(),
                                order,
                            )
                            .await;
                            Self {
                                operation_id,
                                state: PredictionMarketState::NewOrderFailed,
                            }
                        }),
                    },
                )]
            }
            PredictionMarketState::NewOrderAccepted => vec![],
            PredictionMarketState::NewOrderFailed => vec![],

            PredictionMarketState::CancelOrder { tx_id, order } => {
                vec![StateTransition::new(
                    await_tx_accepted(global_context.clone(), operation_id, tx_id),
                    move |dbtx, res, _state: Self| match res {
                        // tx accepted
                        Ok(_) => Box::pin(async move {
                            PredictionMarketsClientModule::set_order_needs_update(
                                dbtx.module_tx(),
                                vec![order],
                            )
                            .await;
                            Self {
                                operation_id,
                                state: PredictionMarketState::CancelOrderAccepted,
                            }
                        }),
                        // tx rejected
                        Err(_) => Box::pin(async move {
                            Self {
                                operation_id,
                                state: PredictionMarketState::CancelOrderFailed,
                            }
                        }),
                    },
                )]
            }
            PredictionMarketState::CancelOrderAccepted => vec![],
            PredictionMarketState::CancelOrderFailed => vec![],

            PredictionMarketState::ConsumeOrderBitcoinBalance { tx_id, order } => {
                vec![StateTransition::new(
                    await_tx_accepted(global_context.clone(), operation_id, tx_id),
                    move |dbtx, res, _state: Self| match res {
                        // tx accepted
                        Ok(_) => Box::pin(async move {
                            PredictionMarketsClientModule::set_order_needs_update(
                                dbtx.module_tx(),
                                vec![order],
                            )
                            .await;
                            Self {
                                operation_id,
                                state: PredictionMarketState::ConsumeOrderBitcoinBalanceAccepted,
                            }
                        }),
                        // tx rejected
                        Err(_) => Box::pin(async move {
                            Self {
                                operation_id,
                                state: PredictionMarketState::ConsumeOrderBitcoinBalanceFailed,
                            }
                        }),
                    },
                )]
            }
            PredictionMarketState::ConsumeOrderBitcoinBalanceAccepted => vec![],
            PredictionMarketState::ConsumeOrderBitcoinBalanceFailed => vec![],
        }
    }

    fn operation_id(&self) -> OperationId {
        self.operation_id
    }
}

async fn await_tx_accepted(
    context: DynGlobalClientContext,
    _id: OperationId,
    txid: TransactionId,
) -> Result<(), String> {
    context.await_tx_accepted(txid).await
}

impl IntoDynInstance for PredictionMarketsStateMachine {
    type DynType = DynState;

    fn into_dyn(self, instance_id: ModuleInstanceId) -> Self::DynType {
        DynState::from_typed(instance_id, self)
    }
}
