use fedimint_client::sm::{DynState, State, StateTransition};
use fedimint_client::DynGlobalClientContext;
use fedimint_core::core::{IntoDynInstance, ModuleInstanceId, OperationId};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::TransactionId;
use fedimint_prediction_markets_common::OrderIdClientSide;

// use serde::{Deserialize, Serialize};
// use thiserror::Error;
use crate::{PredictionMarketsClientContext, PredictionMarketsClientModule};

/// Tracks a transaction.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Decodable, Encodable)]
pub enum PredictionMarketsStateMachine {
    NewMarket {
        operation_id: OperationId,
        tx_id: TransactionId,
    },
    NewMarketAccepted(OperationId),
    NewMarketFailed(OperationId),

    ProposePayout {
        operation_id: OperationId,
        tx_id: TransactionId,
    },
    ProposePayoutAccepted(OperationId),
    ProposePayoutFailed(OperationId),

    NewOrder {
        operation_id: OperationId,
        tx_id: TransactionId,
        order: OrderIdClientSide,
        sources: Vec<OrderIdClientSide>,
    },
    NewOrderAccepted(OperationId),
    NewOrderFailed(OperationId),

    CancelOrder {
        operation_id: OperationId,
        tx_id: TransactionId,
        order: OrderIdClientSide,
    },
    CancelOrderAccepted(OperationId),
    CancelOrderFailed(OperationId),

    ConsumeOrderBitcoinBalance {
        operation_id: OperationId,
        tx_id: TransactionId,
        order: OrderIdClientSide,
    },
    ConsumeOrderBitcoinBalanceAccepted(OperationId),
    ConsumeOrderBitcoinBalanceFailed(OperationId),

    ConsumePayoutControlBitcoinBalance {
        operation_id: OperationId,
        tx_id: TransactionId,
    },
    ConsumePayoutControlBitcoinBalanceAccepted(OperationId),
    ConsumePayoutControlBitcoinBalanceFailed(OperationId),
}

impl State for PredictionMarketsStateMachine {
    type ModuleContext = PredictionMarketsClientContext;

    fn transitions(
        &self,
        _context: &Self::ModuleContext,
        global_context: &DynGlobalClientContext,
    ) -> Vec<StateTransition<Self>> {
        match self.clone() {
            Self::NewMarket {
                operation_id,
                tx_id,
            } => {
                vec![StateTransition::new(
                    await_tx_accepted(global_context.clone(), operation_id, tx_id),
                    move |_dbtx, res, _state: Self| match res {
                        // tx accepted
                        Ok(_) => Box::pin(async move {
                            PredictionMarketsStateMachine::NewMarketAccepted(operation_id)
                        }),
                        // tx rejected
                        Err(_) => Box::pin(async move {
                            PredictionMarketsStateMachine::NewMarketFailed(operation_id)
                        }),
                    },
                )]
            }
            Self::NewMarketAccepted(_) => vec![],
            Self::NewMarketFailed(_) => vec![],

            Self::ProposePayout {
                operation_id,
                tx_id,
            } => {
                vec![StateTransition::new(
                    await_tx_accepted(global_context.clone(), operation_id, tx_id),
                    move |_dbtx, res, _state: Self| match res {
                        // tx accepted
                        Ok(_) => Box::pin(async move {
                            PredictionMarketsStateMachine::ProposePayoutAccepted(operation_id)
                        }),
                        // tx rejected
                        Err(_) => Box::pin(async move {
                            PredictionMarketsStateMachine::ProposePayoutFailed(operation_id)
                        }),
                    },
                )]
            }
            Self::ProposePayoutAccepted(_) => vec![],
            Self::ProposePayoutFailed(_) => vec![],

            Self::NewOrder {
                operation_id,
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
                                PredictionMarketsStateMachine::NewOrderAccepted(operation_id)
                            })
                        }
                        // tx rejected
                        Err(_) => Box::pin(async move {
                            PredictionMarketsClientModule::unreserve_order_id_slot(
                                dbtx.module_tx(),
                                order,
                            )
                            .await;
                            PredictionMarketsStateMachine::NewOrderFailed(operation_id)
                        }),
                    },
                )]
            }
            Self::NewOrderAccepted(_) => vec![],
            Self::NewOrderFailed(_) => vec![],

            Self::CancelOrder {
                operation_id,
                tx_id,
                order,
            } => {
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
                            PredictionMarketsStateMachine::CancelOrderAccepted(operation_id)
                        }),
                        // tx rejected
                        Err(_) => Box::pin(async move {
                            PredictionMarketsStateMachine::CancelOrderFailed(operation_id)
                        }),
                    },
                )]
            }
            Self::CancelOrderAccepted(_) => vec![],
            Self::CancelOrderFailed(_) => vec![],

            Self::ConsumeOrderBitcoinBalance {
                operation_id,
                tx_id,
                order,
            } => {
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
                            PredictionMarketsStateMachine::ConsumeOrderBitcoinBalanceAccepted(
                                operation_id,
                            )
                        }),
                        // tx rejected
                        Err(_) => Box::pin(async move {
                            PredictionMarketsStateMachine::ConsumeOrderBitcoinBalanceFailed(
                                operation_id,
                            )
                        }),
                    },
                )]
            }
            Self::ConsumeOrderBitcoinBalanceAccepted(_) => vec![],
            Self::ConsumeOrderBitcoinBalanceFailed(_) => vec![],

            Self::ConsumePayoutControlBitcoinBalance {
                operation_id,
                tx_id,
            } => {
                vec![StateTransition::new(
                    await_tx_accepted(global_context.clone(), operation_id, tx_id),
                    move |_dbtx, res, _state: Self| match res {
                        // tx accepted
                        Ok(_) => Box::pin(async move {
                            PredictionMarketsStateMachine::ConsumePayoutControlBitcoinBalanceAccepted(
                                operation_id,
                            )
                        }),
                        // tx rejected
                        Err(_) => Box::pin(async move {
                            PredictionMarketsStateMachine::ConsumePayoutControlBitcoinBalanceFailed(
                                operation_id,
                            )
                        }),
                    },
                )]
            }
            Self::ConsumePayoutControlBitcoinBalanceAccepted(_) => vec![],
            Self::ConsumePayoutControlBitcoinBalanceFailed(_) => vec![],
        }
    }

    fn operation_id(&self) -> OperationId {
        match self {
            Self::NewMarket {
                operation_id,
                tx_id: _,
            } => *operation_id,
            Self::NewMarketAccepted(operation_id) => *operation_id,
            Self::NewMarketFailed(operation_id) => *operation_id,

            Self::ProposePayout {
                operation_id,
                tx_id: _,
            } => *operation_id,
            Self::ProposePayoutAccepted(operation_id) => *operation_id,
            Self::ProposePayoutFailed(operation_id) => *operation_id,

            Self::NewOrder {
                operation_id,
                tx_id: _,
                order: _,
                sources: _,
            } => *operation_id,
            Self::NewOrderAccepted(operation_id) => *operation_id,
            Self::NewOrderFailed(operation_id) => *operation_id,

            Self::CancelOrder {
                operation_id,
                tx_id: _,
                order: _,
            } => *operation_id,
            Self::CancelOrderAccepted(operation_id) => *operation_id,
            Self::CancelOrderFailed(operation_id) => *operation_id,

            Self::ConsumeOrderBitcoinBalance {
                operation_id,
                tx_id: _,
                order: _,
            } => *operation_id,
            Self::ConsumeOrderBitcoinBalanceAccepted(operation_id) => *operation_id,
            Self::ConsumeOrderBitcoinBalanceFailed(operation_id) => *operation_id,

            Self::ConsumePayoutControlBitcoinBalance {
                operation_id,
                tx_id: _,
            } => *operation_id,
            Self::ConsumePayoutControlBitcoinBalanceAccepted(operation_id) => *operation_id,
            Self::ConsumePayoutControlBitcoinBalanceFailed(operation_id) => *operation_id,
        }
    }
}

// TODO: Boiler-plate, should return OutputOutcome
async fn await_tx_accepted(
    context: DynGlobalClientContext,
    _id: OperationId,
    txid: TransactionId,
) -> Result<(), String> {
    context.await_tx_accepted(txid).await
}

// TODO: Boiler-plate
impl IntoDynInstance for PredictionMarketsStateMachine {
    type DynType = DynState;

    fn into_dyn(self, instance_id: ModuleInstanceId) -> Self::DynType {
        DynState::from_typed(instance_id, self)
    }
}
