use std::collections::BTreeSet;

use fedimint_client::sm::StateTransition;
use fedimint_client::DynGlobalClientContext;
use fedimint_core::core::OperationId;
use fedimint_core::db::IDatabaseTransactionOpsCoreTyped;
use fedimint_core::{OutPoint, TransactionId};

use super::triggers::{await_market_from_federation, await_orders_from_federation};
use super::{PredictionMarketState, PredictionMarketsStateMachine};
use crate::{db, OrderId, PredictionMarketsClientContext};

pub fn await_tx_accepted(
    operation_id: OperationId,
    global_context: &DynGlobalClientContext,
    tx_id: TransactionId,
    accepted: impl Into<PredictionMarketState>,
    rejected: impl Into<PredictionMarketState>,
) -> StateTransition<PredictionMarketsStateMachine> {
    let accepted_next_state = accepted.into();
    let rejected_next_state = rejected.into();
    let global_context = global_context.clone();

    StateTransition::new(
        async move { global_context.await_tx_accepted(tx_id).await },
        move |_dbtx, res, _state| {
            let accepted_next_state = accepted_next_state.clone();
            let rejected_next_state = rejected_next_state.clone();

            Box::pin(async move {
                match res {
                    Ok(_) => PredictionMarketsStateMachine {
                        operation_id,
                        state: accepted_next_state,
                    },
                    Err(_) => PredictionMarketsStateMachine {
                        operation_id,
                        state: rejected_next_state,
                    },
                }
            })
        },
    )
}

pub fn sync_orders(
    operation_id: OperationId,
    context: &PredictionMarketsClientContext,
    global_context: &DynGlobalClientContext,
    orders: BTreeSet<OrderId>,
    next: impl Into<PredictionMarketState>,
) -> StateTransition<PredictionMarketsStateMachine> {
    let next = next.into();

    StateTransition::new(
        await_orders_from_federation(context.clone(), global_context.clone(), orders),
        move |dbtx, orders, _state| {
            let next = next.clone();

            Box::pin(async move {
                for (order_id, order) in orders {
                    crate::PredictionMarketsClientModule::save_order_to_db(
                        &mut dbtx.module_tx(),
                        order_id,
                        &order,
                    )
                    .await;
                }

                PredictionMarketsStateMachine {
                    operation_id,
                    state: next,
                }
            })
        },
    )
}

pub fn sync_market(
    operation_id: OperationId,
    global_context: &DynGlobalClientContext,
    market: OutPoint,
    next: impl Into<PredictionMarketState>,
) -> StateTransition<PredictionMarketsStateMachine> {
    let next = next.into();
    let market_outpoint = market;

    StateTransition::new(
        await_market_from_federation(global_context.clone(), market_outpoint),
        move |dbtx, market, _| {
            let next = next.clone();

            Box::pin(async move {
                dbtx.module_tx()
                    .insert_entry(&db::MarketKey(market_outpoint), &market)
                    .await;

                PredictionMarketsStateMachine {
                    operation_id,
                    state: next,
                }
            })
        },
    )
}

pub fn do_nothing(
    operation_id: OperationId,
    next: impl Into<PredictionMarketState>,
) -> StateTransition<PredictionMarketsStateMachine> {
    let next = next.into();

    StateTransition::new(async {}, move |_, _, _| {
        let next = next.clone();

        Box::pin(async move {
            PredictionMarketsStateMachine {
                operation_id,
                state: next,
            }
        })
    })
}
