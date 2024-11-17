use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::{Amount, OutPoint};
use fedimint_prediction_markets_common::{ContractOfOutcomeAmount, Order, Side};
use prediction_market_event::Outcome;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub struct OrderFilter(pub OrderPath, pub OrderState);

impl OrderFilter {
    pub fn filter(&self, order: &Order) -> bool {
        let res = match &self.0 {
            OrderPath::All => true,
            OrderPath::Market { market } => &order.market == market,
            OrderPath::MarketOutcome { market, outcome } => {
                &order.market == market && &order.outcome == outcome
            }
            OrderPath::MarketOutcomeSide {
                market,
                outcome,
                side,
            } => &order.market == market && &order.outcome == outcome && &order.side == side,
        } && match &self.1 {
            OrderState::Any => true,
            OrderState::NonZeroQuantityWaitingForMatch => {
                order.quantity_waiting_for_match != ContractOfOutcomeAmount::ZERO
            }
            OrderState::NonZeroContractOfOutcomeBalance => {
                order.contract_of_outcome_balance != ContractOfOutcomeAmount::ZERO
            }
            OrderState::NonZeroBitcoinBalance => order.bitcoin_balance != Amount::ZERO,
        };

        res
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub enum OrderPath {
    All,
    Market {
        market: OutPoint,
    },
    MarketOutcome {
        market: OutPoint,
        outcome: Outcome,
    },
    MarketOutcomeSide {
        market: OutPoint,
        outcome: Outcome,
        side: Side,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encodable, Decodable, PartialEq, Eq, Hash)]
pub enum OrderState {
    Any,
    NonZeroQuantityWaitingForMatch,
    NonZeroContractOfOutcomeBalance,
    NonZeroBitcoinBalance,
}
