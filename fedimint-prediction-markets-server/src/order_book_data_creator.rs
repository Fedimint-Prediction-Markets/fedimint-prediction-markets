use std::collections::HashMap;

use fedimint_core::db::{DatabaseTransaction, IDatabaseTransactionOpsCoreTyped};
use fedimint_core::{Amount, OutPoint};
use fedimint_prediction_markets_common::config::GeneralConsensus;
use fedimint_prediction_markets_common::{ContractOfOutcomeAmount, Side};
use prediction_market_event::Outcome;

use crate::{db, MarketSpecificationsNeededForNewOrders};

pub struct OrderBookDataCreator {
    market: OutPoint,
    market_contract_price: Amount,
    round_increment: u64,
    addition: Option<((Outcome, Side, Amount), ContractOfOutcomeAmount)>,
    subtractions: HashMap<(Outcome, Side, Amount), ContractOfOutcomeAmount>,
}

impl OrderBookDataCreator {
    pub fn new(
        gc: &GeneralConsensus,
        market: OutPoint,
        market_specifications: &MarketSpecificationsNeededForNewOrders,
    ) -> Self {
        let round_increment = u64::max(
            1,
            market_specifications.contract_price.msats / gc.order_book_precision,
        );

        Self {
            market,
            market_contract_price: market_specifications.contract_price,
            round_increment,
            addition: None,
            subtractions: HashMap::new(),
        }
    }

    pub fn process_addition(
        &mut self,
        outcome: Outcome,
        side: Side,
        price: Amount,
        quantity: ContractOfOutcomeAmount,
    ) {
        if matches!(self.addition, Some(_)) {
            panic!("OrderBookDataCreator: addition should only be set once")
        }
        self.addition = Some(((outcome, side, price), quantity));
    }

    pub fn process_subtraction(
        &mut self,
        outcome: Outcome,
        side: Side,
        price: Amount,
        quantity: ContractOfOutcomeAmount,
    ) {
        if !self.subtractions.contains_key(&(outcome, side, price)) {
            self.subtractions
                .insert((outcome, side, price), ContractOfOutcomeAmount::ZERO);
        }

        let val = self.subtractions.get_mut(&(outcome, side, price)).unwrap();
        *val += quantity;
    }

    pub async fn save(self, dbtx: &mut DatabaseTransaction<'_>) {
        // addition
        {
            let Some(((outcome, side, price), quantity)) = self.addition else {
                panic!("OrderBookDataCreator: addition should always be set")
            };

            let price = round_price_down(
                self.market_contract_price,
                self.round_increment,
                side,
                price,
            );
            let key = db::MarketOutcomeOrderBookKey {
                market: self.market,
                outcome,
                side,
                price,
            };

            let mut value = dbtx
                .get_value(&key)
                .await
                .unwrap_or(ContractOfOutcomeAmount::ZERO);

            value += quantity;

            dbtx.insert_entry(&key, &value).await;
        }

        // subtractions
        {
            for ((outcome, side, price), quantity) in self.subtractions {
                let price = round_price_down(
                    self.market_contract_price,
                    self.round_increment,
                    side,
                    price,
                );
                let key = db::MarketOutcomeOrderBookKey {
                    market: self.market,
                    outcome,
                    side,
                    price,
                };

                let mut value = dbtx
                    .get_value(&key)
                    .await
                    .expect("OrderBookDataCreator: tried to subtract from non existing quantity in order book");

                value -= quantity;

                if value == ContractOfOutcomeAmount::ZERO {
                    dbtx.remove_entry(&key).await;
                } else {
                    dbtx.insert_entry(&key, &value).await;
                }
            }
        }
    }
}

fn round_price_down(
    market_contract_price: Amount,
    round_increment: u64,
    side: Side,
    price: Amount,
) -> Amount {
    let msats = match side {
        Side::Buy => (price.msats / round_increment) * round_increment,
        Side::Sell => {
            market_contract_price.msats
                - (((market_contract_price - price).msats / round_increment) * round_increment)
        }
    };

    Amount { msats }
}
