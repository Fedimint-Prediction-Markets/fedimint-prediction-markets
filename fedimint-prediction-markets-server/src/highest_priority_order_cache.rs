use fedimint_prediction_markets_common::Outcome;
use secp256k1::PublicKey;

use crate::MarketSpecificationsNeededForNewOrders;

pub struct HighestPriorityOrderCache {
    v: Vec<Option<PublicKey>>,
}

impl HighestPriorityOrderCache {
    pub fn new(market_specifications: &MarketSpecificationsNeededForNewOrders) -> Self {
        Self {
            v: vec![None; market_specifications.outcome_count.into()],
        }
    }

    pub fn set(&mut self, outcome: Outcome, order_owner: Option<PublicKey>) {
        let v = self
            .v
            .get_mut::<usize>(outcome.into())
            .expect("vec's length is number of outcomes");
        *v = order_owner;
    }

    pub fn get<'a>(&'a self, outcome: Outcome) -> &'a Option<PublicKey> {
        self.v
            .get::<usize>(outcome.into())
            .expect("vec's length is number of outcomes")
    }
}
