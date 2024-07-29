use fedimint_prediction_markets_common::{Market, Outcome};
use secp256k1::PublicKey;


pub struct HighestPriorityOrderCache {
    v: Vec<Option<PublicKey>>,
}

impl HighestPriorityOrderCache {
    pub fn new(market: &Market) -> Self {
        Self {
            v: vec![None; market.outcomes.into()],
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