use std::collections::HashMap;

use fedimint_core::db::{DatabaseTransaction, IDatabaseTransactionOpsCoreTyped};
use fedimint_prediction_markets_common::Order;
use secp256k1::PublicKey;

use crate::db;

pub struct OrderCache {
    m: HashMap<PublicKey, Order>,
    mut_orders: HashMap<PublicKey, ()>,
}

impl OrderCache {
    pub fn new() -> Self {
        Self {
            m: HashMap::new(),
            mut_orders: HashMap::new(),
        }
    }

    pub async fn get<'a>(
        &'a mut self,
        dbtx: &mut DatabaseTransaction<'_>,
        order_owner: &PublicKey,
    ) -> &'a Order {
        if !self.m.contains_key(order_owner) {
            let order = dbtx
                .get_value(&db::OrderKey(*order_owner))
                .await
                .expect("OrderCache always expects order to exist");
            self.m.insert(*order_owner, order);
        }

        self.m.get(order_owner).unwrap()
    }

    pub async fn get_mut<'a>(
        &'a mut self,
        dbtx: &mut DatabaseTransaction<'_>,
        order_owner: &PublicKey,
    ) -> &'a mut Order {
        if !self.m.contains_key(order_owner) {
            let order = dbtx
                .get_value(&db::OrderKey(*order_owner))
                .await
                .expect("OrderCache always expects order to exist");
            self.m.insert(*order_owner, order);
        }

        self.mut_orders.insert(*order_owner, ());

        self.m.get_mut(order_owner).unwrap()
    }

    pub async fn save(self, dbtx: &mut DatabaseTransaction<'_>) {
        for (order_owner, _) in self.mut_orders {
            let order = self.m.get(&order_owner).unwrap();
            dbtx.insert_entry(&db::OrderKey(order_owner), order).await;
        }
    }
}
