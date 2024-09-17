use fedimint_core::db::{DatabaseTransaction, IDatabaseTransactionOpsCoreTyped};
use fedimint_core::{Amount, OutPoint};
use fedimint_prediction_markets_common::config::GeneralConsensus;
use fedimint_prediction_markets_common::{
    Candlestick, ContractOfOutcomeAmount, Outcome, Seconds, UnixTimestamp,
};
use futures::{future, StreamExt};

use crate::{db, MarketSpecificationsNeededForNewOrders};

pub struct CandlestickDataCreator {
    market: OutPoint,
    consensus_max_candlesticks_kept_per_market_outcome_interval: u64,
    consensus_timestamp: UnixTimestamp,

    candlestick_intervals: Vec<(
        // candlestick interval
        Seconds,
        // outcome to candlstick
        Vec<Option<Candlestick>>,
    )>,
}

impl CandlestickDataCreator {
    pub fn new(
        gc: &GeneralConsensus,
        consensus_timestamp: UnixTimestamp,
        market: OutPoint,
        market_specifications: &MarketSpecificationsNeededForNewOrders,
    ) -> Self {
        Self {
            market,
            consensus_max_candlesticks_kept_per_market_outcome_interval: gc
                .max_candlesticks_kept_per_market_outcome_interval,
            consensus_timestamp,

            candlestick_intervals: gc
                .candlestick_intervals
                .iter()
                .map(|candlestick_interval_seconds| {
                    (
                        *candlestick_interval_seconds,
                        vec![None; market_specifications.outcome_count.into()],
                    )
                })
                .collect(),
        }
    }

    pub async fn add(
        &mut self,
        dbtx: &mut DatabaseTransaction<'_>,
        outcome: Outcome,
        price: Amount,
        volume: ContractOfOutcomeAmount,
    ) {
        for (candlestick_interval_seconds, candlesticks_by_outcome) in
            self.candlestick_intervals.iter_mut()
        {
            let candlestick_timestamp = self
                .consensus_timestamp
                .round_down(*candlestick_interval_seconds);

            let candlestick_opt = candlesticks_by_outcome
                .get_mut::<usize>(outcome.into())
                .expect("vec's length is number of outcomes");

            if let None = candlestick_opt {
                let candlestick_in_db_or_new = dbtx
                    .get_value(&db::MarketOutcomeCandlesticksKey {
                        market: self.market,
                        outcome,
                        candlestick_interval: *candlestick_interval_seconds,
                        candlestick_timestamp,
                    })
                    .await
                    .unwrap_or(Candlestick {
                        open: price,
                        close: price,
                        high: price,
                        low: price,
                        volume: ContractOfOutcomeAmount::ZERO,
                    });

                *candlestick_opt = Some(candlestick_in_db_or_new);
            }

            let Some(candlestick) = candlestick_opt else {
                panic!("candlestick should always be some")
            };
            candlestick.close = price;
            candlestick.high = candlestick.high.max(price);
            candlestick.low = candlestick.low.min(price);
            candlestick.volume = candlestick.volume + volume;
        }
    }

    pub async fn save(mut self, dbtx: &mut DatabaseTransaction<'_>) {
        self.remove_old_candlesticks(dbtx).await;

        for (candlestick_interval, candlesticks_by_outcome) in self.candlestick_intervals {
            let candlestick_timestamp = self.consensus_timestamp.round_down(candlestick_interval);

            for (i, candlestick_opt) in candlesticks_by_outcome.into_iter().enumerate() {
                let Some(candlestick) = candlestick_opt else {
                    continue;
                };

                dbtx.insert_entry(
                    &db::MarketOutcomeCandlesticksKey {
                        market: self.market,
                        outcome: i as Outcome,
                        candlestick_interval,
                        candlestick_timestamp,
                    },
                    &candlestick,
                )
                .await;

                dbtx.insert_entry(
                    &db::MarketOutcomeNewestCandlestickVolumeKey {
                        market: self.market,
                        outcome: i as Outcome,
                        candlestick_interval,
                    },
                    &(candlestick_timestamp, candlestick.volume),
                )
                .await;
            }
        }
    }

    pub async fn remove_old_candlesticks(&mut self, dbtx: &mut DatabaseTransaction<'_>) {
        for (candlestick_interval, candlesticks_by_outcome) in self.candlestick_intervals.iter() {
            let candlestick_timestamp = self.consensus_timestamp.round_down(*candlestick_interval);

            let min_candlestick_timestamp = UnixTimestamp(candlestick_timestamp.0.saturating_sub(
                candlestick_interval
                    * self.consensus_max_candlesticks_kept_per_market_outcome_interval,
            ));

            for outcome in 0..candlesticks_by_outcome.len() {
                let keys_to_remove = dbtx
                    .find_by_prefix(&db::MarketOutcomeCandlesticksPrefix3 {
                        market: self.market,
                        outcome: outcome as Outcome,
                        candlestick_interval: *candlestick_interval,
                    })
                    .await
                    .map(|(k, _)| k)
                    .take_while(|k| {
                        future::ready(k.candlestick_timestamp < min_candlestick_timestamp)
                    })
                    .collect::<Vec<_>>()
                    .await;

                for key in keys_to_remove {
                    dbtx.remove_entry(&key)
                        .await
                        .expect("should always be some");
                }
            }
        }
    }
}
