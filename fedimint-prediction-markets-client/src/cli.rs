use std::{ffi, iter};

use clap::Parser;
use fedimint_core::{Amount, OutPoint, TransactionId};
use fedimint_prediction_markets_common::{EventJson, NostrPublicKeyHex};
use serde::Serialize;
use serde_json::Value;

use crate::PredictionMarketsClientModule;

#[derive(Parser, Serialize)]
enum Opts {
    NewMarket {
        event_json: EventJson,
        contract_price: Amount,
        payout_control: NostrPublicKeyHex,
    },
    GetMarket {
        market_txid: TransactionId,
        #[clap(short, long)]
        from_local_cache: bool,
    },
}

pub async fn handle_cli_command(
    prediction_markets: &PredictionMarketsClientModule,
    args: &[ffi::OsString],
) -> anyhow::Result<serde_json::Value> {
    let opts =
        Opts::parse_from(iter::once(&ffi::OsString::from("prediction-markets")).chain(args.iter()));

    let value = match opts {
        Opts::NewMarket {
            event_json,
            contract_price,
            payout_control,
        } => {
            let payout_control_weight_map = vec![(payout_control, 1u16)].into_iter().collect();
            let weight_required_for_payout = 1;

            let result = prediction_markets
                .new_market(
                    event_json,
                    contract_price,
                    payout_control_weight_map,
                    weight_required_for_payout,
                )
                .await;
            json(result?)
        }
        Opts::GetMarket {
            market_txid,
            from_local_cache,
        } => {
            let market_outpoint = OutPoint {
                txid: market_txid,
                out_idx: 0,
            };

            let result = prediction_markets
                .get_market(market_outpoint, from_local_cache)
                .await;
            json(result?)
        }
    };

    Ok(value)
}

fn json<T: Serialize>(value: T) -> Value {
    serde_json::to_value(value).expect("JSON serialization failed")
}
