use std::time::Duration;

use fedimint_core::task::sleep;
use fedimint_core::util::NextOrPending;
use fedimint_core::Amount;
use fedimint_dummy_client::common::config::DummyGenParams;
use fedimint_dummy_client::{DummyClientInit, DummyClientModule};
use fedimint_dummy_server::DummyInit;
use fedimint_prediction_markets_client::{
    block_till_reciever_closed, OrderFilter, PredictionMarketsClientInit,
    PredictionMarketsClientModule,
};
use fedimint_prediction_markets_common::config::PredictionMarketsGenParams;
use fedimint_prediction_markets_common::{ContractOfOutcomeAmount, Side, UnixTimestamp};
use fedimint_prediction_markets_server::PredictionMarketsInit;
use fedimint_testing::fixtures::Fixtures;
use prediction_market_event::information::Information;
use prediction_market_event::Event;
use prediction_market_event_nostr_client::nostr_sdk::Keys;
use tokio::spawn;
use tracing::info;

fn fixtures() -> Fixtures {
    Fixtures::new_primary(DummyClientInit, DummyInit, DummyGenParams::default()).with_module(
        PredictionMarketsClientInit,
        PredictionMarketsInit,
        PredictionMarketsGenParams::default(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn create_market_and_get_market() -> anyhow::Result<()> {
    let fed = fixtures().new_default_fed().await;
    let (client1, client2) = fed.two_clients().await;

    let client1_pm = client1.get_first_module::<PredictionMarketsClientModule>();
    let client2_pm = client2.get_first_module::<PredictionMarketsClientModule>();

    let payout_control = Keys::generate();
    let market_outpoint = client1_pm
        .new_market(
            Event::new_with_random_nonce(2, 1, Information::None).try_to_json_string()?,
            Amount::from_msats(100),
            vec![(payout_control.public_key.to_hex(), 1u16)]
                .into_iter()
                .collect(),
            1,
        )
        .await?;

    let market = client2_pm.get_market(market_outpoint, false).await?;
    println!("{market:?}");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn candlestick_stream() -> anyhow::Result<()> {
    let fed = fixtures().new_default_fed().await;
    let (client1, client2) = fed.two_clients().await;

    let client1_dummy = client1.get_first_module::<DummyClientModule>();
    client1_dummy.print_money(Amount::from_sats(1000)).await?;

    let client1_pm = client1.get_first_module::<PredictionMarketsClientModule>();

    let payout_control = Keys::generate();
    let market = client1_pm
        .new_market(
            Event::new_with_random_nonce(2, 1, Information::None).try_to_json_string()?,
            Amount::from_msats(100),
            vec![(payout_control.public_key.to_hex(), 1u16)]
                .into_iter()
                .collect(),
            1,
        )
        .await?;

    spawn(async move {
        let client2_pm2 = client2.get_first_module::<PredictionMarketsClientModule>();
        let mut stream = client2_pm2
            .stream_candlesticks(market, 0, 15, UnixTimestamp::ZERO, Duration::from_secs(1))
            .await;
        loop {
            println!("{:?}", stream.ok().await);
        }
    });

    client1_pm
        .new_order(
            market,
            0,
            Side::Buy,
            Amount::from_msats(60),
            ContractOfOutcomeAmount(10),
        )
        .await?;
    for _ in 0..10 {
        client1_pm
            .new_order(
                market,
                1,
                Side::Buy,
                Amount::from_msats(40),
                ContractOfOutcomeAmount(1),
            )
            .await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn order_stream() -> anyhow::Result<()> {
    let fed = fixtures().new_default_fed().await;
    let client1 = fed.new_client().await;

    let client1_dummy = client1.get_first_module::<DummyClientModule>();
    client1_dummy.print_money(Amount::from_sats(1000)).await?;

    let client1_pm = client1.get_first_module::<PredictionMarketsClientModule>();
    let payout_control = Keys::generate();
    let market = client1_pm
        .new_market(
            Event::new_with_random_nonce(2, 1, Information::None).try_to_json_string()?,
            Amount::from_msats(100),
            vec![(payout_control.public_key.to_hex(), 1u16)]
                .into_iter()
                .collect(),
            1,
        )
        .await?;

    let stop_watch_order_matches = client1_pm
        .watch_orders_on_market_outcome_side(market, 0, Side::Buy)
        .await?;

    // let mut order_0_stream = client1_pm.stream_order_from_db(OrderId(0)).await;
    // spawn(async move {
    //     loop {
    //         let order = order_0_stream.next_or_pending().await;
    //         dbg!(order);
    //     }
    // });

    client1_pm
        .new_order(
            market,
            0,
            Side::Buy,
            Amount::from_msats(60),
            ContractOfOutcomeAmount(1000),
        )
        .await?;

    for _ in 0..10 {
        client1_pm
            .new_order(
                market,
                1,
                Side::Buy,
                Amount::from_msats(40),
                ContractOfOutcomeAmount(1),
            )
            .await?;
    }

    let (mut stream_of_order_streams, stop_order_streams) =
        client1_pm.stream_orders_from_db(OrderFilter::All).await;
    spawn(async move {
        loop {
            let (order_id, mut order_stream) = stream_of_order_streams.next_or_pending().await;
            spawn(async move {
                loop {
                    let order = order_stream.next_or_pending().await;
                    info!(
                        "{}: {}",
                        order_id.0,
                        order.unwrap().quantity_waiting_for_match.0
                    );
                }
            });
        }
    });

    for _ in 0..1000 {
        let res =  client1_pm
            .new_order(
                market,
                1,
                Side::Buy,
                Amount::from_msats(40),
                ContractOfOutcomeAmount(1),
            )
            .await;

        info!("order create: {res:?}");

        sleep(Duration::from_millis(10)).await;
    }

    block_till_reciever_closed(stop_watch_order_matches).await?;
    block_till_reciever_closed(stop_order_streams).await?;
    info!("test_main end");

    Ok(())
}
