use std::collections::BTreeMap;
use std::iter;
use std::time::Duration;

use fedimint_client::ClientModuleInstance;
use fedimint_core::task::sleep;
use fedimint_core::util::NextOrPending;
use fedimint_core::Amount;
use fedimint_dummy_client::common::config::DummyGenParams;
use fedimint_dummy_client::{DummyClientInit, DummyClientModule};
use fedimint_dummy_server::DummyInit;
use fedimint_prediction_markets_client::order_filter::{OrderFilter, OrderPath, OrderState};
use fedimint_prediction_markets_client::{
    OrderId, PredictionMarketsClientInit, PredictionMarketsClientModule,
};
use fedimint_prediction_markets_common::config::PredictionMarketsGenParams;
use fedimint_prediction_markets_common::{
    ContractAmount, ContractOfOutcomeAmount, Market, MarketDynamic, MarketStatic,
    NostrPublicKeyHex, Order, Side, SignedAmount, UnixTimestamp, Weight,
};
use fedimint_prediction_markets_server::PredictionMarketsInit;
use fedimint_testing::fixtures::Fixtures;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
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
    let client1 = fed.new_client_rocksdb().await;
    let client2 = fed.new_client_rocksdb().await;

    let client1_pm = client1.get_first_module::<PredictionMarketsClientModule>();
    let client2_pm = client2.get_first_module::<PredictionMarketsClientModule>();

    let event_json = Event::new_with_random_nonce(2, 1, Information::None).try_to_json_string()?;
    let contract_price = Amount::from_msats(100);
    let payout_control_weight_map: BTreeMap<NostrPublicKeyHex, Weight> =
        iter::once((Keys::generate().public_key.to_hex(), 1u16)).collect();
    let weight_required_for_payout = 1;
    let market_outpoint = client1_pm
        .new_market(
            event_json.clone(),
            contract_price,
            payout_control_weight_map.clone(),
            weight_required_for_payout,
        )
        .await?;

    let market = client2_pm
        .get_market(market_outpoint, false)
        .await?
        .unwrap();
    let created_consensus_timestamp = market.0.created_consensus_timestamp;
    assert_eq!(
        market,
        Market(
            MarketStatic {
                event_json,
                contract_price,
                payout_control_weight_map,
                weight_required_for_payout,
                created_consensus_timestamp
            },
            MarketDynamic {
                open_contracts: ContractAmount::ZERO,
                payout: None
            }
        )
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn candlestick_stream() -> anyhow::Result<()> {
    let fed = fixtures().new_default_fed().await;
    let client1 = fed.new_client_rocksdb().await;

    let client1_dummy = client1.get_first_module::<DummyClientModule>();
    client1_dummy.print_money(Amount::from_sats(1000)).await?;

    let client1_pm = client1.get_first_module::<PredictionMarketsClientModule>();

    let event_json = Event::new_with_random_nonce(2, 1, Information::None).try_to_json_string()?;
    let contract_price = Amount::from_msats(100);
    let payout_control_weight_map: BTreeMap<NostrPublicKeyHex, Weight> =
        iter::once((Keys::generate().public_key.to_hex(), 1u16)).collect();
    let weight_required_for_payout = 1;
    let market = client1_pm
        .new_market(
            event_json.clone(),
            contract_price,
            payout_control_weight_map.clone(),
            weight_required_for_payout,
        )
        .await?;

    let mut stream = client1_pm
        .stream_candlesticks(market, 0, 15, UnixTimestamp::ZERO, Duration::ZERO)
        .await;
    spawn(async move {
        loop {
            info!("{:?}", stream.ok().await);
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
        sleep(Duration::from_millis(10)).await;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn order_stream() -> anyhow::Result<()> {
    let fed = fixtures().new_default_fed().await;
    let client1 = fed.new_client_rocksdb().await;

    let client1_dummy = client1.get_first_module::<DummyClientModule>();
    client1_dummy.print_money(Amount::from_sats(1000)).await?;

    let client1_pm = client1.get_first_module::<PredictionMarketsClientModule>();

    let event_json = Event::new_with_random_nonce(2, 1, Information::None).try_to_json_string()?;
    let contract_price = Amount::from_msats(100);
    let payout_control_weight_map: BTreeMap<NostrPublicKeyHex, Weight> =
        iter::once((Keys::generate().public_key.to_hex(), 1u16)).collect();
    let weight_required_for_payout = 1;
    let market = client1_pm
        .new_market(
            event_json.clone(),
            contract_price,
            payout_control_weight_map.clone(),
            weight_required_for_payout,
        )
        .await?;

    let watch_for_order_matches_stop_future = client1_pm
        .watch_for_order_matches(OrderPath::Market { market })
        .await?;

    client1_pm
        .new_order(
            market,
            0,
            Side::Buy,
            Amount::from_msats(60),
            ContractOfOutcomeAmount(1000),
        )
        .await?;

    // for _ in 0..10 {
    //     client1_pm
    //         .new_order(
    //             market,
    //             1,
    //             Side::Buy,
    //             Amount::from_msats(40),
    //             ContractOfOutcomeAmount(1),
    //         )
    //         .await?;
    // }

    let mut stream_of_order_streams = client1_pm
        .stream_orders_from_db(OrderFilter(OrderPath::All, OrderState::Any))
        .await;
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

    let iter = 0..10;
    iter.map(|_| async {
        let res = client1_pm
            .new_order(
                market,
                1,
                Side::Buy,
                Amount::from_msats(40),
                ContractOfOutcomeAmount(1),
            )
            .await;

        if let Err(e) = res {
            info!("error creating order: {e}");
        }
    })
    .collect::<FuturesUnordered<_>>()
    .collect::<()>()
    .await;

    watch_for_order_matches_stop_future.await?;

    info!("test_main end");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn general1() -> anyhow::Result<()> {
    let fed = fixtures().new_default_fed().await;
    let client1 = fed.new_client_rocksdb().await;
    let client2 = fed.new_client_rocksdb().await;

    client1
        .get_first_module::<DummyClientModule>()
        .print_money(Amount::from_sats(1000))
        .await?;
    client2
        .get_first_module::<DummyClientModule>()
        .print_money(Amount::from_sats(1000))
        .await?;

    let client1_pm = client1.get_first_module::<PredictionMarketsClientModule>();
    let client2_pm = client2.get_first_module::<PredictionMarketsClientModule>();

    let event_json = Event::new_with_random_nonce(2, 1, Information::None).try_to_json_string()?;
    let contract_price = Amount::from_msats(100);
    let payout_control_weight_map: BTreeMap<NostrPublicKeyHex, Weight> =
        iter::once((Keys::generate().public_key.to_hex(), 1u16)).collect();
    let weight_required_for_payout = 1;
    let market_outpoint = client1_pm
        .new_market(
            event_json.clone(),
            contract_price,
            payout_control_weight_map.clone(),
            weight_required_for_payout,
        )
        .await?;

    // orderbook setup

    let client1_order0 = client1_pm
        .new_order(
            market_outpoint,
            0,
            Side::Buy,
            Amount::from_msats(10),
            ContractOfOutcomeAmount(30),
        )
        .await?;

    let client1_order1 = client1_pm
        .new_order(
            market_outpoint,
            0,
            Side::Buy,
            Amount::from_msats(50),
            ContractOfOutcomeAmount(15),
        )
        .await?;

    let client1_order2 = client1_pm
        .new_order(
            market_outpoint,
            0,
            Side::Buy,
            Amount::from_msats(30),
            ContractOfOutcomeAmount(10),
        )
        .await?;

    let client2_order0 = client2_pm
        .new_order(
            market_outpoint,
            1,
            Side::Buy,
            Amount::from_msats(15),
            ContractOfOutcomeAmount(10),
        )
        .await?;

    let client2_order1 = client2_pm
        .new_order(
            market_outpoint,
            1,
            Side::Buy,
            Amount::from_msats(25),
            ContractOfOutcomeAmount(10),
        )
        .await?;

    let client2_order2 = client2_pm
        .new_order(
            market_outpoint,
            1,
            Side::Buy,
            Amount::from_msats(45),
            ContractOfOutcomeAmount(10),
        )
        .await?;

    // orderbook state
    //
    // outcome 0
    // 10 - 30
    // 30 - 10
    // 50 - 15
    //
    // outcome 1
    // 15 - 10
    // 25 - 10
    // 45 - 10
    dbg!(client1_pm.get_order_book(market_outpoint, 0).await?);
    dbg!(client1_pm.get_order_book(market_outpoint, 1).await?);

    let client1_order3 = client1_pm
        .new_order(
            market_outpoint,
            0,
            Side::Buy,
            Amount::from_msats(60),
            ContractOfOutcomeAmount(15),
        )
        .await?;

    assert_order_mutated_values(
        &client1_pm,
        client1_order3,
        true,
        AssertOrderMutatedValues {
            quantity_waiting_for_match: ContractOfOutcomeAmount(5),
            contract_of_outcome_balance: ContractOfOutcomeAmount(10),
            bitcoin_balance: Amount::from_msats(50),
            quantity_fulfilled: ContractOfOutcomeAmount(10),
            bitcoin_acquired_from_order_matches: SignedAmount {
                amount: Amount::from_msats(550),
                negative: true,
            },
            bitcoin_acquired_from_payout: Amount::ZERO,
        },
    )
    .await;
    assert_order_mutated_values(
        &client2_pm,
        client2_order2,
        true,
        AssertOrderMutatedValues {
            quantity_waiting_for_match: ContractOfOutcomeAmount(10),
            contract_of_outcome_balance: ContractOfOutcomeAmount(0),
            bitcoin_balance: Amount::from_msats(0),
            quantity_fulfilled: ContractOfOutcomeAmount(0),
            bitcoin_acquired_from_order_matches: SignedAmount::ZERO,
            bitcoin_acquired_from_payout: Amount::ZERO,
        },
    )
    .await;
    assert_order_mutated_values(
        &client2_pm,
        client2_order2,
        false,
        AssertOrderMutatedValues {
            quantity_waiting_for_match: ContractOfOutcomeAmount(0),
            contract_of_outcome_balance: ContractOfOutcomeAmount(10),
            bitcoin_balance: Amount::from_msats(0),
            quantity_fulfilled: ContractOfOutcomeAmount(10),
            bitcoin_acquired_from_order_matches: SignedAmount {
                amount: Amount::from_msats(450),
                negative: true,
            },
            bitcoin_acquired_from_payout: Amount::ZERO,
        },
    )
    .await;

    // orderbook state
    //
    // outcome 0
    // 10 - 30
    // 30 - 10
    // 50 - 15
    // 60 - 5
    //
    // outcome 1
    // 15 - 10
    // 25 - 10
    dbg!(client1_pm.get_order_book(market_outpoint, 0).await?);
    dbg!(client1_pm.get_order_book(market_outpoint, 1).await?);

    let client1_order4 = client1_pm
        .new_order(
            market_outpoint,
            0,
            Side::Buy,
            Amount::from_msats(80),
            ContractOfOutcomeAmount(5),
        )
        .await?;
    assert_order_mutated_values(
        &client1_pm,
        client1_order4,
        true,
        AssertOrderMutatedValues {
            quantity_waiting_for_match: ContractOfOutcomeAmount(0),
            contract_of_outcome_balance: ContractOfOutcomeAmount(5),
            bitcoin_balance: Amount::from_msats(25),
            quantity_fulfilled: ContractOfOutcomeAmount(5),
            bitcoin_acquired_from_order_matches: SignedAmount {
                amount: Amount::from_msats(375),
                negative: true,
            },
            bitcoin_acquired_from_payout: Amount::ZERO,
        },
    )
    .await;
    assert_order_mutated_values(
        &client2_pm,
        client2_order1,
        false,
        AssertOrderMutatedValues {
            quantity_waiting_for_match: ContractOfOutcomeAmount(5),
            contract_of_outcome_balance: ContractOfOutcomeAmount(5),
            bitcoin_balance: Amount::from_msats(0),
            quantity_fulfilled: ContractOfOutcomeAmount(5),
            bitcoin_acquired_from_order_matches: SignedAmount {
                amount: Amount::from_msats(125),
                negative: true,
            },
            bitcoin_acquired_from_payout: Amount::ZERO,
        },
    )
    .await;

    // orderbook state
    //
    // outcome 0
    // 10 - 30
    // 30 - 10
    // 50 - 15
    // 60 - 5
    //
    // outcome 1
    // 15 - 10
    // 25 - 5
    dbg!(client1_pm.get_order_book(market_outpoint, 0).await?);
    dbg!(client1_pm.get_order_book(market_outpoint, 1).await?);

    let client2_order3 = client2_pm
        .new_order(
            market_outpoint,
            1,
            Side::Buy,
            Amount::from_msats(80),
            ContractOfOutcomeAmount(35),
        )
        .await?;
    assert_order_mutated_values(
        &client2_pm,
        client2_order3,
        true,
        AssertOrderMutatedValues {
            quantity_waiting_for_match: ContractOfOutcomeAmount(5),
            contract_of_outcome_balance: ContractOfOutcomeAmount(30),
            bitcoin_balance: Amount::from_msats(40 * 5 + 30 * 15 + 10 * 10),
            quantity_fulfilled: ContractOfOutcomeAmount(30),
            bitcoin_acquired_from_order_matches: SignedAmount {
                amount: Amount::from_msats(40 * 5 + 50 * 15 + 70 * 10),
                negative: true,
            },
            bitcoin_acquired_from_payout: Amount::ZERO,
        },
    )
    .await;
    assert_order_mutated_values(
        &client1_pm,
        client1_order3,
        false,
        AssertOrderMutatedValues {
            quantity_waiting_for_match: ContractOfOutcomeAmount(0),
            contract_of_outcome_balance: ContractOfOutcomeAmount(15),
            bitcoin_balance: Amount::from_msats(50),
            quantity_fulfilled: ContractOfOutcomeAmount(15),
            bitcoin_acquired_from_order_matches: SignedAmount {
                amount: Amount::from_msats(550 + 300),
                negative: true,
            },
            bitcoin_acquired_from_payout: Amount::ZERO,
        },
    )
    .await;
    assert_order_mutated_values(
        &client1_pm,
        client1_order1,
        false,
        AssertOrderMutatedValues {
            quantity_waiting_for_match: ContractOfOutcomeAmount(0),
            contract_of_outcome_balance: ContractOfOutcomeAmount(15),
            bitcoin_balance: Amount::from_msats(0),
            quantity_fulfilled: ContractOfOutcomeAmount(15),
            bitcoin_acquired_from_order_matches: SignedAmount {
                amount: Amount::from_msats(750),
                negative: true,
            },
            bitcoin_acquired_from_payout: Amount::ZERO,
        },
    )
    .await;
    assert_order_mutated_values(
        &client1_pm,
        client1_order2,
        false,
        AssertOrderMutatedValues {
            quantity_waiting_for_match: ContractOfOutcomeAmount(0),
            contract_of_outcome_balance: ContractOfOutcomeAmount(10),
            bitcoin_balance: Amount::from_msats(0),
            quantity_fulfilled: ContractOfOutcomeAmount(10),
            bitcoin_acquired_from_order_matches: SignedAmount {
                amount: Amount::from_msats(300),
                negative: true,
            },
            bitcoin_acquired_from_payout: Amount::ZERO,
        },
    )
    .await;

    // orderbook state
    //
    // outcome 0
    // 10 - 30
    //
    // outcome 1
    // 15 - 10
    // 25 - 5
    // 80 - 5
    dbg!(client1_pm.get_order_book(market_outpoint, 0).await?);
    dbg!(client1_pm.get_order_book(market_outpoint, 1).await?);

    Ok(())
}

async fn assert_order_mutated_values(
    client_pm: &ClientModuleInstance<'_, PredictionMarketsClientModule>,
    order_id: OrderId,
    from_local_cache: bool,
    values: AssertOrderMutatedValues,
) {
    let Ok(Some(order)) = client_pm.get_order(order_id, from_local_cache).await else {
        panic!("could not retrieve order")
    };

    assert_eq!(
        order.quantity_waiting_for_match,
        values.quantity_waiting_for_match
    );
    assert_eq!(
        order.contract_of_outcome_balance,
        values.contract_of_outcome_balance
    );
    assert_eq!(order.bitcoin_balance, values.bitcoin_balance);
    assert_eq!(order.quantity_fulfilled, values.quantity_fulfilled);
    assert_eq!(
        order.bitcoin_acquired_from_order_matches,
        values.bitcoin_acquired_from_order_matches
    );
    assert_eq!(
        order.bitcoin_acquired_from_payout,
        values.bitcoin_acquired_from_payout
    );
}

struct AssertOrderMutatedValues {
    pub quantity_waiting_for_match: ContractOfOutcomeAmount,
    pub contract_of_outcome_balance: ContractOfOutcomeAmount,
    pub bitcoin_balance: Amount,
    pub quantity_fulfilled: ContractOfOutcomeAmount,
    pub bitcoin_acquired_from_order_matches: SignedAmount,
    pub bitcoin_acquired_from_payout: Amount,
}
