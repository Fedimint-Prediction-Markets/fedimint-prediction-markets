# Fedimint Prediction Markets

### New market
```bash
bash-5.2$ # `new-market` command expects 2 arguments: <contract_price_msats> <outcomes>
bash-5.2$ fedimint-cli module --module prediction-markets new-market 1000 2
2023-11-13T03:15:38.278681Z  INFO fedimint_client::sm::executor: Starting state machine executor task
2023-11-13T03:15:39.497268Z  INFO fedimint_client: Last client reference dropped, shutting down client task group
2023-11-13T03:15:39.498230Z  INFO fedimint_client::sm::executor: Shutting down state machine executor runner due to shutdown signal
"aa69d3d6616514fce454ad16e1cd11b4ff98a8bb330bd1158ce1b87420fecd4c"
```

### Get market
```bash
bash-5.2$ # `get-market` command expects 1 argument: <market_txid>
bash-5.2$ fedimint-cli module --module prediction-markets get-market aa69d3d6616514fce454ad16e1cd11b4ff98a8bb330bd1158ce1b87420fecd4c
2023-11-13T03:15:55.965053Z  INFO fedimint_client::sm::executor: Starting state machine executor task
2023-11-13T03:15:55.970458Z  INFO fedimint_client: Last client reference dropped, shutting down client task group
2023-11-13T03:15:55.970622Z  INFO fedimint_client::sm::executor: Shutting down state machine executor runner due to shutdown signal
{
  "contract_price": 1000,
  "outcomes": 2,
  "payout_controls_weights": {
    "f8f6908fab3ec57966ac036770e68a4e917ccfbd67f71cb99cdfa4a459d1f6d7": 1
  },
  "weight_required_for_payout": 1,
  "information": {
    "title": "my market",
    "description": "this is my market",
    "outcome_titles": [
      "Outcome 0",
      "Outcome 1"
    ],
    "expected_payout_time": {
      "seconds": 0
    }
  },
  "created_consensus_timestamp": {
    "seconds": 1699845330
  },
  "open_contracts": 0,
  "payout": null
}
```

### New Order
```bash
bash-5.2$ # `new-order` command expects 5 arguments: <market_txid> <outcome> <side> <price_msats> <quantity>
bash-5.2$ fedimint-cli module --module prediction-markets new-order aa69d3d6616514fce454ad16e1cd11b4ff98a8bb330bd1158ce1b87420fecd4c 0 buy 350 1
2023-11-13T03:31:16.950132Z  INFO fedimint_client::sm::executor: Starting state machine executor task
2023-11-13T03:31:18.163474Z  INFO fedimint_client: Last client reference dropped, shutting down client task group
2023-11-13T03:31:18.164123Z  INFO fedimint_client::sm::executor: Shutting down state machine executor runner due to shutdown signal
0
bash-5.2$ fedimint-cli module --module prediction-markets new-order aa69d3d6616514fce454ad16e1cd11b4ff98a8bb330bd1158ce1b87420fecd4c 1 buy 700 1
2023-11-13T03:31:44.942557Z  INFO fedimint_client::sm::executor: Starting state machine executor task
2023-11-13T03:31:46.166304Z  INFO fedimint_client: Last client reference dropped, shutting down client task group
2023-11-13T03:31:46.167044Z  INFO fedimint_client::sm::executor: Shutting down state machine executor runner due to shutdown signal
1
bash-5.2$ # Return value is ID of new order
```

### Sync Orders
```bash
bash-5.2$ # `sync-order` command only accepts 2 optional arguments: (market_txid) (outcome)
bash-5.2$ fedimint-cli module --module prediction-markets sync-orders
2023-11-13T03:34:48.988459Z  INFO fedimint_client::sm::executor: Starting state machine executor task
2023-11-13T03:34:48.993748Z  INFO fedimint_client: Last client reference dropped, shutting down client task group
2023-11-13T03:34:48.993887Z  INFO fedimint_client::sm::executor: Shutting down state machine executor runner due to shutdown signal
2
bash-5.2$ # Return value is number of orders synced with federation
```

### List Orders
```bash
bash-5.2$ # `list-orders` command has 2 optional arguments: (market_txid) (outcome)
bash-5.2$ fedimint-cli module --module prediction-markets list-orders 
2023-11-13T03:42:27.717917Z  INFO fedimint_client::sm::executor: Starting state machine executor task
2023-11-13T03:42:27.720192Z  INFO fedimint_client: Last client reference dropped, shutting down client task group
2023-11-13T03:42:27.720344Z  INFO fedimint_client::sm::executor: Shutting down state machine executor runner due to shutdown signal
{
  "0": {
    "market": {
      "txid": "aa69d3d6616514fce454ad16e1cd11b4ff98a8bb330bd1158ce1b87420fecd4c",
      "out_idx": 0
    },
    "outcome": 0,
    "side": "Buy",
    "price": 350,
    "original_quantity": 1,
    "time_ordering": 0,
    "created_consensus_timestamp": {
      "seconds": 1699846275
    },
    "quantity_waiting_for_match": 0,
    "contract_of_outcome_balance": 1,
    "bitcoin_balance": 0,
    "bitcoin_cost": {
      "amount": 350,
      "negative": false
    }
  },
  "1": {
    "market": {
      "txid": "aa69d3d6616514fce454ad16e1cd11b4ff98a8bb330bd1158ce1b87420fecd4c",
      "out_idx": 0
    },
    "outcome": 1,
    "side": "Buy",
    "price": 700,
    "original_quantity": 1,
    "time_ordering": 1,
    "created_consensus_timestamp": {
      "seconds": 1699846305
    },
    "quantity_waiting_for_match": 0,
    "contract_of_outcome_balance": 1,
    "bitcoin_balance": 50,
    "bitcoin_cost": {
      "amount": 650,
      "negative": false
    }
  }
}
```