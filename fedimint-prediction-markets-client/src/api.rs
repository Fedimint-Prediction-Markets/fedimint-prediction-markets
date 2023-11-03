use std::collections::BTreeMap;

use fedimint_core::api::{FederationApiExt, FederationResult, IModuleFederationApi};

use fedimint_core::module::ApiRequestErased;
use fedimint_core::task::{MaybeSend, MaybeSync};
use fedimint_core::{apply, async_trait_maybe_send, Amount, OutPoint};
use fedimint_prediction_markets_common::{
    GetOutcomeControlMarketsParams, GetOutcomeControlMarketsResult, Market, Order, UnixTimestamp,
};
use secp256k1::XOnlyPublicKey;

#[apply(async_trait_maybe_send!)]
pub trait PredictionMarketsFederationApi {
    async fn get_market(&self, market: OutPoint) -> FederationResult<Option<Market>>;
    async fn get_order(&self, order: XOnlyPublicKey) -> FederationResult<Option<Order>>;
    async fn get_outcome_control_markets(
        &self,
        outcome_control: XOnlyPublicKey,
        markets_created_after_and_including: UnixTimestamp,
    ) -> FederationResult<GetOutcomeControlMarketsResult>;
    async fn get_market_outcome_control_proposals(
        &self,
        market: OutPoint,
    ) -> FederationResult<BTreeMap<XOnlyPublicKey, Vec<Amount>>>;
}

#[apply(async_trait_maybe_send!)]
impl<T: ?Sized> PredictionMarketsFederationApi for T
where
    T: IModuleFederationApi + MaybeSend + MaybeSync + 'static,
{
    async fn get_market(&self, market: OutPoint) -> FederationResult<Option<Market>> {
        self.request_current_consensus("get_market".to_string(), ApiRequestErased::new(market))
            .await
    }

    async fn get_order(&self, order: XOnlyPublicKey) -> FederationResult<Option<Order>> {
        self.request_current_consensus("get_order".to_string(), ApiRequestErased::new(order))
            .await
    }

    async fn get_outcome_control_markets(
        &self,
        outcome_control: XOnlyPublicKey,
        markets_created_after_and_including: UnixTimestamp,
    ) -> FederationResult<GetOutcomeControlMarketsResult> {
        self.request_current_consensus(
            "get_outcome_control_markets".to_string(),
            ApiRequestErased::new(GetOutcomeControlMarketsParams {
                outcome_control,
                markets_created_after_and_including,
            }),
        )
        .await
    }

    async fn get_market_outcome_control_proposals(
        &self,
        market: OutPoint,
    ) -> FederationResult<BTreeMap<XOnlyPublicKey, Vec<Amount>>> {
        self.request_current_consensus(
            "get_market_outcome_control_proposals".to_string(),
            ApiRequestErased::new(market),
        )
        .await
    }
}
