use fedimint_core::api::{FederationApiExt, FederationResult, IModuleFederationApi};
use fedimint_core::module::ApiRequestErased;
use fedimint_core::task::{MaybeSend, MaybeSync};
use fedimint_core::{apply, async_trait_maybe_send};
use fedimint_prediction_markets_common::api::{
    GetEventPayoutAttestationsUsedToPermitPayoutParams,
    GetEventPayoutAttestationsUsedToPermitPayoutResult, GetMarketDynamicParams,
    GetMarketDynamicResult, GetMarketOutcomeCandlesticksParams, GetMarketOutcomeCandlesticksResult,
    GetMarketParams, GetMarketResult, GetOrderParams, GetOrderResult,
    WaitMarketOutcomeCandlesticksParams, WaitMarketOutcomeCandlesticksResult, WaitOrderMatchParams,
    WaitOrderMatchResult, GET_EVENT_PAYOUT_ATTESTATIONS_USED_TO_PERMIT_PAYOUT_ENDPOINT,
    GET_MARKET_DYNAMIC_ENDPOINT, GET_MARKET_ENDPOINT, GET_MARKET_OUTCOME_CANDLESTICKS_ENDPOINT,
    GET_ORDER_ENDPOINT, WAIT_MARKET_OUTCOME_CANDLESTICKS_ENDPOINT, WAIT_ORDER_MATCH_ENDPOINT,
};

#[apply(async_trait_maybe_send!)]
pub trait PredictionMarketsFederationApi {
    async fn get_market(&self, params: GetMarketParams) -> FederationResult<GetMarketResult>;
    async fn get_market_dynamic(
        &self,
        params: GetMarketDynamicParams,
    ) -> FederationResult<GetMarketDynamicResult>;
    async fn get_event_payout_attestations_used_to_permit_payout(
        &self,
        params: GetEventPayoutAttestationsUsedToPermitPayoutParams,
    ) -> FederationResult<GetEventPayoutAttestationsUsedToPermitPayoutResult>;
    async fn get_order(&self, params: GetOrderParams) -> FederationResult<GetOrderResult>;
    async fn wait_order_match(
        &self,
        params: WaitOrderMatchParams,
    ) -> FederationResult<WaitOrderMatchResult>;
    async fn get_market_outcome_candlesticks(
        &self,
        params: GetMarketOutcomeCandlesticksParams,
    ) -> FederationResult<GetMarketOutcomeCandlesticksResult>;
    async fn wait_market_outcome_candlesticks(
        &self,
        params: WaitMarketOutcomeCandlesticksParams,
    ) -> FederationResult<WaitMarketOutcomeCandlesticksResult>;
}

#[apply(async_trait_maybe_send!)]
impl<T: ?Sized> PredictionMarketsFederationApi for T
where
    T: IModuleFederationApi + MaybeSend + MaybeSync + 'static,
{
    async fn get_market(&self, params: GetMarketParams) -> FederationResult<GetMarketResult> {
        self.request_current_consensus(GET_MARKET_ENDPOINT.into(), ApiRequestErased::new(params))
            .await
    }

    async fn get_market_dynamic(
        &self,
        params: GetMarketDynamicParams,
    ) -> FederationResult<GetMarketDynamicResult> {
        self.request_current_consensus(
            GET_MARKET_DYNAMIC_ENDPOINT.into(),
            ApiRequestErased::new(params),
        )
        .await
    }

    async fn get_event_payout_attestations_used_to_permit_payout(
        &self,
        params: GetEventPayoutAttestationsUsedToPermitPayoutParams,
    ) -> FederationResult<GetEventPayoutAttestationsUsedToPermitPayoutResult> {
        self.request_current_consensus(
            GET_EVENT_PAYOUT_ATTESTATIONS_USED_TO_PERMIT_PAYOUT_ENDPOINT.into(),
            ApiRequestErased::new(params),
        )
        .await
    }

    async fn get_order(&self, params: GetOrderParams) -> FederationResult<GetOrderResult> {
        self.request_current_consensus(GET_ORDER_ENDPOINT.into(), ApiRequestErased::new(params))
            .await
    }

    async fn wait_order_match(
        &self,
        params: WaitOrderMatchParams,
    ) -> FederationResult<WaitOrderMatchResult> {
        self.request_current_consensus(
            WAIT_ORDER_MATCH_ENDPOINT.into(),
            ApiRequestErased::new(params),
        )
        .await
    }

    async fn get_market_outcome_candlesticks(
        &self,
        params: GetMarketOutcomeCandlesticksParams,
    ) -> FederationResult<GetMarketOutcomeCandlesticksResult> {
        self.request_current_consensus(
            GET_MARKET_OUTCOME_CANDLESTICKS_ENDPOINT.into(),
            ApiRequestErased::new(params),
        )
        .await
    }

    async fn wait_market_outcome_candlesticks(
        &self,
        params: WaitMarketOutcomeCandlesticksParams,
    ) -> FederationResult<WaitMarketOutcomeCandlesticksResult> {
        self.request_current_consensus(
            WAIT_MARKET_OUTCOME_CANDLESTICKS_ENDPOINT.into(),
            ApiRequestErased::new(params),
        )
        .await
    }
}
