use fedimint_core::api::{FederationApiExt, FederationResult, IModuleFederationApi};
use fedimint_core::module::ApiRequestErased;
use fedimint_core::task::{MaybeSend, MaybeSync};
use fedimint_core::{apply, async_trait_maybe_send};
use fedimint_prediction_markets_common::api::{
    GetMarketOutcomeCandlesticksParams, GetMarketOutcomeCandlesticksResult, GetMarketParams,
    GetMarketPayoutControlProposalsParams, GetMarketPayoutControlProposalsResult, GetMarketResult,
    GetOrderParams, GetOrderResult, GetPayoutControlBalanceParams, GetPayoutControlBalanceResult,
    GetPayoutControlMarketsParams, GetPayoutControlMarketsResult,
    WaitMarketOutcomeCandlesticksParams, WaitMarketOutcomeCandlesticksResult, GET_MARKET,
    GET_MARKET_OUTCOME_CANDLESTICKS, GET_MARKET_PAYOUT_CONTROL_PROPOSALS, GET_ORDER,
    GET_PAYOUT_CONTROL_BALANCE, GET_PAYOUT_CONTROL_MARKETS, WAIT_MARKET_OUTCOME_CANDLESTICKS,
};

#[apply(async_trait_maybe_send!)]
pub trait PredictionMarketsFederationApi {
    async fn get_market(&self, params: GetMarketParams) -> FederationResult<GetMarketResult>;
    async fn get_order(&self, params: GetOrderParams) -> FederationResult<GetOrderResult>;
    async fn get_payout_control_markets(
        &self,
        params: GetPayoutControlMarketsParams,
    ) -> FederationResult<GetPayoutControlMarketsResult>;
    async fn get_market_payout_control_proposals(
        &self,
        params: GetMarketPayoutControlProposalsParams,
    ) -> FederationResult<GetMarketPayoutControlProposalsResult>;
    async fn get_market_outcome_candlesticks(
        &self,
        params: GetMarketOutcomeCandlesticksParams,
    ) -> FederationResult<GetMarketOutcomeCandlesticksResult>;
    async fn wait_market_outcome_candlesticks(
        &self,
        params: WaitMarketOutcomeCandlesticksParams,
    ) -> FederationResult<WaitMarketOutcomeCandlesticksResult>;
    async fn get_payout_control_balance(
        &self,
        params: GetPayoutControlBalanceParams,
    ) -> FederationResult<GetPayoutControlBalanceResult>;
}

#[apply(async_trait_maybe_send!)]
impl<T: ?Sized> PredictionMarketsFederationApi for T
where
    T: IModuleFederationApi + MaybeSend + MaybeSync + 'static,
{
    async fn get_market(&self, params: GetMarketParams) -> FederationResult<GetMarketResult> {
        self.request_current_consensus(GET_MARKET.into(), ApiRequestErased::new(params))
            .await
    }

    async fn get_order(&self, params: GetOrderParams) -> FederationResult<GetOrderResult> {
        self.request_current_consensus(GET_ORDER.into(), ApiRequestErased::new(params))
            .await
    }

    async fn get_payout_control_markets(
        &self,
        params: GetPayoutControlMarketsParams,
    ) -> FederationResult<GetPayoutControlMarketsResult> {
        self.request_current_consensus(
            GET_PAYOUT_CONTROL_MARKETS.into(),
            ApiRequestErased::new(params),
        )
        .await
    }

    async fn get_market_payout_control_proposals(
        &self,
        params: GetMarketPayoutControlProposalsParams,
    ) -> FederationResult<GetMarketPayoutControlProposalsResult> {
        self.request_current_consensus(
            GET_MARKET_PAYOUT_CONTROL_PROPOSALS.into(),
            ApiRequestErased::new(params),
        )
        .await
    }

    async fn get_market_outcome_candlesticks(
        &self,
        params: GetMarketOutcomeCandlesticksParams,
    ) -> FederationResult<GetMarketOutcomeCandlesticksResult> {
        self.request_current_consensus(
            GET_MARKET_OUTCOME_CANDLESTICKS.into(),
            ApiRequestErased::new(params),
        )
        .await
    }

    async fn wait_market_outcome_candlesticks(
        &self,
        params: WaitMarketOutcomeCandlesticksParams,
    ) -> FederationResult<WaitMarketOutcomeCandlesticksResult> {
        self.request_current_consensus(
            WAIT_MARKET_OUTCOME_CANDLESTICKS.into(),
            ApiRequestErased::new(params),
        )
        .await
    }

    async fn get_payout_control_balance(
        &self,
        params: GetPayoutControlBalanceParams,
    ) -> FederationResult<GetPayoutControlBalanceResult> {
        self.request_current_consensus(
            GET_PAYOUT_CONTROL_BALANCE.into(),
            ApiRequestErased::new(params),
        )
        .await
    }
}
