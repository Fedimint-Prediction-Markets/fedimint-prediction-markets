use fedimint_core::api::{FederationApiExt, FederationResult, IModuleFederationApi};

use fedimint_core::module::ApiRequestErased;
use fedimint_core::task::{MaybeSend, MaybeSync};
use fedimint_core::{apply, async_trait_maybe_send, OutPoint};
use fedimint_dummy_common::{Market, Order};
use secp256k1::XOnlyPublicKey;

#[apply(async_trait_maybe_send!)]
pub trait OddsMarketsFederationApi {
    async fn get_market(&self, out_point: OutPoint) -> FederationResult<Market>;
    async fn get_order(&self, order: XOnlyPublicKey) -> FederationResult<Option<Order>>;
    async fn get_outcome_control_markets(
        &self,
        outcome_control: XOnlyPublicKey,
    ) -> FederationResult<Vec<OutPoint>>;
}

#[apply(async_trait_maybe_send!)]
impl<T: ?Sized> OddsMarketsFederationApi for T
where
    T: IModuleFederationApi + MaybeSend + MaybeSync + 'static,
{
    async fn get_market(&self, out_point: OutPoint) -> FederationResult<Market> {
        self.request_current_consensus("get_market".to_string(), ApiRequestErased::new(out_point))
            .await
    }

    async fn get_order(&self, order: XOnlyPublicKey) -> FederationResult<Option<Order>> {
        self.request_current_consensus("get_order".to_string(), ApiRequestErased::new(order))
            .await
    }

    async fn get_outcome_control_markets(
        &self,
        outcome_control: XOnlyPublicKey,
    ) -> FederationResult<Vec<OutPoint>> {
        self.request_current_consensus(
            "get_outcome_control_markets".to_string(),
            ApiRequestErased::new(outcome_control),
        )
        .await
    }
}
