use fedimint_core::api::{FederationApiExt, FederationResult, IModuleFederationApi};

use fedimint_core::module::ApiRequestErased;
use fedimint_core::task::{MaybeSend, MaybeSync};
use fedimint_core::{apply, async_trait_maybe_send, OutPoint};
use fedimint_dummy_common::{Market, Order};
use secp256k1::XOnlyPublicKey;

#[apply(async_trait_maybe_send!)]
pub trait OddsMarketsFederationApi {
    async fn get_market(&self, out_point: OutPoint) -> FederationResult<Market>;
    async fn get_order(&self, order: XOnlyPublicKey) -> FederationResult<Order>;
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

    async fn get_order(&self, order: XOnlyPublicKey) -> FederationResult<Order> {
        self.request_current_consensus("get_order".to_string(), ApiRequestErased::new(order))
            .await
    }
}
