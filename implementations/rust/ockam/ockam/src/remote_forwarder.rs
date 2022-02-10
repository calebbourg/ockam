#![deny(missing_docs)]

use crate::{route, Context, Message, OckamError};
use ockam_core::compat::rand::random;
use ockam_core::compat::{
    boxed::Box,
    string::{String, ToString},
    vec::Vec,
};
use ockam_core::{Address, Any, LocalMessage, Result, Route, Routed, TransportMessage, Worker};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

/// Information about a remotely forwarded worker.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Message)]
pub struct RemoteForwarderInfo {
    forwarding_route: Route,
    remote_address: String,
    worker_address: Address,
}

impl RemoteForwarderInfo {
    /// Returns the forwarding route.
    pub fn forwarding_route(&self) -> &Route {
        &self.forwarding_route
    }
    /// Returns the remote address.
    pub fn remote_address(&self) -> &str {
        &self.remote_address
    }
    /// Returns the worker address.
    pub fn worker_address(&self) -> &Address {
        &self.worker_address
    }
}

enum RemoteForwarderState {
    PubSub { name: String, topic: String },
    Forwarder,
}

/// This Worker is responsible for registering on Ockam Hub and forwarding messages to local Worker
pub struct RemoteForwarder {
    state: RemoteForwarderState,
    hub_addr: Address,
    destination: Route,
    callback_address: Address,
}

impl RemoteForwarder {
    fn new(
        state: RemoteForwarderState,
        hub_addr: Address,
        destination: impl Into<Address>,
        callback_address: Address,
    ) -> Self {
        Self {
            state,
            hub_addr,
            destination: route![destination],
            callback_address,
        }
    }

    /// Create and start static RemoteForwarder at predefined address with given Ockam Hub address
    /// and Address of destination Worker that should receive forwarded messages
    pub async fn create_static(
        ctx: &Context,
        hub_addr: impl Into<Address>,
        destination: impl Into<Address>,
        name: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<RemoteForwarderInfo> {
        let address: Address = random();
        let mut child_ctx = ctx.new_context(address).await?;
        let state = RemoteForwarderState::PubSub {
            name: name.into(),
            topic: topic.into(),
        };
        let forwarder = Self::new(state, hub_addr.into(), destination, child_ctx.address());

        let worker_address: Address = random();
        debug!("Starting static RemoteForwarder at {}", &worker_address);
        ctx.start_worker(worker_address, forwarder).await?;

        let resp = child_ctx
            .receive::<RemoteForwarderInfo>()
            .await?
            .take()
            .body();

        Ok(resp)
    }

    /// Create and start new ephemeral RemoteForwarder at random address with given Ockam Hub address
    /// and Address of destination Worker that should receive forwarded messages
    pub async fn create(
        ctx: &Context,
        hub_addr: impl Into<Address>,
        destination: impl Into<Address>,
    ) -> Result<RemoteForwarderInfo> {
        let address: Address = random();
        let mut child_ctx = ctx.new_context(address).await?;
        let forwarder = Self::new(
            RemoteForwarderState::Forwarder,
            hub_addr.into(),
            destination,
            child_ctx.address(),
        );

        let worker_address: Address = random();
        debug!("Starting ephemeral RemoteForwarder at {}", &worker_address);
        ctx.start_worker(worker_address, forwarder).await?;

        let resp = child_ctx
            .receive::<RemoteForwarderInfo>()
            .await?
            .take()
            .body();

        Ok(resp)
    }
}

#[crate::worker]
impl Worker for RemoteForwarder {
    type Context = Context;
    type Message = Any;

    async fn initialize(&mut self, ctx: &mut Self::Context) -> Result<()> {
        debug!("RemoteForwarder registration...");

        let (route, payload) = match &self.state {
            RemoteForwarderState::Forwarder => (
                route![self.hub_addr.clone(), "forwarding_service"],
                "register".to_string(),
            ),
            RemoteForwarderState::PubSub { name, topic } => (
                route![self.hub_addr.clone(), "pub_sub_service"],
                format!("{}:{}", name, topic),
                // TODO: Start periodic pings
            ),
        };

        ctx.send(route, payload.clone()).await?;

        let resp = ctx.receive::<String>().await?.take();
        let route = resp.return_route();
        let resp = resp.body();
        if resp != payload {
            return Err(OckamError::InvalidHubResponse.into());
        }

        info!("RemoteForwarder registered with route: {}", route);
        let address;
        if let Some(a) = route.clone().recipient().to_string().strip_prefix("0#") {
            address = a.to_string();
        } else {
            return Err(OckamError::InvalidHubResponse.into());
        }

        ctx.send(
            self.callback_address.clone(),
            RemoteForwarderInfo {
                forwarding_route: route,
                remote_address: address,
                worker_address: ctx.address(),
            },
        )
        .await?;

        Ok(())
    }

    async fn handle_message(
        &mut self,
        ctx: &mut Context,
        msg: Routed<Self::Message>,
    ) -> Result<()> {
        let return_route = msg.return_route();
        let payload = msg.into_transport_message().payload;
        debug!("RemoteForwarder received message");

        let msg = TransportMessage::v1(self.destination.clone(), return_route, payload);

        ctx.forward(LocalMessage::new(msg, Vec::new())).await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::workers::Echoer;
    use ockam_transport_tcp::{TcpTransport, TCP};
    use std::env;

    fn get_cloud_address() -> Option<String> {
        if let Ok(v) = env::var("CLOUD_ADDRESS") {
            if !v.is_empty() {
                return Some(v);
            }
        }

        warn!("No CLOUD_ADDRESS specified, skipping the test");

        None
    }

    #[allow(non_snake_case)]
    #[ockam_macros::test]
    async fn forwarding__ephemeral_address__should_respond(ctx: &mut Context) -> Result<()> {
        let cloud_address;
        if let Some(c) = get_cloud_address() {
            cloud_address = c;
        } else {
            ctx.stop().await?;
            return Ok(());
        }

        ctx.start_worker("echoer", Echoer).await?;

        TcpTransport::create(&ctx).await?;

        let node_in_hub = (TCP, cloud_address);
        let remote_info = RemoteForwarder::create(ctx, node_in_hub.clone(), "echoer").await?;

        let mut child_ctx = ctx.new_context(Address::random(0)).await?;

        child_ctx
            .send(
                route![node_in_hub, remote_info.remote_address()],
                "Hello".to_string(),
            )
            .await?;

        let resp = child_ctx.receive::<String>().await?.take().body();

        assert_eq!(resp, "Hello");

        ctx.stop().await
    }

    #[allow(non_snake_case)]
    #[ockam_macros::test]
    async fn forwarding__static_address__should_respond(ctx: &mut Context) -> Result<()> {
        let cloud_address;
        if let Some(c) = get_cloud_address() {
            cloud_address = c;
        } else {
            ctx.stop().await?;
            return Ok(());
        }

        ctx.start_worker("echoer", Echoer).await?;

        TcpTransport::create(&ctx).await?;

        let node_in_hub = (TCP, cloud_address);
        let remote_info =
            RemoteForwarder::create_static(ctx, node_in_hub.clone(), "echoer", "name", "topic")
                .await?;

        let mut child_ctx = ctx.new_context(Address::random(0)).await?;

        child_ctx
            .send(
                route![node_in_hub, remote_info.remote_address()],
                "Hello".to_string(),
            )
            .await?;

        let resp = child_ctx.receive::<String>().await?.take().body();

        assert_eq!(resp, "Hello");

        ctx.stop().await
    }
}
