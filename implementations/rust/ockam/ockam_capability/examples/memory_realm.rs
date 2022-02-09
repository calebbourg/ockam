///! Connectivity begets connectivity: Memory Realm
use core::fmt;
use ockam::compat::collections::HashMap;
use ockam::{Address, Context, Routed, Worker};

// - types --------------------------------------------------------------------

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct UniqueUnforgeableActorAddress(u64);

impl fmt::Display for UniqueUnforgeableActorAddress {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        println!("Converting {:?} -> {}", self, self.0);
        fmt.write_str(&format!("{:#08X}", self.0))
    }
}

impl From<UniqueUnforgeableActorAddress> for Address {
    fn from(val: UniqueUnforgeableActorAddress) -> Self {
        Address::new(128, val.0.to_string())
    }
}

trait Uuaa {
    fn uuaa(&self) -> UniqueUnforgeableActorAddress;
}

type Capabilities = HashMap<&'static str, UniqueUnforgeableActorAddress>;

// - CapRequest ---------------------------------------------------------------

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
enum CapRequest {
    Request(String),
}

impl ockam::Message for CapRequest {}

// - ockam::node --------------------------------------------------------------

#[ockam::node]
async fn main(ctx: Context) -> ockam::Result<()> {
    // Connectivity by initial conditions: Alice is already connected to Carol at system initialization
    let carol = Carol::default();
    let alice = Alice {
        _capabilities: HashMap::from_iter([("cap_carol", carol.uuaa())]),
    };

    let alice_uuaa: Address = alice.uuaa().into();
    let carol_uuaa: Address = carol.uuaa().into();
    ctx.start_worker(alice_uuaa, alice).await?;
    ctx.start_worker(carol_uuaa, carol).await?;

    Ok(())
}

// - Alice --------------------------------------------------------------------

#[derive(Debug, Default)]
struct Alice {
    _capabilities: Capabilities,
}

impl Uuaa for Alice {
    fn uuaa(&self) -> UniqueUnforgeableActorAddress {
        let ptr: *const u64 = self as *const _ as *const u64;
        println!("Alice's uuaa is: {}", ptr as u64);
        UniqueUnforgeableActorAddress(ptr as u64)
    }
}

#[ockam::worker]
impl Worker for Alice {
    type Context = Context;
    type Message = CapRequest;

    async fn initialize(&mut self, _ctx: &mut Self::Context) -> ockam::Result<()> {
        // Start Carol

        Ok(())
    }

    async fn handle_message(
        &mut self,
        ctx: &mut Context,
        msg: Routed<CapRequest>,
    ) -> ockam::Result<()> {
        println!(
            "\n[Alice]\t[✓] Address: {}, Received: {:?}",
            ctx.address(),
            msg
        );

        let _return_route = msg.return_route();

        /*if let CapRequest::Create = &msg.body() {
            ctx.start_worker("carol", Carol::default()).await?;
            self.carol_ref = "carol".to_string();
            ctx.send(return_route, CapRequest::Reference(
                "carol".to_string(),
                "carol_ref".to_string()
            )).await?;
        }*/

        Ok(())
    }
}

// - Bob ----------------------------------------------------------------------

#[derive(Debug, Default)]
struct Bob {
    _capabilities: Capabilities,
}

impl Uuaa for Bob {
    fn uuaa(&self) -> UniqueUnforgeableActorAddress {
        let ptr: *const u64 = self as *const _ as *const u64;
        println!("Alice's uuaa is: {}", ptr as u64);
        UniqueUnforgeableActorAddress(ptr as u64)
    }
}

#[ockam::worker]
impl Worker for Bob {
    type Context = Context;
    type Message = CapRequest;

    async fn handle_message(
        &mut self,
        ctx: &mut Context,
        msg: Routed<CapRequest>,
    ) -> ockam::Result<()> {
        println!(
            "\n[Bob]\t[✓] Address: {}, Received: {:?}",
            ctx.address(),
            msg
        );
        Ok(())
    }
}

// - Carol --------------------------------------------------------------------

#[derive(Debug, Default)]
struct Carol {
    _capabilities: Capabilities,
}

impl Uuaa for Carol {
    fn uuaa(&self) -> UniqueUnforgeableActorAddress {
        let ptr: *const u64 = self as *const _ as *const u64;
        println!("Alice's uuaa is: {}", ptr as u64);
        UniqueUnforgeableActorAddress(ptr as u64)
    }
}

#[ockam::worker]
impl Worker for Carol {
    type Context = Context;
    type Message = CapRequest;

    async fn handle_message(
        &mut self,
        ctx: &mut Context,
        msg: Routed<CapRequest>,
    ) -> ockam::Result<()> {
        println!(
            "\n[Carol]\t[✓] Address: {}, Received: {:?}",
            ctx.address(),
            msg
        );
        Ok(())
    }
}
