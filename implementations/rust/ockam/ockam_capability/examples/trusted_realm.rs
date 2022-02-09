use ockam::{
    route, Context, Identity, RemoteForwarder, Result, Routed, TcpTransport, TrustEveryonePolicy,
    Vault, Worker, TCP,
};

use core::time::Duration;
use std::io;

// - ockam::node --------------------------------------------------------------

fn main() {
    match bootstrap() {
        Ok(()) => (),
        Err(e) => {
            println!("Fatal error: {}", e);
        }
    }
}

fn bootstrap() -> Result<()> {
    // alice
    let _join_handle: std::thread::JoinHandle<Result<(), ockam::Error>> =
        std::thread::spawn(|| {
            let (alice_ctx, mut alice_executor) = ockam::start_node();
            let _ = alice_executor.execute(async move { alice(alice_ctx).await })?;
            Ok(())
        });

    // wait for alice to start up
    std::thread::sleep(Duration::from_secs(2));

    // carol
    let _join_handle: std::thread::JoinHandle<Result<(), ockam::Error>> =
        std::thread::spawn(|| {
            let (carol_ctx, mut carol_executor) = ockam::start_node();
            let _ = carol_executor.execute(async move { carol(carol_ctx).await })?;
            Ok(())
        });

    // wait for carol to start up
    std::thread::sleep(Duration::from_secs(2));

    // bob
    let (bob_ctx, mut bob_executor) = ockam::start_node();
    let _ = bob_executor.execute(async move { bob(bob_ctx).await })?;

    Ok(())
}

// - Alice --------------------------------------------------------------------

struct Realm; // aka Scope Gateway aka Border Gateway Worker

// Define a Realm worker
#[ockam::worker]
impl Worker for Realm {
    type Context = Context;
    type Message = String;

    async fn handle_message(&mut self, ctx: &mut Context, msg: Routed<String>) -> Result<()> {
        println!(
            "\n[Alice]\t[✓] Address: {}, Received: {}",
            ctx.address(),
            msg
        );

        // TODO authorize Bob and return a capability
        ctx.send(msg.return_route(), msg.body()).await
    }
}

async fn alice(ctx: Context) -> Result<()> {
    // Start a worker, of type Realm, at address "realm".
    ctx.start_worker("realm", Realm).await?;

    // We won't call ctx.stop() here, this program will run until you
    // stop it with Ctrl-C
    Ok(())
}

// - Bob ----------------------------------------------------------------------

async fn bob(mut ctx: Context) -> Result<()> {
    // Initialize the TCP Transport.
    TcpTransport::create(&ctx).await?;

    // Create a Vault to safely store secret keys for Bob.
    let vault = Vault::create();

    // Create an Identity to represent Bob.
    let mut bob = Identity::create(&ctx, &vault).await?;

    // This program expects that Carol has setup a forwarding address,
    // for their secure channel listener, on the Ockam node at
    // 1.node.ockam.network:4000.
    //
    // From standard input, read this forwarding address for Carol's secure channel listener.
    println!("\n[Bob]\tEnter the forwarding address for Carol: ");
    let mut address = String::new();
    io::stdin()
        .read_line(&mut address)
        .expect("Error reading from stdin.");
    let forwarding_address = address.trim();

    // Combine the tcp address of the node and the forwarding_address to get a route
    // to Carol's secure channel listener.
    let route_to_carol_listener = route![(TCP, "1.node.ockam.network:4000"), forwarding_address];

    // As Bob, connect to Carol's secure channel listener, and perform an
    // Authenticated Key Exchange to establish an encrypted secure channel with Carol.
    let channel = bob
        .create_secure_channel(route_to_carol_listener, TrustEveryonePolicy)
        .await?;

    println!("\n[Bob]\t[✓] End-to-end encrypted secure channel was established.\n");

    loop {
        // Read a message from standard input.
        println!("[Bob]\tType a message for Carol's echoer:");
        let mut message = String::new();
        io::stdin()
            .read_line(&mut message)
            .expect("Error reading from stdin.");
        let message = message.trim();

        // Send the provided message, through the channel, to Carol's echoer.
        ctx.send(route![channel.clone(), "echoer"], message.to_string())
            .await?;

        // Wait to receive an echo and print it.
        let reply = ctx.receive::<String>().await?;
        println!("[Bob]\tBob received an echo: {}\n", reply); // should print "Hello Ockam!"
    }

    // This program will keep running until you stop it with Ctrl-C
}

// - Carol --------------------------------------------------------------------

struct Echoer;

// Define an Echoer worker that prints any message it receives and
// echoes it back on its return route.
#[ockam::worker]
impl Worker for Echoer {
    type Context = Context;
    type Message = String;

    async fn handle_message(&mut self, ctx: &mut Context, msg: Routed<String>) -> Result<()> {
        println!(
            "\n[Carol]\t[✓] Address: {}, Received: {}",
            ctx.address(),
            msg
        );

        // Echo the message body back on its return_route.
        ctx.send(msg.return_route(), msg.body()).await
    }
}

async fn carol(ctx: Context) -> Result<()> {
    // Initialize the TCP Transport.
    TcpTransport::create(&ctx).await?;

    // Create a Vault to safely store secret keys for Carol.
    let vault = Vault::create();

    // Create an Identity to represent Carol.
    let mut carol = Identity::create(&ctx, &vault).await?;

    // Create a secure channel listener for Carol that will wait for
    // requests to initiate an Authenticated Key Exchange.
    carol
        .create_secure_channel_listener("listener", TrustEveryonePolicy)
        .await?;

    // The computer that is running this program is likely within a
    // private network and not accessible over the internet.
    //
    // To allow Bob and others to initiate an end-to-end secure
    // channel with this program we connect with
    // 1.node.ockam.network:4000 as a TCP client and ask the
    // forwarding service on that node to create a forwarder for us.
    //
    // All messages that arrive at that forwarding address will be
    // sent to this program using the TCP connection we created as a
    // client.
    let node_in_hub = (TCP, "1.node.ockam.network:4000");
    let forwarder = RemoteForwarder::create(&ctx, node_in_hub, "listener").await?;
    println!(
        "\n[Carol]\t[✓] RemoteForwarder was created on the node at: 1.node.ockam.network:4000"
    );
    println!("[Carol]\tForwarding address for Carol is:");
    println!("[Carol]\t{}", forwarder.remote_address());

    // Start a worker, of type Echoer, at address "echoer".  This
    // worker will echo back every message it receives, along its
    // return route.
    ctx.start_worker("echoer", Echoer).await?;

    // We won't call ctx.stop() here, this program will run until you
    // stop it with Ctrl-C
    Ok(())
}
