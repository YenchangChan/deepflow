//! Minimal Lumberjack v2 server.
//!
//! Run it:
//!
//!     cargo run --example server -- 127.0.0.1:5044
//!
//! Then in another terminal run the client example pointing at the same address.

use std::env;
use std::time::Duration;

use lumberjack::{Result, Server};

#[tokio::main]
async fn main() -> Result<()> {
    let addr = env::args().nth(1).unwrap_or_else(|| "127.0.0.1:5044".to_string());

    let mut server = Server::builder()
        .keepalive(Duration::from_secs(15)) // send Ack(0) every 15s while a batch is held
        .channel_capacity(256)
        .bind(&addr)
        .await?;
    println!("listening on {}", server.local_addr());

    while let Some(batch) = server.recv().await {
        println!("got batch of {} events:", batch.len());
        for ev in batch.events() {
            println!("  {ev}");
        }
        // Acknowledge so the client's blocking `send()` returns. Dropping the
        // batch without calling `ack()` would auto-ack on Drop, but calling it
        // explicitly makes the intent obvious.
        batch.ack();
    }
    Ok(())
}
