//! Minimal Lumberjack v2 client.
//!
//! Run the server example first, then:
//!
//!     cargo run --example client -- 127.0.0.1:5044
//!
//! Sends three small JSON events as one batch and exits when the server acks.

use std::env;
use std::time::Duration;

use lumberjack::{Client, Result};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = env::args().nth(1).unwrap_or_else(|| "127.0.0.1:5044".to_string());

    let mut client = Client::builder()
        .compression_level(3) // 0 disables compression; 1..=9 enables zlib
        .ack_timeout(Duration::from_secs(30))
        // .local_port_range(60000, 65000) // pin source port window if needed
        .connect(&addr)
        .await?;

    let events = vec![
        json!({"timestamp": "2026-04-09T10:00:00Z", "level": "info", "msg": "hello"}),
        json!({"timestamp": "2026-04-09T10:00:01Z", "level": "warn", "msg": "uh oh"}),
        json!({"timestamp": "2026-04-09T10:00:02Z", "level": "error", "msg": "boom"}),
    ];

    let acked = client.send(&events).await?;
    println!("acked {acked} events");

    client.close().await?;
    Ok(())
}
