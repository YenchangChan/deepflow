#![cfg(feature = "compression")]

use std::time::Duration;

use lumberjack::{Client, Server};
use serde_json::json;

#[tokio::test]
async fn compressed_batch_round_trip() {
    let mut server = Server::bind("127.0.0.1:0").await.unwrap();
    let addr = server.local_addr();

    let client_task = tokio::spawn(async move {
        let mut client = Client::builder()
            .compression_level(6)
            .ack_timeout(Duration::from_secs(5))
            .connect(addr)
            .await
            .unwrap();
        let big: Vec<_> = (0..50)
            .map(|i| json!({"i": i, "filler": "xxxxxxxxxxxxxxxx"}))
            .collect();
        let n = client.send(&big).await.unwrap();
        assert_eq!(n, 50);
    });

    let batch = server.recv().await.unwrap();
    assert_eq!(batch.len(), 50);
    assert_eq!(batch.events()[0]["i"], 0);
    assert_eq!(batch.events()[49]["i"], 49);
    batch.ack();
    client_task.await.unwrap();
}
