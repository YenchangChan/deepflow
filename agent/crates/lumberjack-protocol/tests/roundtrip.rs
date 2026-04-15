use std::time::Duration;

use lumberjack::{Client, Server};
use serde_json::json;

#[tokio::test]
async fn end_to_end_uncompressed_round_trip() {
    let mut server = Server::builder()
        .no_keepalive()
        .bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = server.local_addr();

    let client_task = tokio::spawn(async move {
        let mut client = Client::builder()
            .compression_level(0)
            .ack_timeout(Duration::from_secs(5))
            .connect(addr)
            .await
            .unwrap();
        for i in 0..10u32 {
            let n = client.send(&[json!({"i": i})]).await.unwrap();
            assert_eq!(n, 1);
        }
    });

    let mut total = 0;
    while total < 10 {
        let batch = server.recv().await.unwrap();
        total += batch.len();
        batch.ack();
    }
    client_task.await.unwrap();
    assert_eq!(total, 10);
}

#[tokio::test]
async fn server_handles_multiple_concurrent_clients() {
    let mut server = Server::bind("127.0.0.1:0").await.unwrap();
    let addr = server.local_addr();

    let mut clients = Vec::new();
    for cid in 0..5u32 {
        clients.push(tokio::spawn(async move {
            let mut client = Client::builder()
                .compression_level(0)
                .connect(addr)
                .await
                .unwrap();
            for i in 0..3u32 {
                client.send(&[json!({"cid": cid, "i": i})]).await.unwrap();
            }
        }));
    }

    let mut total = 0;
    while total < 15 {
        let batch = server.recv().await.unwrap();
        total += batch.len();
        batch.ack();
    }
    for c in clients {
        c.await.unwrap();
    }
}
