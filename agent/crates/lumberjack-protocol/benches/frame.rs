use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use lumberjack::Frame;

fn make_json_payload(size: usize) -> Bytes {
    // A small JSON object padded to roughly `size` bytes.
    let filler = "x".repeat(size.saturating_sub(20));
    Bytes::from(format!("{{\"k\":\"{filler}\"}}"))
}

fn build_inner_payload(events: usize, event_size: usize) -> Bytes {
    let mut buf = BytesMut::new();
    let payload = make_json_payload(event_size);
    for i in 0..events {
        Frame::Json {
            seq: (i as u32) + 1,
            data: payload.clone(),
        }
        .encode(&mut buf);
    }
    buf.freeze()
}

fn bench_encode_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_encode_json");
    for &size in &[64usize, 256, 1024, 4096] {
        let payload = make_json_payload(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &payload, |b, p| {
            b.iter(|| {
                let mut buf = BytesMut::with_capacity(64 + p.len());
                Frame::Json {
                    seq: 1,
                    data: p.clone(),
                }
                .encode(&mut buf);
                buf
            });
        });
    }
    group.finish();
}

fn bench_decode_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_decode_json");
    for &size in &[64usize, 256, 1024, 4096] {
        let payload = make_json_payload(size);
        let mut encoded = BytesMut::new();
        Frame::Json {
            seq: 1,
            data: payload,
        }
        .encode(&mut encoded);
        let encoded = encoded.freeze();

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &encoded, |b, e| {
            b.iter(|| {
                let mut buf = BytesMut::from(&e[..]);
                Frame::decode(&mut buf).unwrap().unwrap()
            });
        });
    }
    group.finish();
}

fn bench_compressed_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_compressed_round_trip");
    for &events in &[10usize, 100] {
        let inner = build_inner_payload(events, 256);
        group.throughput(Throughput::Bytes(inner.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(events), &inner, |b, i| {
            b.iter(|| {
                let mut enc = BytesMut::new();
                Frame::Compressed(i.clone()).encode(&mut enc);
                let mut dec = enc;
                Frame::decode(&mut dec).unwrap().unwrap()
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_encode_json,
    bench_decode_json,
    bench_compressed_round_trip,
);
criterion_main!(benches);
