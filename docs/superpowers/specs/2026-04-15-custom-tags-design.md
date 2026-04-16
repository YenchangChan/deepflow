# Lumberjack JSON Output Enhancements

## Summary

Three enhancements to the deepflow-agent Lumberjack JSON output path:

1. **Custom Labels** — per-data-type KV labels injected as a `"labels"` nested JSON object
2. **Topic Routing** — per-data-type Kafka topic injected as `"@topic"` top-level field
3. **Metrics Logging** — periodic (30s) metrics output to console with configurable instance ID

Protobuf serialization path is out of scope for all three features.

---

## 1. Custom Labels

### Configuration

Location: `outputs.labels` in `UserConfig`.

```yaml
outputs:
  labels:
    flow_log:
      - env: production
      - cluster: k8s-prod
    protocol_log:
      - env: staging
    flow_metrics: []
    application_log:
      - region: cn-east
    proc_events: []
    profile: []
```

Each data type holds a list of single-entry maps (`Vec<HashMap<String, String>>`). Omitted categories default to empty (no labels).

### Data Types

| Category | `SendMessageType` | `_msg_type` in JSON |
|---|---|---|
| Flow Log | `TaggedFlow` | `tagged_flow` |
| Protocol Log | `ProtocolLog` | `protocol_log` |
| Flow Metrics | `Metrics` | `metrics` |
| Application Log | `ApplicationLog` | `application_log` |
| Process Events | `ProcEvents` | `proc_events` |
| Profile | `Profile` | `profile` |

### Config Propagation

```
UserConfig.outputs.labels: Labels
  -> handler.rs: flatten Vec<HashMap<String, String>> per type
  -> SenderConfig.labels: HashMap<SendMessageType, Vec<(String, String)>>
  -> LumberjackSender reads via self.config.load()
```

### JSON Output

Labels are injected as a nested object. Only present when labels are configured for the data type.

```json
{
  "labels": {
    "env": "production",
    "cluster": "k8s-prod"
  }
}
```

---

## 2. Topic Routing

### Configuration

Location: `outputs.lumberjack.topics` in `UserConfig`.

```yaml
outputs:
  lumberjack:
    topics:
      flow_log: aimeter_deepflow_l4_flow_log
      l7_flow_log: aimeter_deepflow_l7_flow_log
      flow_metrics: aimeter_deepflow_flow_metrics
      application_log: aimeter_deepflow_application_log
      proc_events: aimeter_deepflow_event
      profile: aimeter_deepflow_profile
      integration: aimeter_deepflow_integration
```

All values have defaults prefixed with `aimeter_deepflow_*`. Empty values fall back to defaults automatically via `LumberjackTopics::fill_empty_with_defaults()`.

### Topic Mapping

7 configurable topics cover 11 `SendMessageType` variants:

| Config Key | Default Topic | Covered Message Types |
|---|---|---|
| `flow_log` | `aimeter_deepflow_l4_flow_log` | `TaggedFlow` |
| `l7_flow_log` | `aimeter_deepflow_l7_flow_log` | `ProtocolLog` |
| `flow_metrics` | `aimeter_deepflow_flow_metrics` | `Metrics` |
| `application_log` | `aimeter_deepflow_application_log` | `ApplicationLog` |
| `proc_events` | `aimeter_deepflow_event` | `ProcEvents` |
| `profile` | `aimeter_deepflow_profile` | `Profile` |
| `integration` | `aimeter_deepflow_integration` | `OpenTelemetry`, `OpenTelemetryCompressed`, `Prometheus`, `Telegraf`, `Datadog` |

External integration sources share a single topic, separate from native `l7_flow_log`.

### Config Propagation

```
UserConfig.outputs.lumberjack.topics: LumberjackTopics
  -> handler.rs: fill_empty_with_defaults(), expand to 11 entries
  -> SenderConfig.topics: HashMap<SendMessageType, String>
  -> LumberjackSender reads via self.config.load()
```

### JSON Output

`@topic` is injected as the first field in the JSON object, before all other metadata.

```json
{
  "@topic": "aimeter_deepflow_l4_flow_log",
  "_agent_id": 1,
  "_msg_type": "tagged_flow",
  ...
}
```

---

## 3. Metrics Logging

### Configuration

Location: `outputs.lumberjack.id` in `UserConfig`.

```yaml
outputs:
  lumberjack:
    id: "aimeter-apm-deepflow-678765765768756"
```

When `id` is non-empty, the Lumberjack sender logs metrics to console every 30 seconds. When `id` is empty (default), no metrics are logged.

### Metrics Format

```
[metrics_logging] out_<id>.timestamp=1776301533020,out_<id>.send_bytes=12668233,out_<id>.send_lines=4565,out_<id>.ack_lines=4565,out_<id>.rx_lines=5000,out_<id>.dropped_lines=435,out_<id>.write_failures=0,out_<id>.retry_successes=0
```

### Metrics Description

| Metric | Description |
|---|---|
| `timestamp` | Current Unix timestamp in milliseconds |
| `send_bytes` | Bytes sent in the last 30s interval |
| `send_lines` | Messages successfully sent (incremental) |
| `ack_lines` | Messages acknowledged (= send_lines in Lumberjack v2 batch ack) |
| `rx_lines` | Messages received from internal queue (incremental) |
| `dropped_lines` | Messages dropped due to rate limiting, pool failure, or serialization error (incremental) |
| `write_failures` | Connection-level write failures (incremental) |
| `retry_successes` | Successful retries after initial failure (incremental) |

All counters are read-and-reset (`AtomicU64::swap(0)`), so each log line represents the delta since the previous log.

### Implementation

`LumberjackSender::log_metrics()` is called in the main `run()` loop when `Instant::elapsed() >= 30s`. Uses `info!` macro via the `log` crate.

---

## Complete JSON Output Example

```json
{
  "@topic": "aimeter_deepflow_l4_flow_log",
  "_agent_id": 1,
  "_team_id": 1,
  "_org_id": 1,
  "_msg_type": "tagged_flow",
  "src_ip": "10.0.0.1",
  "dst_ip": "10.0.0.2",
  "labels": {
    "env": "production",
    "cluster": "k8s-prod"
  }
}
```

Field injection order: `@topic` → `_agent_id` / `_team_id` / `_org_id` → (original fields) → `labels`.

---

## Files Changed

| File | Change |
|---|---|
| `agent/crates/public/src/sender.rs` | Add `Eq`, `Hash` derives to `SendMessageType` |
| `agent/src/config/config.rs` | Add `Labels`, `LumberjackTopics` structs; add `id`, `topics` to `Lumberjack`; add `labels` to `Outputs`; unit tests |
| `agent/src/config/handler.rs` | Add `labels`, `topics`, `lumberjack_id` to `SenderConfig`; flatten/propagate during construction |
| `agent/src/sender/lumberjack_sender.rs` | Inject `@topic` and `labels` in JSON loop; add `log_metrics()` with 30s periodic logging |
| `server/agent_config/template.yaml` | Add `outputs.labels`, `outputs.lumberjack.id`, `outputs.lumberjack.topics` config sections |

## Out of Scope

- Protobuf serialization path (uniform_sender) — deferred to a later phase
- Key/value validation for labels (length, character set, duplicate detection)
- Partial ACK tracking (ack_lines currently equals send_lines)
