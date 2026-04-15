# Custom Tags for Lumberjack JSON Output

## Summary

Add per-data-type custom KV tags to the deepflow-agent. Tags are configured under `outputs.labels` in the agent config and injected as a `labels` nested JSON object in the Lumberjack output path. Protobuf serialization is out of scope for this phase.

## Data Types

The 6 data categories that support custom tags:

| Category | `SendMessageType` | `_msg_type` in JSON |
|---|---|---|
| Flow Log | `TaggedFlow` | `tagged_flow` |
| Protocol Log | `ProtocolLog` | `protocol_log` |
| Flow Metrics | `Metrics` | `metrics` |
| Application Log | `ApplicationLog` | `application_log` |
| Process Events | `ProcEvents` | `proc_events` |
| Profile | `Profile` | `profile` |

## Configuration

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

Each data type holds a list of single-entry maps (`Vec<HashMap<String, String>>`). Omitted categories default to empty (no tags). No validation on key/value length or character set in this phase.

## Rust Config Structs

In `agent/src/config/config.rs`, add to the `Outputs` struct:

```rust
#[derive(Clone, Debug, Default, Deserialize)]
pub struct Labels {
    #[serde(default)]
    pub flow_log: Vec<HashMap<String, String>>,
    #[serde(default)]
    pub protocol_log: Vec<HashMap<String, String>>,
    #[serde(default)]
    pub flow_metrics: Vec<HashMap<String, String>>,
    #[serde(default)]
    pub application_log: Vec<HashMap<String, String>>,
    #[serde(default)]
    pub proc_events: Vec<HashMap<String, String>>,
    #[serde(default)]
    pub profile: Vec<HashMap<String, String>>,
}
```

## Config Propagation

```
UserConfig.outputs.labels: Labels
  -> handler.rs: flatten Vec<HashMap<String, String>> into Vec<(String, String)> per type
  -> SenderConfig.labels: HashMap<SendMessageType, Vec<(String, String)>>
  -> LumberjackSender reads via self.config.load()
```

In `handler.rs`, during `SenderConfig` construction, each category's `Vec<HashMap<String, String>>` is flattened:

```rust
let flatten = |entries: &[HashMap<String, String>]| -> Vec<(String, String)> {
    entries.iter().flat_map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone()))).collect()
};
```

Then assembled into a `HashMap<SendMessageType, Vec<(String, String)>>` keyed by message type.

## JSON Injection

In `lumberjack_sender.rs`, `LumberjackSender::run()` method, after the existing `_agent_id`/`_team_id`/`_org_id` insertion (around line 441):

```rust
if let Some(tags) = cfg.labels.get(&msg.message_type()) {
    if !tags.is_empty() {
        let tag_obj: serde_json::Map<String, serde_json::Value> = tags
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect();
        obj.insert("labels".into(), serde_json::Value::Object(tag_obj));
    }
}
```

Output example:

```json
{
  "src_ip": "10.0.0.1",
  "dst_ip": "10.0.0.2",
  "_agent_id": 1,
  "_team_id": 1,
  "_org_id": 1,
  "labels": {
    "env": "production",
    "cluster": "k8s-prod"
  }
}
```

## Files Changed

| File | Change |
|---|---|
| `server/agent_config/template.yaml` | Add `outputs.labels` config section with documentation |
| `agent/src/config/config.rs` | Add `Labels` struct, add `labels` field to `Outputs` |
| `agent/src/config/handler.rs` | Add `labels` field to `SenderConfig`, flatten and populate during construction |
| `agent/src/sender/lumberjack_sender.rs` | Inject `labels` JSON object in `run()` loop |

## Out of Scope

- Protobuf serialization path (uniform_sender) — deferred to a later phase
- Key/value validation (length, character set, duplicate detection)
- Integration/third-party data types (OpenTelemetry, Prometheus, Telegraf, Datadog) — only the 6 core categories
