# Agent Custom Tags Design

## Overview

Add a generic custom KV tag mechanism to deepflow-agent. Users define static key-value pairs in the agent config, grouped by data type. The agent attaches these tags to data objects in memory. A downstream Lumberjack v2 Sender (out of scope for this spec) reads the tags during serialization.

## Non-Goals

- Ingester/ClickHouse storage changes (user has their own downstream pipeline)
- Lumberjack v2 Sender implementation (separate effort)
- Custom payload format definition (separate effort)
- Tag merging, inheritance, or group references
- Hot reload (agent restart required)

## Configuration

New top-level field `custom_tags` in agent config. Five data type keys, each holding a flat string-to-string map. Unspecified types carry no custom tags.

```yaml
custom_tags:
  flow_log:
    env: "production"
    cluster: "cn-east-1"
    business: "payment"
  flow_metrics:
    env: "production"
    cluster: "cn-east-1"
  event:
    env: "production"
  app_log:
    env: "production"
    owner: "team-a"
  profile:
    env: "production"
```

### Data Type Scope

| Config Key     | Covers                                                        |
|----------------|---------------------------------------------------------------|
| `flow_log`     | l4_flow_log, l7_flow_log, l4_packet, l7_packet                |
| `flow_metrics` | network/application 1s/1m/map variants, traffic_policy        |
| `event`        | resource events, file events                                  |
| `app_log`      | application logs                                              |
| `profile`      | continuous profiling                                          |

### Constraints

- Keys and values are both `String`, no nesting.
- No merging or inheritance between types. Each type's config is self-contained.
- Changes require agent restart to take effect.

## Data Structure

### Type Alias

Defined in `agent/crates/public/`:

```rust
pub type CustomTags = Arc<HashMap<String, String>>;
```

`Arc` allows zero-copy sharing across all data objects of the same type within the agent.

### Config Struct

Parsed during agent config loading in `agent/src/config/`:

```rust
pub struct CustomTagsConfig {
    pub flow_log: Option<HashMap<String, String>>,
    pub flow_metrics: Option<HashMap<String, String>>,
    pub event: Option<HashMap<String, String>>,
    pub app_log: Option<HashMap<String, String>>,
    pub profile: Option<HashMap<String, String>>,
}
```

`None` means no custom tags for that data type.

At agent startup, each `Some(map)` is wrapped into `Arc<HashMap<String, String>>` and passed to the corresponding module.

## Injection Mechanism

### Principle

Inject at the sender boundary, not in core collection logic. Each collection module's sender/writer receives the `CustomTags` reference at initialization and attaches it when building outgoing data objects.

### Injection Points

| Module           | Location                                    | What Happens                                         |
|------------------|---------------------------------------------|------------------------------------------------------|
| flow_log sender  | `agent/src/sender/` (flow log path)         | Attach `custom_tags` to each flow log data object    |
| flow_metrics     | `agent/src/collector/` (document builder)    | Attach `custom_tags` to each metrics document        |
| event            | event data builder                           | Attach `custom_tags` to each event record            |
| app_log          | app log data builder                         | Attach `custom_tags` to each log record              |
| profile          | profile data builder                         | Attach `custom_tags` to each profile record          |

### Data Object Change

Each data object type (FlowLog, Document, EventRecord, etc.) gains a field:

```rust
pub custom_tags: Option<CustomTags>,
```

`Option` so that when no custom tags are configured, there is zero overhead -- no allocation, no field access in the serialization path.

## Serialization Contract

The downstream Lumberjack v2 Sender (out of scope) reads `custom_tags` from the data object and expands it as additional KV pairs in the output frame. The exact serialization format is defined by the Sender implementation.

## Performance Considerations

- **Memory**: One `HashMap` allocation per data type (up to 5), shared via `Arc`. Negligible.
- **CPU**: No per-record computation. Just a pointer copy (`Arc::clone`) per data object.
- **Network**: Static KV pairs are highly compressible due to repetition. As the user noted, compression makes the bandwidth overhead manageable.
