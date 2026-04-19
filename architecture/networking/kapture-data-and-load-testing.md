# Kapture Data Storage and Load Testing

This document proposes a storage format and large-scale replay design for Kapture.
The design is optimized for captured traffic that may be too large to load into a
single process or copy into every replay pod.

## Goals

- Preserve enough request data and timing metadata to replay captured traffic at
  the originally recorded QPS.
- Keep the replay hot path simple: replay agents should read immutable bundles
  from object storage or a local cache, not query a database per request.
- Support aggregation and dataset handlers that transform raw capture segments
  into compact replay bundles.
- Make pre-flight and in-flight validation explicit so a failed load test is
  distinguishable from an invalid dataset, a cold cache, or undersized pods.
- Allow horizontal scaling across many replay agent pods while preserving the
  recorded distribution of routes, tenants, payload sizes, and timing.

## Non-Goals

- Replaying every byte of every response body by default. Response storage should
  be opt-in for validation cases that need it.
- Making raw capture data the replay format. Raw segments are ingestion artifacts;
  replay agents consume processed bundles.
- Coordinating each replay request centrally. The coordinator assigns work and a
  start time, then agents pace requests locally.

## Storage Model

Kapture should use a two-tier storage model:

1. Raw capture segments are append-only objects written during capture.
1. Dataset handlers process raw segments into immutable replay datasets.

Raw data can be large, redundant, and protocol-specific. The replay dataset is
the durable contract for load tests. A dataset is published only after handlers
finish compaction, validation oracle generation, shard assignment, and manifest
checks.

Recommended object layout:

```text
kapture/
  captures/<capture-id>/
    raw/<source>/<yyyy>/<mm>/<dd>/<hh>/<segment-id>.kraw.zst
    capture.json
  datasets/<dataset-id>/
    dataset.json
    indexes/
      shard-map.json
      route-summary.json
      qps-windows.json
    bundles/
      shard=<n>/window=<start>-<end>/bundle-<seq>.kbnd.zst
    payloads/
      sha256/<first-two>/<body-sha>.blob.zst
```

Object storage is the system of record. A catalog database can index dataset
metadata for discovery, but replay must not require database reads after a pod
has received its assignment.

## Raw Capture Segment Format

Raw capture segments should be optimized for safe, append-only writes from the
capture path. They are handler inputs, not replay inputs.

`*.kraw.zst` should be a compressed, length-delimited stream:

- `SegmentHeader`: capture id, source id, segment id, schema version, capture
  start time, node or pod identity, and redaction state.
- `RequestStart`: request id, observed timestamp, protocol, method, authority,
  path, selected headers, and route metadata if available.
- `RequestBodyChunk`: request id, sequence number, body bytes or a body chunk
  hash when bytes have already been externalized.
- `ResponseHeaders`: request id, observed timestamp, status, selected response
  headers, and upstream metadata.
- `ResponseBodyChunk`: optional response body bytes for validation modes that
  need them.
- `RequestEnd`: request id, observed timestamp, byte counts, transport result,
  and error details if the request did not complete normally.
- `SegmentFooter`: record count, request count, byte count, time range, and
  checksum.

Raw segment writers should rotate by size and time so failures lose at most one
small segment. A practical initial target is 128-512 MiB compressed or 1-5
minutes of traffic per segment, whichever comes first.

## Dataset Manifest

`dataset.json` is small enough to load fully into each agent. It describes the
dataset, its integrity, the replay schedule, and bundle assignments.

```json
{
  "version": "kapture.dataset.v1",
  "datasetId": "checkout-prod-2026-04-18T10-00Z",
  "sourceCaptureIds": ["checkout-prod-2026-04-18"],
  "createdAt": "2026-04-18T12:45:00Z",
  "recordedStart": "2026-04-18T10:00:00Z",
  "recordedEnd": "2026-04-18T11:00:00Z",
  "durationSeconds": 3600,
  "requestCount": 184000000,
  "targetQPS": {
    "average": 51111.1,
    "peakOneSecond": 92000,
    "windows": "indexes/qps-windows.json"
  },
  "bundleDefaults": {
    "compression": "zstd",
    "targetCompressedMiB": 128,
    "maxCompressedMiB": 512
  },
  "shards": {
    "count": 1024,
    "assignment": "indexes/shard-map.json"
  },
  "validation": {
    "mode": "status-class-and-header-fingerprint",
    "oraclesIncluded": true,
    "maxAllowedMismatchRatio": 0.001
  },
  "redactionPolicy": {
    "id": "prod-http-v3",
    "state": "applied"
  },
  "replaySizeBudget": {
    "compressedBytes": 68719476736,
    "largestBundleBytes": 536870912,
    "largestPayloadBytes": 67108864,
    "minimumPerPodCacheBytes": 2147483648
  },
  "integrity": {
    "manifestSha256": "<sha256>",
    "bundleSetSha256": "<sha256>",
    "payloadSetSha256": "<sha256>"
  }
}
```

The manifest should include summary counters that make pre-flight checks cheap:
request count, byte count, time range, route distribution, body payload count,
largest bundle, largest payload, and expected object storage footprint.

## Bundle Format

Replay agents should consume `*.kbnd.zst` bundles. A bundle is a compressed,
length-delimited stream with a small header followed by request records. The
header contains dictionaries and bundle metadata; the records reference those
dictionaries to avoid repeating headers, route names, hosts, and validation
templates.

Suggested logical sections:

- `BundleHeader`: dataset id, shard id, recorded time range, record count, byte
  count, compression details, schema version, and checksums.
- `Dictionaries`: methods, schemes, authorities, path templates, header names,
  common header values, upstream clusters, route ids, tenant ids, and validation
  oracle ids.
- `Schedule`: per-request timestamp delta from recorded start, or delta from the
  previous request in the bundle.
- `Requests`: method reference, authority reference, path or path-template
  reference, header block reference, body reference, timeout, and retry policy.
- `Bodies`: inline bodies for small payloads and content-addressed references for
  large or repeated payloads.
- `ValidationOracles`: expected status class, selected response headers, optional
  response fingerprints, and route-specific success criteria.

The bundle format should favor sequential reads. Random access indexes are useful
for debugging, but the replay loop should be able to stream through its assigned
bundles with bounded memory.

## Data Size Controls

Captured traffic can become the limiting factor before QPS does. Kapture should
make data reduction an explicit part of dataset creation:

- Store repeated request bodies once by `sha256` and reference them from records.
- Inline only small bodies; move larger bodies into `payloads/`.
- Dictionary-encode header names, common values, hosts, route ids, and tenants.
- Drop response bodies unless a validation handler opts in.
- Store response validation as compact oracles: status class, selected headers,
  content length buckets, hashes, regex ids, or semantic checks.
- Redact before bundling, and store the redaction policy id in the manifest.
- Split bundles by time window and shard so no agent needs the entire dataset.
- Target 64-256 MiB compressed bundles, with a hard maximum near 512 MiB.
- Keep raw captures on a shorter retention policy after the replay dataset is
  published and verified.

The dataset manifest should expose a `replaySizeBudget` summary so the planner
can fail early when the dataset cannot fit the configured object store, node
cache, or per-pod ephemeral disk budget.

## Aggregation and Dataset Handlers

Kapture should model dataset creation as a handler pipeline. Each handler reads
raw segments or intermediate records and emits a smaller, more replayable
artifact.

Recommended handlers:

- `NormalizeHandler`: canonicalizes protocol fields, timestamps, headers, and
  route identity.
- `RedactionHandler`: removes or transforms sensitive data before durable replay
  artifacts are written.
- `BodyDedupeHandler`: hashes request bodies and emits content-addressed payload
  blobs.
- `RouteAggregationHandler`: computes per-route, per-tenant, and per-status
  histograms for sharding and validation.
- `TimingHandler`: converts wall-clock capture times into replay deltas and QPS
  windows.
- `ValidationOracleHandler`: builds compact expected-result records.
- `ShardPlannerHandler`: assigns records to replay shards while preserving QPS
  and traffic mix.
- `BundleWriterHandler`: writes immutable bundles and bundle checksums.
- `DatasetPublisherHandler`: writes the final manifest only after every bundle
  and index has passed integrity checks.

This keeps raw ingestion independent from replay optimization. New protocols or
validation modes can add handlers without changing the replay agent contract.

## Replay Load Test Architecture

The load test should have three control-plane pieces:

- `Dataset planner`: reads `dataset.json`, applies the target speed factor and
  resource limits, and produces pod assignments.
- `Replay coordinator`: creates or scales replay agents, waits at a start barrier,
  and monitors aggregate progress.
- `Replay agents`: sidecar-style pods that fetch assigned bundles, pace requests
  locally, send traffic, and report metrics.

Replay agents should be stateless beyond local cache. A pod can be replaced by
another pod that receives the same shard assignment and starts from the next
bundle boundary.

## Replay Agent Contract

A replay agent should expose a small state machine to the coordinator:

- `Assigned`: the coordinator has written the dataset id, shard ids, bundle URIs,
  target speed factor, validation mode, and replay start time.
- `Prefetching`: the agent is reading assigned bundle headers and warming its
  local cache.
- `ReadyForReplay`: the agent has enough local data, target connectivity, and
  clock confidence to participate in the start barrier.
- `Running`: the agent is pacing requests from its assigned bundles.
- `Draining`: no new scheduled requests are being started; in-flight requests are
  allowed to finish.
- `Complete`: every assigned bundle was consumed and reported.
- `Failed`: the agent cannot make valid QPS or validation claims for its assigned
  shards.

This state is also useful for Kubernetes readiness. Pods should not become
`ReadyForReplay` until the first replay window is local or streamable within the
configured lag budget.

## Hub Connectivity Sensors

Replay agents should use a sensor pattern to prove that they can connect to the
hub and to any hub-adjacent services they need before the test starts. The hub
should not carry a hardcoded list of sensor names. Instead, each agent registers
the sensors it supports, the sensor schema version, and the current result.

Suggested sensor flow:

1. Agent starts and opens a control-plane connection to the hub.
1. Agent sends a `SensorCatalog` containing supported sensor ids, versions,
   labels, and result schemas.
1. Hub stores the catalog with the agent session and evaluates required sensors
   by policy or label selectors.
1. Agent periodically sends `SensorResult` records with status, observed latency,
   last success time, failure reason, and optional structured details.
1. Hub gates `ReadyForReplay` on the policy-selected sensors, not a compiled-in
   sensor list.

Initial sensors should include:

- `hub.control.grpc`: can open and maintain the control stream to the hub.
- `hub.assignment.read`: can receive dataset and shard assignments.
- `hub.metrics.write`: can publish progress and validation metrics.
- `object-store.read`: can read the assigned dataset manifest and bundle prefix.
- `target.connectivity`: can reach the configured target service or gateway.
- `clock.skew`: local clock is within the configured pacing tolerance.

The important contract is that the hub understands sensor metadata and generic
pass, warn, fail, or unknown states. New sensors can be added by the agent image
or by sidecar plugins without changing hub code, as long as policy can select
them by id, label, or capability.

## Loading Large Datasets

Do not copy the full dataset to every pod. Use shard-local loading:

1. The coordinator assigns each pod a list of shard ids and bundle URIs.
1. An init container or agent warm-up phase prefetches only the assigned bundles
   and large payloads needed for the first replay window.
1. The agent streams later bundles from object storage or a node-local cache
   while replay is running.
1. A daemonset cache can optionally keep hot bundles on each node when repeated
   tests use the same dataset.

The planner should account for object store throughput. A test can fail before
traffic starts if `requiredPrefetchBytes / warmupDuration` exceeds the measured
or configured storage bandwidth.

## Achieving Recorded QPS

The dataset stores recorded timestamps as deltas from `recordedStart`. At test
start, the coordinator sends agents a `replayStartTime` and `speedFactor`.
Agents compute due time locally:

```text
due = replayStartTime + (recordedDelta / speedFactor)
```

To avoid central bottlenecks:

- Use local pacers in each agent, not a central request scheduler.
- Assign shards so each agent receives a representative slice of QPS and routes.
- Track lag as `now - due` for each request. Lag is a first-class metric.
- Fail or degrade explicitly when lag exceeds a configured threshold.
- Calibrate per-pod maximum QPS with a synthetic bundle before the full run.

The planner should calculate pod count using all major limits:

```text
pods = max(
  ceil(target_peak_qps / measured_pod_peak_qps),
  ceil(target_avg_egress_bps / measured_pod_egress_bps),
  ceil(required_hot_bytes / per_pod_cache_budget),
  configured_min_pods
)
```

For finite runs, Kubernetes Indexed Jobs map cleanly to shard assignments. For
long-running or repeated tests, a Deployment plus coordinator-owned assignments
is easier to rescale and observe.

## Scaling Pods

Large tests should pre-scale before the start barrier. HPA reacts too late for a
replay that must match the first recorded QPS spike.

Recommended flow:

1. Run a calibration phase for the selected agent image, resource requests, and
   target cluster.
1. Compute required pods from peak QPS, payload bandwidth, CPU, memory, cache
   size, and object store read bandwidth.
1. Create the Job or Deployment at the computed size.
1. Wait for every pod to report `ReadyForReplay`.
1. Start the replay at a future timestamp far enough away for all pods to receive
   the barrier message.

The coordinator should support over-provisioning. It is better for agents to run
at 70 percent of measured capacity than to start a production-scale test with no
headroom for GC, TLS handshakes, object store jitter, or target-side latency.

## Pre-Flight Validation

Pre-flight checks should run before any load is sent:

- Dataset manifest is present, schema-compatible, and immutable.
- Every bundle and payload object referenced by the manifest exists.
- Bundle checksums, record counts, byte counts, and time ranges match indexes.
- Dataset QPS windows cover the requested replay interval and speed factor.
- Route distribution and payload size summaries match expected test intent.
- Redaction policy is present and marked successful.
- Replay pod count is sufficient for QPS, bandwidth, cache, and CPU budgets.
- Object storage warm-up throughput is sufficient.
- Kubernetes quota, node capacity, PodDisruptionBudgets, and image pulls are
  ready before the start barrier.
- Target services, DNS, mTLS, certificates, and ext-proc clusters are reachable.
- A dry-run can read bundles and construct requests without sending traffic.
- Required agent sensors are registered and passing according to hub policy.

Pre-flight output should be machine-readable so automation can block unsafe
tests and explain the exact failed check.

## In-Flight Validation

Agents should emit per-shard metrics and periodic progress records:

- Scheduled, sent, completed, failed, retried, and skipped requests.
- Target QPS, actual QPS, and replay lag by second.
- Bundle read latency, cache misses, object store retries, and bytes read.
- Request latency, connection errors, status codes, and timeout counts.
- Validation mismatches by oracle id, route id, and status class.
- Agent CPU, memory, open connections, and ephemeral disk usage.

The coordinator should aggregate these into run-level health:

- `on_schedule`: actual QPS is within tolerance and lag is below threshold.
- `data_healthy`: bundle reads and cache misses are not throttling replay.
- `target_healthy`: target error rate and latency are within expected bounds.
- `validation_healthy`: response oracles are passing within tolerance.

If an agent cannot keep up, it should report lag and missed deadlines rather than
silently sending late traffic. Depending on the test mode, the coordinator can
either fail fast or continue and mark the affected shards as invalid for QPS
claims.

## Validation Modes

Not every load test needs strict response comparison. Kapture should support
graduated validation modes:

- `send-only`: prove QPS and target capacity, with transport errors only.
- `status-class`: validate expected 2xx, 3xx, 4xx, or 5xx classes.
- `header-fingerprint`: validate selected headers and status class.
- `body-fingerprint`: validate body hash, length bucket, or regex id.
- `handler-defined`: delegate validation to a dataset handler-specific oracle.

The validation mode is stored in `dataset.json` so replay results are comparable.

## Operational Guardrails

- Never mount datasets through ConfigMaps or Secrets.
- Avoid one PVC per pod containing the full dataset.
- Cap per-agent in-memory queued requests.
- Make start time, speed factor, pod count, dataset id, and validation mode part
  of the run record.
- Treat clock skew as a pre-flight failure if it can affect QPS pacing.
- Preserve enough run metadata to replay the same bundle assignment later.
- Add new agent readiness checks as sensors so the hub remains capability-driven
  instead of accumulating hardcoded probe names.

## Open Questions

- Which object store is the first supported backend: S3, GCS, Azure Blob, or
  in-cluster MinIO?
- What is the initial replay agent implementation language and preferred binary
  encoding for `*.kbnd` records?
- What is the default retention policy for raw captures after dataset publishing?
- Should shard assignment preserve source-client affinity, or is route and QPS
  distribution sufficient for the first version?
- Which validation mode should be the default for production captures?
- Should hub sensor policy be stored in the dataset manifest, the run spec, or a
  separate cluster-level policy object?
