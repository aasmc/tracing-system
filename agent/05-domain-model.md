# 05 — Domain Model of the Trace Collector System (TCS)

**Related documents**

- [01 — Technology overview (Tempo, OpenTelemetry, Jaeger, ...)](01-technology-overview.md)
- [02 — High-level design (C4, ADRs)](02-high-level-design.md)
- [03 — Low-level design of the services](03-low-level-design.md)
- [04 — Storage design (PostgreSQL, capacity)](04-storage-design.md)
- **05 — Domain model (this document)**
- [06 — Use cases](06-use-cases.md)
- [07 — Sequence diagrams](07-sequence-diagrams.md)
- [08 — ER diagrams](08-er-diagrams.md)

This document gives TCS one vocabulary and a model that every service shares. It covers:

- the ubiquitous language;
- the bounded contexts and how they relate;
- aggregates, entities and value objects as Java 25 code with their invariants;
- the state machines;
- the internal domain events;
- the exact mapping from the producer's wire format (`TraceMessage` v1) to the domain and to the PostgreSQL schema `tc`.

Code lives in the shared-kernel module `tc-domain` (base package `ru.aasmc.tracing.collector.domain`). That module is pure Java with no Spring, JDBC or Kafka dependencies. Persistence code (`tc-persistence`) and Kafka code (`tc-kafka-support`) depend on `tc-domain`, never the reverse.

> **Implementation note (2026-11-22).** The `tc-domain` API was built from this document in Phase 1a. Names, invariants that changed (name pattern, mapping count, MASK argument, reserved target keys, `TimeRange`, `SpanEvent` time, index token size, …) and added types are listed in [09 §2.6](09-implementation-plan.md#26-phase-1a-results-2026-11-22). Where the two differ, the code and 09 §2.6 win.

Package layout of `tc-domain`:

| Package | Contents |
|---|---|
| `domain.trace` | `TraceId`, `SpanId`, `Span`, `TraceFragment`, `TraceSummary`, `TraceDay`, `TimeRange`, `SpanKind`, `SpanStatus`, `TraceFlags`, `SpanFlag`, `Completeness` |
| `domain.attribute` | `AttributeKey`, `AttributeValue` (sealed), `Attributes`, `AttributeOrigin`, `IndexedAttribute`, `DigestTransform`, `PiiClass` |
| `domain.resource` | `Resource`, `ResourceId`, `InstrumentationScope`, `ScopeId`, `ServiceId`, `SpanNameId` |
| `domain.enrichment` | `KafkaConnection`, `EnrichmentSource`, `SourceRevision`, `SourceDefinition`, `TraceContextExtraction` (sealed), `FieldMapping`, `Transform` (sealed), `EnrichmentRecord` |
| `domain.correlation` | `CorrelationType`, `CorrelationKey`, `OutcomeRule`, `CorrelationObservation`, `CorrelationValue`, `BusinessOperation`, `OperationForest` |
| `domain.config` | `ConfigRevision`, `ConfigSnapshot`, `RetentionPolicy`, `RetentionTier`, `RetentionTiers` |
| `domain.digest` | `TraceDigest`, `BusinessOperationDigest`, `DigestStep`, `TraceOutcome`, `OperationOutcome`, `CompactionExclusion`, `CompactionState`, `CompactionStatus` (ADR-13, two-tier retention) |
| `domain.event` | `DomainEvent` (sealed) and its records |
| `domain.dlq` | `DeadLetterReason`, `DeadLetter` |

---

## 1. Ubiquitous language

The terms below are used with exactly these meanings in code, API, UI and documentation.

| Term | Definition | Code / storage |
|---|---|---|
| **Trace** | Every span that shares one 128-bit trace id. A trace is never stored or delivered as a single unit. It is assembled at read time from spans, the summary, enrichments, links and correlations. | `TraceId`; `tc.span` rows with the same `trace_id` |
| **Trace Fragment** | One Kafka record on `otlp-traces`: a `TraceMessage` holding the spans of one trace that a single `BatchSpanProcessor` export in a single service instance produced. A trace normally arrives as many fragments, possibly minutes apart and from many services. Every fragment of a trace lands in the same partition, because key = traceId. | `TraceFragment` (transient, never stored as such) |
| **Span** | One timed operation: id, parent, name, kind, start time, duration, status, attributes, events and links. Spans are **immutable** once stored. | `Span`; `tc.span` |
| **Root span** | A span with no parent (wire `parentSpanId = "0000000000000000"`). A trace normally has exactly one. | `Span.isRoot()` |
| **Orphan span** | A span whose parent id is not, or not yet, present among the stored spans of its trace. | read-model concept |
| **Resource** | The entity that produced telemetry, described by attributes such as `service.name`, `service.instance.id` or `host.name`. The wire repeats it in every span. TCS stores it once, identified by a content hash. | `Resource`, `ResourceId`; `tc.resource` |
| **Instrumentation Scope** | The library that created the span (name, version, schema URL, attributes). Deduplicated by content hash. | `InstrumentationScope`; `tc.instrumentation_scope` |
| **Service** | The value of the resource attribute `service.name`. It is dictionary-encoded. | `ServiceId`; `tc.service` |
| **Span Name** | Dictionary entry for a span name within one service. There is a cardinality guard of 5 000 names per service; overflowing names go to `span.name_overflow`. | `SpanNameId`; `tc.span_name` |
| **Trace Summary** | The trace-level header: min start, max end, root service and name, span and error counts, services, fragment count and ingest timestamps. It is built incrementally and commutatively from the spans that were actually inserted. | `TraceSummary`; `tc.trace_summary` |
| **Trace Day** | The UTC date a trace's summary row is filed under, which is the partition key of `trace_summary`. It is fixed when the trace is first seen (`TraceDayResolver`) and then reused for later fragments. | `TraceDay`; `trace_summary.trace_day` |
| **Trace Completeness** | The derived state of a trace: `RECEIVING` while fragments may still arrive, `SETTLED` after an idle period. There is also a `ROOTLESS` flag. It is never stored. | `Completeness` |
| **Attribute** | A typed key/value pair. The types are OTel's: STRING, BOOLEAN, LONG, DOUBLE and their arrays. | `AttributeKey`, `AttributeValue` |
| **Attribute Origin** | Where an attribute value came from: `SPAN` (1), `RESOURCE` (2), `ENRICHMENT` (3) or `DERIVED` (4, computed by TCS, e.g. `status=ERROR`). | `AttributeOrigin`; `origin smallint` |
| **Indexed Attribute** | An attribute key registered for search. Only these keys are written to the inverted index `trace_attribute`. Built-in keys are `service.name`, `span.name` (root only) and `status`. | `IndexedAttribute`; `tc.indexed_attribute`, `tc.trace_attribute` |
| **Brute-force scan** | Bounded search on non-indexed keys by scanning `span.attributes`. The window is at most 1 h and a statement timeout applies. | UC-13 |
| **Kafka Connection** | How to reach a client's Kafka cluster: bootstrap servers, security protocol and SASL mechanism, plus secret *references* (never the secrets). | `KafkaConnection`; `tc.kafka_connection` |
| **Enrichment Source** | A client-defined Kafka topic whose events carry trace context. TCS turns those events into additional trace attributes. It is configured as data, not code. | `EnrichmentSource`; `tc.enrichment_source` |
| **Source Revision** | An immutable, numbered version of a source's definition. Running containers always process exactly one revision. | `SourceRevision`; `tc.enrichment_source_revision` |
| **Source Definition** | The content of a revision: payload format, trace-context extraction, scope, filter, start position, missing-context policy and field mappings. | `SourceDefinition`; `definition jsonb` |
| **Trace Context Extraction** | The strategy that finds the trace (and optionally span) id in an enrichment event: `TRACEPARENT_HEADER`, `RAW_HEADER` or `PAYLOAD_POINTER`. | `TraceContextExtraction` (sealed) |
| **Field Mapping** | A rule taking one field from the event (payload JSON Pointer, header, record key or record timestamp), applying a **Transform**, and producing one attribute `targetKey` of a declared type. | `FieldMapping`, `Transform` |
| **Enrichment Record** | One attribute value derived from one enrichment event. It is append-only, carries the Kafka record coordinates, and is idempotent on them. | `EnrichmentRecord`; `tc.trace_enrichment` |
| **Correlation Type** | A named business-operation family (e.g. `idp.login`). It declares the attribute keys whose values identify an operation, plus a merge window. | `CorrelationType`; `tc.correlation_type` |
| **Correlation Key** | An attribute key that belongs to a correlation type. Its role is `PRIMARY` (the natural business id, used as the display value) or `ALIAS` (a secondary id that bridges traces, e.g. an authorization code). It may carry a step label. | `CorrelationKey`; `tc.correlation_key` |
| **Correlation Observation** | One fact: "trace X carries value V for correlation key K". It is appended synchronously by the ingester or the enricher. | `CorrelationObservation`; `tc.trace_correlation` |
| **Correlation Value** | The current mapping from (correlation type, value) to business operation, maintained by the resolver. | `CorrelationValue`; `tc.correlation_value` |
| **Business Operation** | The set of traces that represent one business action (one IdP login) and are connected through shared correlation values. It can be merged into another operation. | `BusinessOperation`; `tc.business_operation` |
| **Merge** | Joining two business operations when a later observation shows they share a value. The loser points to the survivor through `merged_into`. Implemented as union-find. | `OperationForest` |
| **Span Link** | A native OTel link from a span to a span of another trace, indexed in both directions. | `SpanLinkRef`; `tc.span_link` |
| **Retention Policy** | Per table: its retention **tier** (`FULL` or `DIGEST`), how long data is kept, the partition granularity, and how far ahead partitions are pre-created. Durations are set per tier, not per table (ADR-13). | `RetentionPolicy`; `tc.retention_policy` |
| **Full tier** | All detailed trace data: `span`, `trace_attribute`, `span_link`, `trace_summary`, `trace_enrichment`, `trace_correlation`, `correlation_value`, `business_operation`. Kept `fullDays` = n days (default 7, min 2). Span-level detail, brute-force search and every search operator exist only here. | `RetentionTier.FULL`; `retention_policy.tier = 'FULL'` |
| **Digest tier** | The compact history kept after the full tier ends: `trace_digest` and `business_operation_digest`. Kept `digestDays` = m days (default 90) **after** the full tier, so a digest day is dropped when it is older than n + m days (default 97). `digestDays = 0` disables the tier. | `RetentionTier.DIGEST`; `retention_policy.tier = 'DIGEST'` |
| **Trace Digest** | One row per trace in the digest tier: start, duration, root service and name, span and error counts, services, trace outcome, business operation id, and the retained attributes. No spans, events, links or enrichment lineage. About 0.5 KB per trace (capacity in 04 §9). | `TraceDigest`; `tc.trace_digest` |
| **Business Operation Digest** | One row per root business operation in the digest tier: type, display value, first/last seen, trace count, **operation outcome**, ordered steps (one per member trace) and the union of the members' retained attributes. | `BusinessOperationDigest`; `tc.business_operation_digest` |
| **Retained attribute** | An indexed attribute key flagged `retain_in_digest = true`. Its values are copied from `trace_attribute` into `trace_digest.attributes` at compaction. A retained key must be indexed: compaction never reads spans. Flagging a key is **not retroactive** for already-compacted days. | `IndexedAttribute.retainInDigest`; `indexed_attribute.retain_in_digest` |
| **Digest transform** | How a retained value is stored in the digest: `NONE` (as is), `SHA256` (lowercase hex of SHA-256 over the UTF-8 value) or `MASK` (first 2 chars + `***`). It lets PII stay joinable (SHA256) or recognisable (MASK) without keeping the raw value for n + m days. | `DigestTransform`; `indexed_attribute.digest_transform` |
| **Outcome rule** | Per correlation type, optional: how to derive the outcome of a business operation at compaction from `outcomeKey` + `successValues` / `failureValues`, member errors, and `finalStepLabel`. Evaluated in a fixed order (§4.10). | `OutcomeRule`; `correlation_type.outcome_rule jsonb` |
| **Operation outcome** | Result of the outcome rule for one business operation: `UNKNOWN` (0), `SUCCESS` (1), `FAILED` (2), `INCOMPLETE` (3). The trace-level counterpart is the **trace outcome**: `OK` (1, `error_count = 0`) or `ERROR` (2). | `OperationOutcome`, `TraceOutcome`; `business_operation_digest.outcome`, `trace_digest.outcome` |
| **Compaction** | The batch job (`DigestCompactor` in `tc-maintenance`) that turns one whole trace day of full-tier data, older than n days, into digest rows with set-based `INSERT ... SELECT` statements, in 1-hour chunks, resumably and idempotently. It never reads `span`. | `CompactionState`; `tc.compaction_state` |
| **Compaction exclusion** | A configured pattern (`service_name`, `root_name_pattern` as SQL LIKE) for traces that are **not** digested, e.g. health checks `GET /actuator/%`, unless the trace belongs to a business operation. | `CompactionExclusion`; `tc.compaction_exclusion` |
| **Drop gate** | The rule in `PartitionManager` that a full-tier partition may be dropped only if it is older than n days **and** every trace day whose compaction read window overlaps it is `DONE`. Uncompacted data is never dropped; if compaction lags, full-tier data is kept longer. | `PartitionManager`; `compaction_state.status` |
| **Config Revision** | A single monotonically increasing counter that `tc-control-plane` bumps in the same transaction as any config change. Data-plane services poll it every 10 s and reload their snapshot when it changes. | `ConfigRevision`; `tc.config_revision` |
| **Config Snapshot** | An immutable, in-memory copy of all config tables at one config revision. | `ConfigSnapshot` |
| **DLQ record** | A Kafka record that could not be processed. It is copied byte-for-byte to `otlp-traces.dlq` or `tc-enrichment.dlq` with diagnostic headers. | `DeadLetter`, `DeadLetterReason` |
| **Watermark** | The resolver's position in `trace_correlation` (`correlation_resolver_state.last_processed_id`). | `correlation_resolver_state` |
| **PII class** | How sensitive an attribute key is (`NONE`, `PERSONAL` or `SECRET`). It drives masking in the Query context. | `PiiClass` |

Words that are deliberately **not** used:

- "Message" for a span. A message is a Kafka record, i.e. a fragment.
- "Tag" for an attribute. "Tag" appears only when quoting Micrometer and Spring property names.
- "Session" for a business operation. A session is just one possible correlation key (`idp.auth_session_id`).

---

## 2. Bounded contexts

### 2.1 Contexts and their responsibilities

| Context | Owner deployable(s) | Core concepts | Owns tables (writes) |
|---|---|---|---|
| **Ingestion** | `tc-ingester` | TraceFragment, Span, TraceSummary, Resource, InstrumentationScope, dictionaries, TraceDay | `span`, `trace_summary`, `resource`, `instrumentation_scope`, `service`, `span_name`, `attribute_key` (insert of new keys), `span_link`; appends to `trace_attribute` and `trace_correlation` |
| **Enrichment** | `tc-enricher` | EnrichmentSource runtime, TraceContextExtraction, FieldMapping, EnrichmentRecord | `trace_enrichment`; appends to `trace_attribute` and `trace_correlation` |
| **Correlation** | `tc-maintenance` (CorrelationResolver). The extraction half runs inside Ingestion and Enrichment. | CorrelationType, CorrelationObservation, CorrelationValue, BusinessOperation | `business_operation`, `correlation_value`, `correlation_resolver_state` |
| **Query** | `tc-query-api`, `tc-ui` | TraceView, TraceSummaryView, TraceDigestView, BusinessOperationView, SearchCriteria (split at the tier boundary), PiiMaskingPolicy | none (read replica only); reads both tiers |
| **Configuration (control plane)** | `tc-control-plane` | KafkaConnection, EnrichmentSource definition and lifecycle, IndexedAttribute (incl. retainInDigest, digestTransform), CorrelationType (incl. OutcomeRule), RetentionPolicy / RetentionTiers, CompactionExclusion, ConfigRevision | `kafka_connection`, `enrichment_source`, `enrichment_source_revision`, `indexed_attribute`, `correlation_type`, `correlation_key`, `retention_policy`, `compaction_exclusion`, `config_revision`; inserts into `attribute_key` |
| **Retention** | `tc-maintenance` (PartitionManager, **DigestCompactor**, AttributeBackfill, DictionaryGc, StatsRefresher) | RetentionPolicy enforcement, Partition, **Compaction** (TraceDigest, BusinessOperationDigest, OutcomeRule evaluation, CompactionState), Drop gate | DDL on partitioned tables; `trace_digest`, `business_operation_digest`, `compaction_state`; `indexed_attribute.status` (BACKFILLING to ACTIVE); `resource` GC; `correlation_value` cleanup |

`tc-schema-migrator` is not a context. It is infrastructure that owns the DDL migrations for all contexts.

**Retention now owns compaction (ADR-13).** Before two-tier retention, the Retention context only dropped partitions. It now also **produces** data: `DigestCompactor` reads the full tier (`trace_summary`, `trace_attribute`, `trace_correlation`, `correlation_value`, `business_operation`, `correlation_key`) and writes the digest tier. Retention is the only writer of `trace_digest`, `business_operation_digest` and `compaction_state`, and the only reader of the outcome rule at runtime. Compaction and dropping live in the same context on purpose: the drop gate needs `compaction_state`, and keeping both in one deployable (`tc-maintenance`) means the "never drop uncompacted data" invariant is enforced without cross-service coordination.

### 2.2 Context map

```mermaid
flowchart LR
    subgraph EXT["External / upstream"]
        PROD["Producer context<br/>microservices + otlp-kafka-export-tracing-starter"]
        CLI["Client enrichment producers<br/>e.g. sms-gateway"]
        IDP["Organisation IdP<br/>OIDC"]
    end

    subgraph TCS["Trace Collector System"]
        ING["Ingestion<br/>tc-ingester"]
        ENR["Enrichment<br/>tc-enricher"]
        COR["Correlation<br/>tc-maintenance: CorrelationResolver"]
        QRY["Query<br/>tc-query-api + tc-ui"]
        CFG["Configuration / Control plane<br/>tc-control-plane"]
        RET["Retention<br/>tc-maintenance: PartitionManager etc."]
        SK[["Shared kernel<br/>tc-domain"]]
    end

    PROD -- "Published Language: TraceMessage v1 on otlp-traces<br/>TCS = Conformist + ACL (TraceMessageParser)" --> ING
    CLI -- "Client-owned JSON events + traceparent<br/>TCS = Conformist via configurable ACL (FieldMapping)" --> ENR
    IDP -- "JWT claims / roles<br/>Conformist + role mapping ACL" --> QRY
    IDP -- "JWT claims / roles" --> CFG

    CFG -- "Customer/Supplier: config tables + config_revision (Open Host: ConfigSnapshot)" --> ING
    CFG -- "Customer/Supplier: enrichment_source revisions" --> ENR
    CFG -- "correlation_type / correlation_key" --> COR
    CFG -- "retention_policy (tiers), indexed_attribute (retain_in_digest), outcome_rule, compaction_exclusion" --> RET

    ING -- "trace_correlation (published table)" --> COR
    ENR -- "trace_correlation (published table)" --> COR
    ING -- "span, trace_summary, trace_attribute, span_link" --> QRY
    ENR -- "trace_enrichment, trace_attribute" --> QRY
    COR -- "business_operation, correlation_value" --> QRY
    RET -- "trace_digest, business_operation_digest (published tables)" --> QRY
    ING -. "compacted by" .-> RET
    COR -. "compacted by" .-> RET
    RET -. "drops partitions of" .-> ING
    RET -. "drops partitions of" .-> ENR
    RET -. "drops partitions of" .-> COR

    SK --- ING
    SK --- ENR
    SK --- COR
    SK --- QRY
    SK --- CFG
    SK --- RET
```

### 2.3 Relationships explained

1. **Producer to Ingestion: Published Language plus Conformist plus Anti-Corruption Layer.**
   - The producer starter defines the language, `TraceMessage` with `schemaVersion = 1`. TCS cannot change it and conforms.
   - `TraceMessageParser` (`ingest.parse`) and `SpanNormalizer` (`ingest.normalize`) form the ACL:
     - the wire's redundant, stringly-typed structure becomes typed value objects;
     - ids, times and enums are normalised;
     - duplicated contexts are dropped;
     - resources are deduplicated.
   - Unknown `schemaVersion` values are not interpreted. They go to the DLQ (UC-03).
   - A future `schemaVersion = 2` (resource hoisted to message level, `exportedAt`, `messageId`; see 02) would get a second parser behind the same ACL interface.
2. **Client enrichment producers to Enrichment: Conformist through configurable ACL.**
   - TCS does not dictate the payload format of client topics. It requires only a trace context.
   - The translation is data (field mappings), not code, so each `SourceRevision` is effectively a small, versioned ACL.
3. **Configuration to the data-plane contexts: Customer/Supplier with an Open Host.**
   - The control plane is the only writer of config tables.
   - Consumers read a consistent `ConfigSnapshot` keyed by `config_revision`. That snapshot is the "open host" protocol: a versioned read model that every data-plane service loads the same way (`ConfigSnapshotPoller` in `tc-ingester`, `EnrichmentSourceRegistry` in `tc-enricher`).
4. **Ingestion/Enrichment to Correlation: Customer/Supplier through a published table.**
   - `trace_correlation` is an append-only log of observations. Correlation consumes it asynchronously by watermark.
   - No Kafka topic is used (ADR-01: only pre-created topics, and no new topics). The table is the integration contract.
5. **Shared Kernel `tc-domain`.**
   - Contains `TraceId`, `SpanId`, `AttributeKey`, `AttributeValue`, `Attributes`, `AttributeOrigin`, `ConfigRevision`, `ConfigSnapshot` and the definition records.
   - Changes require agreement of all contexts, because all modules live in one Maven repo, `trace-collector`.
   - The kernel is intentionally small and free of behaviour that belongs to a single context. For example, union-find is in `domain.correlation` and is used only by the resolver.
6. **Query: Conformist to the persistence schema, with separate read models.**
   - The Query context does not reuse write-side aggregates. It has its own DTOs (`TraceSummaryDto` and others, see 03) and the masking policy.
7. **Retention: Separate Ways with writers, Customer of the full tier for compaction.**
   - Retention removes whole partitions on a schedule and never coordinates with writers per row.
   - The only coordination with writers is that they must never need a partition that is older than retention (n days). Spans older than retention are dropped by the ingester (UC-01, alternative flow).
   - Compaction consumes the full tier as a **published table set**, by time: a trace day D is compacted once D + 1 day ≤ now − n days, when ingestion and correlation no longer touch it. Late values outside the read window `[D − 1 h, D + 1 day + 6 h)` are not captured (accepted, documented).
   - The digest tables are a published read model for the Query context. Query never writes them; Retention never reads Query.

---

## 3. Shared-kernel value objects

All value objects are Java 25 `record`s whose compact constructors validate invariants. They are immutable, have no identity beyond their value, and are safe to share across threads (`tc-ingester` runs listener threads, and `tc-query-api` uses virtual threads).

### 3.1 TraceId

Invariants:

- 32 lowercase hex characters (16 bytes);
- not all zeros (W3C Trace Context: an all-zero trace-id is invalid);
- maps losslessly to a PostgreSQL `uuid` (ADR-10).

```java
package ru.aasmc.tracing.collector.domain.trace;

import java.util.HexFormat;
import java.util.Locale;
import java.util.UUID;

/** 128-bit trace id. Stored as PostgreSQL uuid (16 bytes); rendered as 32 lowercase hex. */
public record TraceId(long high, long low) implements Comparable<TraceId> {

    private static final HexFormat HEX = HexFormat.of(); // lowercase

    public TraceId {
        if (high == 0L && low == 0L) {
            throw new IllegalArgumentException("TraceId must not be all zeros");
        }
    }

    /** Accepts upper/lower case on API input; the wire (starter) always sends lowercase. */
    public static TraceId parse(String hex) {
        if (hex == null || hex.length() != 32) {
            throw new IllegalArgumentException("TraceId must be 32 hex chars: " + hex);
        }
        String s = hex.toLowerCase(Locale.ROOT);
        for (int i = 0; i < 32; i++) {
            if (!HexFormat.isHexDigit(s.charAt(i))) {
                throw new IllegalArgumentException("TraceId contains non-hex char: " + hex);
            }
        }
        return new TraceId(HexFormat.fromHexDigitsToLong(s, 0, 16), HexFormat.fromHexDigitsToLong(s, 16, 32));
    }

    public static TraceId fromUuid(UUID uuid) {
        return new TraceId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    /** Raw 128 bits in a uuid column; this is NOT an RFC 9562 versioned UUID, PostgreSQL does not care. */
    public UUID toUuid() {
        return new UUID(high, low);
    }

    public String toHex() {
        return HEX.toHexDigits(high) + HEX.toHexDigits(low);
    }

    @Override
    public int compareTo(TraceId o) {
        int c = Long.compareUnsigned(high, o.high);
        return c != 0 ? c : Long.compareUnsigned(low, o.low);
    }

    @Override
    public String toString() {
        return toHex();
    }
}
```

`toUuid()` and `fromUuid()` are exact inverses. The ordering used by `compareTo` (unsigned byte order) is the same as PostgreSQL's `uuid` ordering, so keyset cursors built in Java agree with `ORDER BY trace_id`.

### 3.2 SpanId

Invariants:

- 16 hex characters (8 bytes);
- not all zeros;
- maps to `bigint` by reinterpreting the 64 bits as two's-complement, so ids ≥ `8000000000000000` become negative longs. The mapping is lossless.

The all-zero parent id on the wire means "no parent". That is handled by `parseParent`, not by the constructor.

```java
package ru.aasmc.tracing.collector.domain.trace;

import java.util.HexFormat;
import java.util.Locale;
import java.util.Optional;

public record SpanId(long value) {

    private static final HexFormat HEX = HexFormat.of();
    public static final String INVALID_HEX = "0000000000000000";

    public SpanId {
        if (value == 0L) {
            throw new IllegalArgumentException("SpanId must not be all zeros");
        }
    }

    public static SpanId parse(String hex) {
        if (hex == null || hex.length() != 16) {
            throw new IllegalArgumentException("SpanId must be 16 hex chars: " + hex);
        }
        String s = hex.toLowerCase(Locale.ROOT);
        for (int i = 0; i < 16; i++) {
            if (!HexFormat.isHexDigit(s.charAt(i))) {
                throw new IllegalArgumentException("SpanId contains non-hex char: " + hex);
            }
        }
        return new SpanId(HexFormat.fromHexDigitsToLong(s)); // unsigned 64 bits -> signed long, lossless
    }

    /** Root spans carry "0000000000000000" (OTel SpanData.getParentSpanId()); null/empty is tolerated. */
    public static Optional<SpanId> parseParent(String hex) {
        if (hex == null || hex.isEmpty() || INVALID_HEX.equals(hex)) {
            return Optional.empty();
        }
        return Optional.of(parse(hex));
    }

    public String toHex() {
        return HEX.toHexDigits(value);
    }

    @Override
    public String toString() {
        return toHex();
    }
}
```

### 3.3 TimeRange and TraceDay

```java
package ru.aasmc.tracing.collector.domain.trace;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

/** Half-open interval [from, to). */
public record TimeRange(Instant from, Instant to) {
    public TimeRange {
        if (from == null || to == null) throw new IllegalArgumentException("bounds required");
        if (to.isBefore(from)) throw new IllegalArgumentException("to < from");
    }
    public Duration length() { return Duration.between(from, to); }
    public boolean contains(Instant t) { return !t.isBefore(from) && t.isBefore(to); }
    public TimeRange requireMaxLength(Duration max) {
        if (length().compareTo(max) > 0) throw new IllegalArgumentException("range exceeds " + max);
        return this;
    }
}

/** UTC calendar day under which a trace's summary is filed (trace_summary partition key). */
public record TraceDay(LocalDate value) {
    public TraceDay {
        if (value == null) throw new IllegalArgumentException("day required");
    }
    public static TraceDay ofEpochNanos(long epochNanos) {
        return new TraceDay(LocalDate.ofInstant(EpochNanos.toInstant(epochNanos), ZoneOffset.UTC));
    }
}

/** Helpers: wire times are epoch nanos (as strings); PostgreSQL timestamptz has microsecond precision. */
public final class EpochNanos {
    private EpochNanos() {}
    public static long parse(String s) { return Long.parseLong(s); }           // never via double
    public static Instant toInstant(long nanos) {
        return Instant.ofEpochSecond(Math.floorDiv(nanos, 1_000_000_000L), Math.floorMod(nanos, 1_000_000_000L));
    }
    /** Value written to timestamptz: truncated (not rounded) to microseconds, per ADR-10. */
    public static Instant toMicros(long nanos) {
        return toInstant(nanos - Math.floorMod(nanos, 1_000L));
    }
}
```

**Trace Day rule.** `TraceDayResolver` in `ingest.trace` uses this order:

1. The Caffeine cache `traceId -> trace_day`.
2. On a miss, `SELECT trace_day FROM tc.trace_summary WHERE trace_id = ? AND trace_day IN (d-1, d, d+1)` where d = UTC date of the fragment's min start.
3. Otherwise `TraceDay.ofEpochNanos(min start of the fragment's spans)`.

Fragments of one trace are serialized on one partition, and therefore on one consumer thread, so two instances race only during a rebalance.

A fragment that arrives more than a day after a trace was first filed, or a race during a rebalance, can produce a **second summary row for the same trace under a different trace_day**. The model accepts this. The PK is `(trace_id, trace_day)`, and the Query context merges the rows with the same `TraceSummary.merge` used on the write path (§4.4). Merge is commutative and associative, so the merged result is identical.

### 3.4 Attributes

```java
package ru.aasmc.tracing.collector.domain.attribute;

import java.util.regex.Pattern;

/** OTel attribute key. Interned via attribute_key dictionary (attribute_key.id). */
public record AttributeKey(String value) implements Comparable<AttributeKey> {
    public static final int MAX_LENGTH = 255;
    private static final Pattern FORBIDDEN = Pattern.compile("[\\p{Cntrl}]");

    public AttributeKey {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("blank key");
        if (value.length() > MAX_LENGTH) throw new IllegalArgumentException("key too long: " + value.length());
        if (FORBIDDEN.matcher(value).find()) throw new IllegalArgumentException("control chars in key");
    }
    public static final AttributeKey SERVICE_NAME = new AttributeKey("service.name");
    public static final AttributeKey SPAN_NAME = new AttributeKey("span.name");
    public static final AttributeKey STATUS = new AttributeKey("status");
    public boolean isReservedForTcs() { return value.startsWith("tc."); }
    @Override public int compareTo(AttributeKey o) { return value.compareTo(o.value); }
}
```

```java
package ru.aasmc.tracing.collector.domain.attribute;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalDouble;

public enum AttributeType { STRING, BOOLEAN, LONG, DOUBLE, STRING_ARRAY, BOOLEAN_ARRAY, LONG_ARRAY, DOUBLE_ARRAY }

/** Typed OTel attribute value. Exactly the eight OTel AttributeType names used by the starter. */
public sealed interface AttributeValue
        permits AttributeValue.StringValue, AttributeValue.BoolValue, AttributeValue.LongValue,
                AttributeValue.DoubleValue, AttributeValue.StringArrayValue, AttributeValue.BoolArrayValue,
                AttributeValue.LongArrayValue, AttributeValue.DoubleArrayValue {

    int MAX_STRING_BYTES = 4 * 1024; // Tempo-like limit; truncate + flag

    AttributeType type();

    /** Tokens written to trace_attribute.value_str (one per array element). */
    List<String> indexTokens();

    /** Value for trace_attribute.value_num when the key is registered as numeric. */
    default OptionalDouble numeric() { return OptionalDouble.empty(); }

    default boolean truncated() { return false; }

    record StringValue(String value, boolean truncated) implements AttributeValue {
        public StringValue {
            if (value == null) throw new IllegalArgumentException("null string");
            if (value.getBytes(StandardCharsets.UTF_8).length > MAX_STRING_BYTES)
                throw new IllegalArgumentException("use StringValue.of() to truncate");
        }
        /** Truncates to 4 KiB of UTF-8 without splitting a surrogate pair. */
        public static StringValue of(String raw) {
            byte[] b = raw.getBytes(StandardCharsets.UTF_8);
            if (b.length <= MAX_STRING_BYTES) return new StringValue(raw, false);
            int end = raw.length();
            while (raw.substring(0, end).getBytes(StandardCharsets.UTF_8).length > MAX_STRING_BYTES) {
                end = Math.max(0, end - Math.max(1, (b.length - MAX_STRING_BYTES) / 4));
                if (end > 0 && Character.isHighSurrogate(raw.charAt(end - 1))) end--;
            }
            return new StringValue(raw.substring(0, end), true);
        }
        public AttributeType type() { return AttributeType.STRING; }
        public List<String> indexTokens() { return List.of(value); }
    }

    record BoolValue(boolean value) implements AttributeValue {
        public AttributeType type() { return AttributeType.BOOLEAN; }
        public List<String> indexTokens() { return List.of(Boolean.toString(value)); }
    }

    /** LONG is parsed from the JSON number as long (never via double: values above 2^53 must survive). */
    record LongValue(long value) implements AttributeValue {
        public AttributeType type() { return AttributeType.LONG; }
        public List<String> indexTokens() { return List.of(Long.toString(value)); }
        @Override public OptionalDouble numeric() { return OptionalDouble.of(value); }
    }

    record DoubleValue(double value) implements AttributeValue {
        public DoubleValue {
            if (Double.isNaN(value)) throw new IllegalArgumentException("NaN not indexable");
        }
        public AttributeType type() { return AttributeType.DOUBLE; }
        public List<String> indexTokens() { return List.of(Double.toString(value)); }
        @Override public OptionalDouble numeric() { return OptionalDouble.of(value); }
    }

    record StringArrayValue(List<String> values) implements AttributeValue {
        public StringArrayValue { values = List.copyOf(values); }
        public AttributeType type() { return AttributeType.STRING_ARRAY; }
        public List<String> indexTokens() { return values; }
    }

    record BoolArrayValue(List<Boolean> values) implements AttributeValue {
        public BoolArrayValue { values = List.copyOf(values); }
        public AttributeType type() { return AttributeType.BOOLEAN_ARRAY; }
        public List<String> indexTokens() { return values.stream().map(String::valueOf).toList(); }
    }

    record LongArrayValue(List<Long> values) implements AttributeValue {
        public LongArrayValue { values = List.copyOf(values); }
        public AttributeType type() { return AttributeType.LONG_ARRAY; }
        public List<String> indexTokens() { return values.stream().map(String::valueOf).toList(); }
    }

    record DoubleArrayValue(List<Double> values) implements AttributeValue {
        public DoubleArrayValue { values = List.copyOf(values); }
        public AttributeType type() { return AttributeType.DOUBLE_ARRAY; }
        public List<String> indexTokens() { return values.stream().map(String::valueOf).toList(); }
    }
}
```

```java
package ru.aasmc.tracing.collector.domain.attribute;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.SequencedMap;

/** Ordered, immutable attribute map (producer order preserved for display). */
public record Attributes(SequencedMap<AttributeKey, AttributeValue> entries) {
    public static final int MAX_ENTRIES = 512;
    public static final Attributes EMPTY = new Attributes(new LinkedHashMap<>());

    public Attributes {
        if (entries.size() > MAX_ENTRIES) throw new IllegalArgumentException("too many attributes");
        entries = Collections.unmodifiableSequencedMap(new LinkedHashMap<>(entries));
    }
    public boolean anyTruncated() { return entries.values().stream().anyMatch(AttributeValue::truncated); }
    public java.util.Optional<AttributeValue> get(AttributeKey k) { return java.util.Optional.ofNullable(entries.get(k)); }
}

public enum AttributeOrigin {
    SPAN((short) 1), RESOURCE((short) 2), ENRICHMENT((short) 3), DERIVED((short) 4);
    public final short code;
    AttributeOrigin(short code) { this.code = code; }
}
```

**Storage form of attributes.** `span.attributes` is compact jsonb that uses JSON-native values, for example `{"user_id":"u-42","http.response.status_code":200,"ratio":0.5,"tags":["a","b"]}`. It does not use the wire's `{type,value}` envelope.

- The type can be recovered. jsonb keeps `numeric` scale, so `1` (LONG) and `1.0` (DOUBLE) stay distinct. Strings, booleans and arrays are self-describing.
- This halves attribute bytes compared with the wire format. That matters for the ~350 B/span estimate in 04.
- Empty arrays lose their element type. This is accepted and rendered as `[]`.

### 3.5 Resource, InstrumentationScope, dictionaries

```java
package ru.aasmc.tracing.collector.domain.resource;

import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue;
import ru.aasmc.tracing.collector.domain.attribute.Attributes;

/** Content-hash identity: xxHash64 over the canonical encoding (ADR-06). */
public record ResourceId(long value) {}
public record ScopeId(long value) {}
public record ServiceId(int value) { public ServiceId { if (value <= 0) throw new IllegalArgumentException(); } }
public record SpanNameId(int value) { public SpanNameId { if (value <= 0) throw new IllegalArgumentException(); } }

public record Resource(ResourceId id, String serviceName, String schemaUrl, Attributes attributes) {
    public static final String UNKNOWN_SERVICE = "unknown_service";

    public Resource {
        if (serviceName == null || serviceName.isBlank()) throw new IllegalArgumentException("serviceName");
        if (!id.equals(ContentHash.resourceId(schemaUrl, attributes)))
            throw new IllegalArgumentException("ResourceId does not match content");
    }

    public static Resource of(String schemaUrl, Attributes attributes) {
        String svc = attributes.get(AttributeKey.SERVICE_NAME)
                .filter(v -> v instanceof AttributeValue.StringValue)
                .map(v -> ((AttributeValue.StringValue) v).value())
                .filter(s -> !s.isBlank())
                .orElse(UNKNOWN_SERVICE);
        return new Resource(ContentHash.resourceId(schemaUrl, attributes), svc, schemaUrl, attributes);
    }
}

public record InstrumentationScope(ScopeId id, String name, String version, String schemaUrl, Attributes attributes) {
    public InstrumentationScope {
        if (name == null) throw new IllegalArgumentException("scope name");
        if (!id.equals(ContentHash.scopeId(name, version, schemaUrl, attributes)))
            throw new IllegalArgumentException("ScopeId does not match content");
    }
}
```

**Canonical encoding.** `ContentHash` encodes the content as:

1. `schemaUrl` (empty string if null), then `'\n'`;
2. the attributes **sorted by key**, each written as `key '\u0001' typeName '\u0001' canonicalValue '\n'`.

That encoding is hashed with xxHash64, seed 0. The encoding is part of the domain contract. Changing it does not corrupt data, but it creates new `resource` rows for the same content. It would therefore be versioned (seed = encoding version) and announced.

A collision between two different resources (probability ≈ n²/2⁶⁵, about 3·10⁻⁸ for 1 M distinct resources) would be accepted silently. The span would point to the first resource seen. This is documented as an accepted risk.

**Why a content hash.** Every service instance can compute the id locally without a DB round-trip. `ResourceDeduplicator` keeps a Caffeine `ResourceId -> known` set. Only unseen ids go into the batch's `INSERT ... ON CONFLICT DO NOTHING`. `last_seen` is refreshed at most once per hour per resource, so the table never becomes a hot spot.

### 3.6 Span

```java
package ru.aasmc.tracing.collector.domain.trace;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import ru.aasmc.tracing.collector.domain.attribute.Attributes;
import ru.aasmc.tracing.collector.domain.resource.ResourceId;
import ru.aasmc.tracing.collector.domain.resource.ScopeId;

public enum SpanKind {
    INTERNAL(1), SERVER(2), CLIENT(3), PRODUCER(4), CONSUMER(5); // OTLP numeric values
    public final short code;
    SpanKind(int c) { this.code = (short) c; }
    public static SpanKind fromWire(String name) { return valueOf(name); } // "SERVER" etc.
}

public enum StatusCode {
    UNSET(0), OK(1), ERROR(2);
    public final short code;
    StatusCode(int c) { this.code = (short) c; }
}

public record SpanStatus(StatusCode code, String message) {
    public SpanStatus {
        if (code == null) throw new IllegalArgumentException("status code");
        if (message != null && message.isEmpty()) message = null; // starter sends "" -> NULL
    }
    public boolean isError() { return code == StatusCode.ERROR; }
}

/** W3C trace-flags; wire sends a signed Java byte, stored as smallint 0..255. */
public record TraceFlags(int value) {
    public TraceFlags {
        if (value < 0 || value > 255) throw new IllegalArgumentException("flags out of range");
    }
    public static TraceFlags fromWireByte(byte b) { return new TraceFlags(b & 0xFF); }
    public boolean sampled() { return (value & 0x01) != 0; }
}

/** Bits of span.flags (smallint). */
public enum SpanFlag {
    ATTRIBUTE_TRUNCATED(0x01), NOT_ENDED(0x02), PARENT_REMOTE(0x04), EVENTS_OR_LINKS_TRUNCATED(0x08),
    NEGATIVE_DURATION_CLAMPED(0x10), INDEX_VALUE_TRUNCATED(0x20);   // canonical table: 04 §5.5
    public final int bit;
    SpanFlag(int bit) { this.bit = bit; }
    public static short toBits(Set<SpanFlag> flags) {
        int b = 0; for (SpanFlag f : flags) b |= f.bit; return (short) b;
    }
}

public record DroppedCounts(int attributes, int events, int links) {
    public DroppedCounts {
        if (attributes < 0 || events < 0 || links < 0) throw new IllegalArgumentException("negative dropped count");
    }
    public boolean any() { return attributes > 0 || events > 0 || links > 0; } // else dropped_counts = NULL
}

public record SpanEvent(String name, long offsetNanos, Attributes attributes, int droppedAttributes) {}

public record SpanLinkRef(TraceId linkedTraceId, SpanId linkedSpanId, String traceState, Attributes attributes) {}

/** Immutable span. Never updated after insert (fillfactor 100). */
public record Span(
        TraceId traceId,
        SpanId spanId,
        SpanId parentSpanId,          // null for root spans
        String name,
        SpanKind kind,
        long startEpochNanos,
        long durationNanos,
        SpanStatus status,
        ResourceId resourceId,
        String serviceName,           // denormalised from Resource for dictionary lookup
        ScopeId scopeId,
        TraceFlags traceFlags,
        String traceState,            // W3C serialisation "k1=v1,k2=v2" or null
        Attributes attributes,
        List<SpanEvent> events,
        List<SpanLinkRef> links,
        DroppedCounts dropped,
        Set<SpanFlag> flags) {

    public Span {
        if (traceId == null || spanId == null) throw new IllegalArgumentException("ids required");
        if (spanId.equals(parentSpanId)) throw new IllegalArgumentException("span cannot be its own parent");
        if (durationNanos < 0) throw new IllegalArgumentException("normalizer must clamp negative duration");
        if (name == null) name = "";
        events = List.copyOf(events);
        links = List.copyOf(links);
        flags = flags.isEmpty() ? Set.of() : Set.copyOf(EnumSet.copyOf(flags));
        if (attributes.anyTruncated() && !flags.contains(SpanFlag.ATTRIBUTE_TRUNCATED))
            throw new IllegalArgumentException("truncated attributes must be flagged");
    }

    public boolean isRoot() { return parentSpanId == null; }
    public long endEpochNanos() { return startEpochNanos + durationNanos; }
    public TraceDay startDay() { return TraceDay.ofEpochNanos(startEpochNanos); }
}
```

Invariants enforced by `SpanNormalizer` before construction:

1. `spanContext.traceId == message.traceId`. Otherwise the record is poison, reason `TRACE_ID_MISMATCH`.
2. `spanId == spanContext.spanId`.
3. `end < start` gives duration 0 and `NEGATIVE_DURATION_CLAMPED`.
4. `start > now + 1 day` makes the record poison, reason `FUTURE_TIMESTAMP`.
5. `start < now - retention(span)` drops the span, increments the metric `tc_ingester_spans_dropped_total{reason="EXPIRED"}`, and lets the rest of the fragment proceed.
6. String values longer than 4 KiB are truncated and flagged.
7. A trace already holding 50 000 spans drops further spans with metric reason `TRACE_TOO_LARGE`. The count comes from `trace_summary.span_count` plus the current batch.

### 3.7 TraceFragment

```java
package ru.aasmc.tracing.collector.domain.trace;

import java.time.Instant;
import java.util.List;

/** Kafka coordinates of the record a fragment/enrichment came from (idempotency + DLQ). */
public record KafkaCoordinates(String topic, int partition, long offset, Instant timestamp) {
    public KafkaCoordinates {
        if (partition < 0 || offset < 0) throw new IllegalArgumentException("bad coordinates");
    }
}

/** One TraceMessage after parsing + normalisation; transient (lives for one poll batch). */
public record TraceFragment(TraceId traceId, List<Span> spans, KafkaCoordinates source, int encodedBytes) {
    public static final int MAX_BYTES = 8 * 1024 * 1024;

    public TraceFragment {
        spans = List.copyOf(spans);
        if (encodedBytes > MAX_BYTES) throw new IllegalArgumentException("FRAGMENT_TOO_LARGE");
        for (Span s : spans) {
            if (!s.traceId().equals(traceId)) throw new IllegalArgumentException("TRACE_ID_MISMATCH");
        }
        // empty fragments are legal after EXPIRED filtering; they produce no rows but still ack.
    }

    public long minStartNanos() { return spans.stream().mapToLong(Span::startEpochNanos).min().orElseThrow(); }
    public long maxEndNanos() { return spans.stream().mapToLong(Span::endEpochNanos).max().orElseThrow(); }
}
```

---

## 4. Aggregates

### 4.1 Aggregate overview

| Aggregate (root) | Context | Consistency boundary | Persistence | Identity |
|---|---|---|---|---|
| **Span** | Ingestion | single immutable row | `span` (+ `span_link`) | `(trace_id, span_id, start_time)` |
| **TraceSummary** | Ingestion | one row per `(trace_id, trace_day)`, updated by commutative merge in the batch transaction | `trace_summary` | `(trace_id, trace_day)` |
| **Resource / InstrumentationScope** | Ingestion | content-addressed, insert-only (plus `last_seen`) | `resource`, `instrumentation_scope` | xxHash64 |
| **KafkaConnection** | Configuration | one row | `kafka_connection` | `id`, unique `name` |
| **EnrichmentSource** (+ SourceRevision entities) | Configuration (definition); Enrichment (runtime) | source row + its revisions; lifecycle transitions are atomic with a `config_revision` bump | `enrichment_source`, `enrichment_source_revision` | `id`, unique `name` |
| **EnrichmentRecord** | Enrichment | single immutable row | `trace_enrichment` | `(source_id, kafka_partition, kafka_offset, key_id, observed_at)` |
| **IndexedAttribute** | Configuration / Retention (backfill) | one row | `indexed_attribute` | `key_id` |
| **CorrelationType** (+ CorrelationKey entities) | Configuration | type + keys | `correlation_type`, `correlation_key` | `id`, unique `name` |
| **BusinessOperation** | Correlation | operation + the `correlation_value` rows pointing at it; modified only by the single-leader resolver | `business_operation`, `correlation_value` | `id` uuidv7 (+ `first_seen` for partitioning) |
| **RetentionPolicy** | Configuration / Retention | one row per table; durations changed per tier (`RetentionTiers`) in one transaction | `retention_policy` | `table_name` (+ `tier`) |
| **TraceDigest** | Retention (compaction) | single immutable row, written once by `INSERT ... ON CONFLICT DO NOTHING` | `trace_digest` | `(trace_id, trace_day)` |
| **BusinessOperationDigest** | Retention (compaction) | single immutable row per root operation, written once after all member trace days are compacted | `business_operation_digest` | `(operation_id, op_day)` |
| **CompactionState** | Retention (compaction) | one row per trace day; modified only by the single-leader `DigestCompactor` | `compaction_state` | `trace_day` |
| **CompactionExclusion** | Configuration | one row | `compaction_exclusion` | `id` |
| **ConfigRevision** | Configuration | singleton | `config_revision` | `id = 1` |

The **Trace** is deliberately *not* an aggregate. At 20 000–60 000 spans/s, with fragments spread over minutes and services, a transactional "trace" boundary would need locking or read-modify-write on hot rows. TCS keeps the consistency units small:

- spans are insert-only;
- the summary is merged commutatively;
- enrichment, attribute index, correlation and links are append-only side tables.

The trace exists only as a **read model**: `TraceView`, assembled by `tc-query-api`.

### 4.2 Class diagram: trace model

```mermaid
classDiagram
    direction LR
    class TraceId {
        +long high
        +long low
        +parse(String) TraceId
        +toUuid() UUID
        +toHex() String
    }
    class SpanId {
        +long value
        +parse(String) SpanId
        +parseParent(String) Optional~SpanId~
    }
    class TraceFragment {
        +TraceId traceId
        +List~Span~ spans
        +KafkaCoordinates source
        +int encodedBytes
        +minStartNanos() long
    }
    class Span {
        +TraceId traceId
        +SpanId spanId
        +SpanId parentSpanId
        +String name
        +SpanKind kind
        +long startEpochNanos
        +long durationNanos
        +SpanStatus status
        +ResourceId resourceId
        +ScopeId scopeId
        +TraceFlags traceFlags
        +Attributes attributes
        +isRoot() boolean
    }
    class TraceSummary {
        +TraceId traceId
        +TraceDay traceDay
        +long startEpochNanos
        +long endEpochNanos
        +RootInfo root
        +int spanCount
        +int errorCount
        +SortedSet~Integer~ serviceIds
        +int fragmentCount
        +merge(TraceSummary) TraceSummary
    }
    class Resource {
        +ResourceId id
        +String serviceName
        +String schemaUrl
        +Attributes attributes
    }
    class InstrumentationScope {
        +ScopeId id
        +String name
        +String version
    }
    class Attributes {
        +SequencedMap entries
    }
    class AttributeValue {
        <<sealed>>
        +type() AttributeType
        +indexTokens() List~String~
    }
    class SpanEvent
    class SpanLinkRef {
        +TraceId linkedTraceId
        +SpanId linkedSpanId
    }
    class EnrichmentRecord {
        +TraceId traceId
        +SpanId spanId
        +long sourceId
        +int sourceRevision
        +AttributeKey key
        +AttributeValue value
        +KafkaCoordinates source
    }
    class CorrelationObservation {
        +int correlationTypeId
        +int correlationKeyId
        +String value
        +TraceId traceId
        +AttributeOrigin origin
    }
    TraceFragment "1" *-- "1..*" Span
    Span --> TraceId
    Span --> SpanId
    Span "*" --> "1" Resource : resourceId
    Span "*" --> "1" InstrumentationScope : scopeId
    Span *-- Attributes
    Span *-- "0..*" SpanEvent
    Span *-- "0..*" SpanLinkRef
    Attributes o-- "0..*" AttributeValue
    TraceSummary ..> Span : built from inserted
    EnrichmentRecord ..> TraceId : joins at read time
    CorrelationObservation ..> TraceId
```

### 4.3 Class diagram: configuration, enrichment and correlation

```mermaid
classDiagram
    direction LR
    class KafkaConnection {
        +KafkaConnectionId id
        +String name
        +String bootstrapServers
        +SecurityProtocol securityProtocol
        +SaslMechanism saslMechanism
        +SecretRef credentials
        +SecretRef truststore
        +Map clientProperties
    }
    class EnrichmentSource {
        +EnrichmentSourceId id
        +String name
        +KafkaConnectionId connectionId
        +String topic
        +SourceStatus status
        +int currentRevision
        +activate() EnrichmentSource
        +pause() EnrichmentSource
        +delete() EnrichmentSource
        +revise(SourceDefinition) Revised
        +consumerGroupId() String
    }
    class SourceRevision {
        +int revision
        +SourceDefinition definition
        +String createdBy
    }
    class SourceDefinition {
        +PayloadFormat payloadFormat
        +TraceContextExtraction traceContext
        +EnrichmentScope scope
        +EventFilter filter
        +StartFrom startFrom
        +OnMissingTraceContext onMissingTraceContext
        +List~FieldMapping~ fieldMappings
    }
    class TraceContextExtraction {
        <<sealed>>
    }
    class TraceparentHeader
    class RawHeader
    class PayloadPointer
    class FieldMapping {
        +FieldSource source
        +String jsonPointer
        +String headerName
        +AttributeKey targetKey
        +AttributeType valueType
        +Transform transform
        +boolean required
    }
    class Transform {
        <<sealed>>
    }
    class IndexedAttribute {
        +int keyId
        +OriginScope originScope
        +boolean numeric
        +IndexStatus status
        +boolean retainInDigest
        +DigestTransform digestTransform
    }
    class DigestTransform {
        <<enumeration>>
        NONE
        SHA256
        MASK
        +apply(String) String
    }
    class CorrelationType {
        +int id
        +String name
        +Duration mergeWindow
        +List~CorrelationKey~ keys
        +OutcomeRule outcomeRule
    }
    class OutcomeRule {
        +AttributeKey outcomeKey
        +Set~String~ successValues
        +Set~String~ failureValues
        +String finalStepLabel
        +evaluate(List~MemberTrace~) Evaluation
    }
    class CorrelationKey {
        +int id
        +int keyId
        +KeyRole role
        +String stepLabel
    }
    class BusinessOperation {
        +UUID id
        +Instant firstSeen
        +Instant lastSeen
        +String displayValue
        +int traceCount
        +UUID mergedInto
    }
    class CorrelationValue {
        +int correlationTypeId
        +String value
        +UUID operationId
    }
    class RetentionPolicy {
        +String tableName
        +RetentionTier tier
        +Duration retention
        +Granularity granularity
        +Duration precreate
    }
    class RetentionTiers {
        +int fullDays
        +int digestDays
        +digestDropAge() Duration
    }
    EnrichmentSource "*" --> "1" KafkaConnection
    EnrichmentSource "1" *-- "1..*" SourceRevision
    SourceRevision *-- SourceDefinition
    SourceDefinition *-- TraceContextExtraction
    SourceDefinition *-- "1..64" FieldMapping
    FieldMapping *-- Transform
    TraceContextExtraction <|.. TraceparentHeader
    TraceContextExtraction <|.. RawHeader
    TraceContextExtraction <|.. PayloadPointer
    CorrelationType "1" *-- "1..*" CorrelationKey
    CorrelationType "1" *-- "0..1" OutcomeRule
    IndexedAttribute --> DigestTransform
    RetentionTiers ..> RetentionPolicy : sets retention of every row of a tier
    CorrelationValue "*" --> "1" BusinessOperation
    BusinessOperation --> BusinessOperation : mergedInto
```

### 4.4 TraceSummary: commutative, idempotent merge

`TraceSummary` is a **commutative monoid over the set of inserted spans**:

- `ofSpans(S1).merge(ofSpans(S2)) == ofSpans(S1 ∪ S2)` whenever S1 ∩ S2 = ∅;
- merge is associative and commutative, so fragment arrival order does not matter.

Disjointness is guaranteed by the span primary key. The writable CTE in `SpanBatchWriter` feeds the summary upsert **only with the rows `RETURNING`'d by `INSERT ... ON CONFLICT DO NOTHING`**, so a replayed fragment contributes the empty set (the identity element). That is why the counters stay correct under at-least-once delivery (UC-02).

```java
package ru.aasmc.tracing.collector.domain.trace;

import java.time.Instant;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

public record RootInfo(SpanId spanId, int serviceId, Integer nameId, long startEpochNanos) {}

public record TraceSummary(
        TraceId traceId,
        TraceDay traceDay,
        long startEpochNanos,      // min(span.start)
        long endEpochNanos,        // max(span.start + duration)
        RootInfo root,             // null while rootless
        int spanCount,
        int errorCount,
        SortedSet<Integer> serviceIds,
        int fragmentCount,
        Instant firstIngestedAt,
        Instant lastIngestedAt) {

    public TraceSummary {
        if (spanCount < 1) throw new IllegalArgumentException("summary needs >= 1 span");
        if (errorCount < 0 || errorCount > spanCount) throw new IllegalArgumentException("errorCount");
        if (endEpochNanos < startEpochNanos) throw new IllegalArgumentException("end < start");
        if (fragmentCount < 1) throw new IllegalArgumentException("fragmentCount");
        if (lastIngestedAt.isBefore(firstIngestedAt)) throw new IllegalArgumentException("ingest times");
        serviceIds = java.util.Collections.unmodifiableSortedSet(new TreeSet<>(serviceIds));
    }

    public long durationNanos() { return endEpochNanos - startEpochNanos; }
    public boolean rootless() { return root == null; }

    /** Summary of spans that were actually inserted from ONE fragment (fragmentCount = 1). */
    public static TraceSummary ofInsertedSpans(TraceDay day, Collection<Span> inserted,
                                               java.util.function.ToIntFunction<Span> serviceIdOf,
                                               java.util.function.Function<Span, Integer> nameIdOf,
                                               Instant now) {
        Span first = inserted.iterator().next();
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE; int errors = 0; RootInfo root = null;
        SortedSet<Integer> services = new TreeSet<>();
        for (Span s : inserted) {
            min = Math.min(min, s.startEpochNanos());
            max = Math.max(max, s.endEpochNanos());
            if (s.status().isError()) errors++;
            services.add(serviceIdOf.applyAsInt(s));
            if (s.isRoot()) {
                RootInfo candidate = new RootInfo(s.spanId(), serviceIdOf.applyAsInt(s), nameIdOf.apply(s), s.startEpochNanos());
                root = pickRoot(root, candidate);
            }
        }
        return new TraceSummary(first.traceId(), day, min, max, root, inserted.size(), errors,
                services, 1, now, now);
    }

    /** Commutative + associative. Mirrors the SQL ON CONFLICT DO UPDATE of trace_summary exactly. */
    public TraceSummary merge(TraceSummary o) {
        if (!traceId.equals(o.traceId)) throw new IllegalArgumentException("different traces");
        SortedSet<Integer> services = new TreeSet<>(serviceIds);
        services.addAll(o.serviceIds);
        return new TraceSummary(
                traceId,
                traceDay.value().isBefore(o.traceDay.value()) ? traceDay : o.traceDay, // read-side merge of split rows
                Math.min(startEpochNanos, o.startEpochNanos),
                Math.max(endEpochNanos, o.endEpochNanos),
                pickRoot(root, o.root),
                spanCount + o.spanCount,
                errorCount + o.errorCount,
                services,
                fragmentCount + o.fragmentCount,
                firstIngestedAt.isBefore(o.firstIngestedAt) ? firstIngestedAt : o.firstIngestedAt,
                lastIngestedAt.isAfter(o.lastIngestedAt) ? lastIngestedAt : o.lastIngestedAt);
    }

    /** Deterministic choice if a (malformed) trace has several roots: earliest start, then smallest span id. */
    private static RootInfo pickRoot(RootInfo a, RootInfo b) {
        if (a == null) return b;
        if (b == null) return a;
        int c = Long.compare(a.startEpochNanos(), b.startEpochNanos());
        if (c != 0) return c < 0 ? a : b;
        return Long.compareUnsigned(a.spanId().value(), b.spanId().value()) <= 0 ? a : b;
    }
}
```

Equivalent SQL semantics for the upsert in the writable CTE (full statement in 03/04):

| Column | On insert | On conflict `(trace_id, trace_day)` |
|---|---|---|
| `start_time` | `min(start_time)` of inserted spans | `LEAST(t.start_time, EXCLUDED.start_time)` |
| `end_time` | `max(start_time + duration)` | `GREATEST(...)` |
| `duration_ns` | `end - start` in ns | recomputed from the merged start and end (µs precision at trace level; exact per span) |
| `root_span_id`, `root_service_id`, `root_name_id` | from an inserted root span, if any | keep existing unless NULL or the new root starts earlier |
| `span_count`, `error_count` | count of inserted spans / of errors | `t.x + EXCLUDED.x` |
| `service_ids` | sorted distinct | sorted distinct union |
| `fragment_count` | number of distinct fragments in the batch that inserted ≥ 1 span (staging carries a batch-local `frag_no`) | `t.fragment_count + EXCLUDED.fragment_count` |
| `first_ingested_at` / `last_ingested_at` | `now()` | `LEAST` / `GREATEST` |

Each poll batch is grouped by `(trace_id, trace_day)` before the upsert. PostgreSQL's `ON CONFLICT DO UPDATE` cannot touch the same row twice in one statement.

`fragment_count` and `last_ingested_at` are the only fields that depend on *delivery* rather than *content*. They are diagnostic, and they are still replay-safe because a replayed fragment inserts zero spans and is not counted.

### 4.5 Trace completeness (derived)

```java
public enum CompletenessState { RECEIVING, SETTLED }

public record Completeness(CompletenessState state, boolean rootless, int orphanSpans) {
    /** settleAfter default PT2M (tc.query.trace-settle-after). Never persisted. */
    public static Completeness of(TraceSummary s, int orphanSpans, Instant now, java.time.Duration settleAfter) {
        var state = s.lastIngestedAt().plus(settleAfter).isBefore(now) ? CompletenessState.SETTLED : CompletenessState.RECEIVING;
        return new Completeness(state, s.rootless(), orphanSpans);
    }
}
```

```mermaid
stateDiagram-v2
    [*] --> RECEIVING : first fragment inserted (trace_summary row created)
    RECEIVING --> RECEIVING : another fragment inserted (last_ingested_at advances)
    RECEIVING --> SETTLED : now - last_ingested_at > settleAfter (default 2 min)
    SETTLED --> RECEIVING : late fragment inserted (e.g. replay, slow service, async consumer)
    SETTLED --> COMPACTED : DigestCompactor wrote trace_digest (trace_day D DONE, D + 1 day <= now - n days)
    SETTLED --> EXPIRED : root matches compaction_exclusion and no business operation (not digested)
    COMPACTED --> EXPIRED : drop gate passed, FULL-tier partitions dropped (n = 7 d)
    EXPIRED --> [*] : trace_digest partition dropped (age > n + m = 97 d)
    note right of COMPACTED
        Full detail and digest coexist until the
        drop gate opens. trace-by-id still serves
        the FULL view (tier FULL).
    end note
    note right of EXPIRED
        Spans gone. GET traces by id answers
        tier DIGEST, spansExpired true, from
        trace_digest only (UC-22).
    end note
    note right of RECEIVING
        Orthogonal flag ROOTLESS = root_span_id IS NULL.
        Cleared when the root span arrives.
        A SETTLED + ROOTLESS trace is shown with a warning
        (root lost by producer, or still in flight).
    end note
```

`COMPACTED` and `EXPIRED` are lifecycle states of the stored data, not of `Completeness` (which is computed only while the trace is in the full tier). They are derived from `compaction_state.status` of the trace day and from which tier answers a lookup; neither is stored per trace. A fragment that arrives after its trace day was compacted is not reflected in the digest (compaction is write-once). This is rare: spans older than n days are dropped at ingest (§3.6 rule 5), and the compaction read window adds 6 h of slack after the day.

TCS, like Tempo's block-builder, has **no notion of "trace finished"**. The producer never signals it (there is no fragment or sequence marker). Completeness is therefore a hint for the UI and for search consumers, never a gate:

- search results include RECEIVING traces;
- the UI labels them "still receiving".

### 4.6 KafkaConnection

```java
package ru.aasmc.tracing.collector.domain.enrichment;

import java.util.Map;
import java.util.Set;

public enum SecurityProtocol { PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL }
public enum SaslMechanism { PLAIN, SCRAM_SHA_256, SCRAM_SHA_512, OAUTHBEARER }

/** Reference to a secret held outside the DB (Vault path or k8s secret name/key). Never the secret itself. */
public record SecretRef(String uri) {
    public SecretRef {
        if (uri == null || !(uri.startsWith("vault:") || uri.startsWith("k8s:")))
            throw new IllegalArgumentException("secret ref must be vault:... or k8s:...");
    }
}

public record KafkaConnectionId(long value) {}

public record KafkaConnection(KafkaConnectionId id, String name, String bootstrapServers,
                              SecurityProtocol securityProtocol, SaslMechanism saslMechanism,
                              SecretRef credentials, SecretRef truststore, Map<String, String> clientProperties) {

    /** Properties TCS owns; a client may not override them. */
    static final Set<String> FORBIDDEN = Set.of(
            "group.id", "enable.auto.commit", "key.deserializer", "value.deserializer",
            "allow.auto.create.topics", "sasl.jaas.config", "ssl.keystore.password", "ssl.truststore.password",
            "bootstrap.servers", "security.protocol", "sasl.mechanism");

    public KafkaConnection {
        if (name == null || !name.matches("[a-z0-9][a-z0-9._-]{1,62}")) throw new IllegalArgumentException("name");
        if (bootstrapServers == null || bootstrapServers.isBlank()) throw new IllegalArgumentException("bootstrap");
        boolean sasl = securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL;
        if (sasl && (saslMechanism == null || credentials == null))
            throw new IllegalArgumentException("SASL requires mechanism and credentials ref");
        if (!sasl && saslMechanism != null) throw new IllegalArgumentException("mechanism without SASL");
        clientProperties = Map.copyOf(clientProperties);
        for (String k : clientProperties.keySet())
            if (FORBIDDEN.contains(k)) throw new IllegalArgumentException("property not allowed: " + k);
    }
}
```

`SecretResolver` (`enrich.kafka`) turns a `SecretRef` into JAAS config or truststore bytes at container start. Secrets therefore never appear in the DB, in API responses, or in `ConfigSnapshot.toString()`.

### 4.7 EnrichmentSource aggregate

#### 4.7.1 Definition value objects

```java
package ru.aasmc.tracing.collector.domain.enrichment;

import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.attribute.AttributeType;

public enum PayloadFormat { JSON }                       // PayloadDecoder SPI: AVRO, PROTOBUF later
public enum EnrichmentScope { TRACE, SPAN }
public enum StartFrom { LATEST, EARLIEST }               // only used when the group has no committed offset
public enum OnMissingTraceContext { DLQ, DROP }
public enum FieldSource { PAYLOAD, HEADER, RECORD_KEY, RECORD_TIMESTAMP }

/** RFC 6901 JSON Pointer: "" (whole doc) or "/a/b~1c". Evaluated with Jackson JsonNode.at(). */
public record JsonPointerExpr(String value) {
    private static final Pattern P = Pattern.compile("^(/([^~/]|~[01])*)*$");
    public JsonPointerExpr {
        if (value == null || !P.matcher(value).matches()) throw new IllegalArgumentException("invalid JSON Pointer: " + value);
    }
}

public sealed interface TraceContextExtraction
        permits TraceContextExtraction.TraceparentHeader, TraceContextExtraction.RawHeader,
                TraceContextExtraction.PayloadPointer {

    /** Default. W3C traceparent "00-<32hex>-<16hex parent span>-<2hex flags>"; yields trace id AND span id. */
    record TraceparentHeader(String headerName) implements TraceContextExtraction {
        public TraceparentHeader { if (headerName == null || headerName.isBlank()) headerName = "traceparent"; }
    }
    /** A header whose value is a bare 32-hex trace id (e.g. "X-Trace-Id"); no span id. */
    record RawHeader(String headerName) implements TraceContextExtraction {
        public RawHeader { if (headerName == null || headerName.isBlank()) throw new IllegalArgumentException("headerName"); }
    }
    /** Trace id (32 hex or a traceparent string) inside the JSON payload. */
    record PayloadPointer(JsonPointerExpr pointer) implements TraceContextExtraction {}

    default boolean yieldsSpanId() { return this instanceof TraceparentHeader; }
}

public sealed interface Transform
        permits Transform.None, Transform.Lowercase, Transform.Uppercase, Transform.Trim,
                Transform.Sha256, Transform.Mask, Transform.RegexExtract {
    record None() implements Transform {}
    record Lowercase() implements Transform {}
    record Uppercase() implements Transform {}
    record Trim() implements Transform {}
    /** Hex SHA-256 of the UTF-8 value; use for PII that must stay joinable but not readable. */
    record Sha256() implements Transform {}
    /** Keep the last N chars, replace the rest by '*'. transformArg = N (default 2). */
    record Mask(int keepLast) implements Transform {
        public Mask { if (keepLast < 0 || keepLast > 8) throw new IllegalArgumentException("keepLast 0..8"); }
    }
    /** transformArg = regex with exactly one capturing group; no match -> mapping yields no value. */
    record RegexExtract(Pattern pattern) implements Transform {
        public RegexExtract {
            if (pattern.matcher("").groupCount() != 1) throw new IllegalArgumentException("regex needs exactly 1 group");
        }
    }

    /** Builds the sealed variant from the persisted (transform, transformArg) pair. */
    static Transform of(String name, String arg) {
        return switch (name == null ? "NONE" : name) {
            case "NONE" -> new None();
            case "LOWERCASE" -> new Lowercase();
            case "UPPERCASE" -> new Uppercase();
            case "TRIM" -> new Trim();
            case "SHA256" -> new Sha256();
            case "MASK" -> new Mask(arg == null ? 2 : Integer.parseInt(arg));
            case "REGEX_EXTRACT" -> new RegexExtract(Pattern.compile(arg));
            default -> throw new IllegalArgumentException("unknown transform " + name);
        };
    }
}

public record FieldMapping(FieldSource source, JsonPointerExpr jsonPointer, String headerName,
                           AttributeKey targetKey, AttributeType valueType, Transform transform, boolean required) {
    public FieldMapping {
        switch (source) {
            case PAYLOAD -> { if (jsonPointer == null) throw new IllegalArgumentException("PAYLOAD needs jsonPointer"); }
            case HEADER -> { if (headerName == null || headerName.isBlank()) throw new IllegalArgumentException("HEADER needs headerName"); }
            case RECORD_KEY, RECORD_TIMESTAMP -> { /* no locator */ }
        }
        if (targetKey.isReservedForTcs()) throw new IllegalArgumentException("tc.* keys are reserved");
        if (targetKey.equals(AttributeKey.SERVICE_NAME) || targetKey.equals(AttributeKey.SPAN_NAME)
                || targetKey.equals(AttributeKey.STATUS)) throw new IllegalArgumentException("built-in key");
        boolean stringOnly = transform instanceof Transform.Sha256 || transform instanceof Transform.Mask
                || transform instanceof Transform.RegexExtract || transform instanceof Transform.Lowercase
                || transform instanceof Transform.Uppercase || transform instanceof Transform.Trim;
        if (stringOnly && valueType != AttributeType.STRING) throw new IllegalArgumentException("transform yields STRING");
        if (source == FieldSource.RECORD_TIMESTAMP && valueType != AttributeType.LONG && valueType != AttributeType.STRING)
            throw new IllegalArgumentException("timestamp maps to LONG (epoch ms) or STRING (ISO-8601)");
        if (transform == null) transform = new Transform.None();
    }
}

public record EventFilter(JsonPointerExpr jsonPointer, String equalsValue) {}

public record SourceDefinition(PayloadFormat payloadFormat, TraceContextExtraction traceContext,
                               EnrichmentScope scope, EventFilter filter, StartFrom startFrom,
                               OnMissingTraceContext onMissingTraceContext, List<FieldMapping> fieldMappings) {
    public SourceDefinition {
        fieldMappings = List.copyOf(fieldMappings);
        if (fieldMappings.isEmpty() || fieldMappings.size() > 64) throw new IllegalArgumentException("1..64 mappings");
        var keys = new HashSet<AttributeKey>();
        for (FieldMapping m : fieldMappings)
            if (!keys.add(m.targetKey())) throw new IllegalArgumentException("duplicate targetKey " + m.targetKey());
        if (scope == EnrichmentScope.SPAN && !traceContext.yieldsSpanId())
            throw new IllegalArgumentException("SPAN scope requires TRACEPARENT_HEADER (only it carries a span id)");
        if (payloadFormat == null) payloadFormat = PayloadFormat.JSON;
        if (startFrom == null) startFrom = StartFrom.LATEST;
        if (onMissingTraceContext == null) onMissingTraceContext = OnMissingTraceContext.DLQ;
    }
}
```

#### 4.7.2 Root and lifecycle

```java
package ru.aasmc.tracing.collector.domain.enrichment;

import java.time.Instant;

public enum SourceStatus { DRAFT, ACTIVE, PAUSED, DELETED }

public record EnrichmentSourceId(long value) {}

public record SourceRevision(EnrichmentSourceId sourceId, int revision, SourceDefinition definition,
                             String createdBy, Instant createdAt) {
    public SourceRevision { if (revision < 1) throw new IllegalArgumentException("revision >= 1"); }
}

public record EnrichmentSource(EnrichmentSourceId id, String name, KafkaConnectionId connectionId, String topic,
                               SourceStatus status, int currentRevision, String createdBy) {

    public EnrichmentSource {
        if (name == null || !name.matches("[a-z0-9][a-z0-9._-]{1,62}")) throw new IllegalArgumentException("name");
        if (topic == null || !topic.matches("[a-zA-Z0-9._-]{1,249}")) throw new IllegalArgumentException("topic");
        if (currentRevision < 1) throw new IllegalArgumentException("a source always has >= 1 revision");
    }

    /** Consumer group in the CLIENT cluster: tc-enricher.<sourceName> (stable across revisions => offsets kept). */
    public String consumerGroupId() { return "tc-enricher." + name; }

    /** Container id in tc-enricher: changes with revision so the registry diff restarts it. */
    public String containerId() { return "enrichment-source-" + id.value() + "-r" + currentRevision; }

    public EnrichmentSource activate() {
        return switch (status) {
            case DRAFT, PAUSED -> withStatus(SourceStatus.ACTIVE);
            case ACTIVE -> this;                                         // idempotent
            case DELETED -> throw new IllegalStateException("deleted source cannot be activated");
        };
    }

    public EnrichmentSource pause() {
        return switch (status) {
            case ACTIVE -> withStatus(SourceStatus.PAUSED);
            case PAUSED -> this;
            case DRAFT, DELETED -> throw new IllegalStateException("only ACTIVE can be paused, was " + status);
        };
    }

    public EnrichmentSource delete() {
        return switch (status) {
            case DRAFT, PAUSED -> withStatus(SourceStatus.DELETED);
            case DELETED -> this;
            case ACTIVE -> throw new IllegalStateException("pause the source before deleting it");
        };
    }

    public record Revised(EnrichmentSource source, int newRevision) {}

    /** Any non-deleted source may get a new revision; ACTIVE ones are restarted by the registry. */
    public Revised revise() {
        if (status == SourceStatus.DELETED) throw new IllegalStateException("deleted source is immutable");
        int next = currentRevision + 1;
        return new Revised(new EnrichmentSource(id, name, connectionId, topic, status, next, createdBy), next);
    }

    private EnrichmentSource withStatus(SourceStatus s) {
        return new EnrichmentSource(id, name, connectionId, topic, s, currentRevision, createdBy);
    }
}
```

Invariants of the aggregate:

1. **Revisions are immutable.** `enrichment_source_revision` rows are never updated. `current_revision` always points to an existing row.
2. **Topic and connection are part of the source, not the revision.** Changing them would require a new consumer group, because offsets belong to a topic in a cluster. Doing that means creating a new source and deleting the old one. The control plane rejects `PUT` with a changed `topic` or `connection_id` with `409 TOPIC_IMMUTABLE`.
3. **Names are unique for the lifetime of the DB**, including DELETED sources. The consumer group `tc-enricher.<sourceName>` may still hold offsets in the client cluster.
4. **Every lifecycle transition and revision is committed in the same transaction as `UPDATE tc.config_revision SET revision = revision + 1`.** That is how the data plane learns about the change (UC-05).
5. **Activation pre-conditions are checked by the control plane, not the aggregate**, because they need I/O:
   - the connection is reachable;
   - the topic exists (`describeTopics`);
   - TCS has READ + DESCRIBE + group READ ACLs;
   - every `targetKey` exists in `attribute_key` (it is auto-registered on save).

```mermaid
stateDiagram-v2
    [*] --> DRAFT : POST /enrichment-sources (revision 1)
    DRAFT --> DRAFT : PUT definition (revision n+1)
    DRAFT --> ACTIVE : POST activate (preconditions ok)
    ACTIVE --> PAUSED : POST pause
    PAUSED --> ACTIVE : POST activate
    ACTIVE --> ACTIVE : PUT definition (revision n+1, container restarted, same group)
    PAUSED --> PAUSED : PUT definition (revision n+1)
    DRAFT --> DELETED : DELETE source
    PAUSED --> DELETED : DELETE source
    DELETED --> [*]
    note right of ACTIVE
        Every tc-enricher instance runs one
        ConcurrentMessageListenerContainer per ACTIVE source,
        group tc-enricher.SOURCE_NAME. Kafka balances partitions.
    end note
    note left of DELETED
        Terminal. Rows in trace_enrichment
        remain until retention drops them.
    end note
```

### 4.8 EnrichmentRecord

```java
package ru.aasmc.tracing.collector.domain.enrichment;

import java.time.Instant;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue;
import ru.aasmc.tracing.collector.domain.trace.KafkaCoordinates;
import ru.aasmc.tracing.collector.domain.trace.SpanId;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/** One attribute contributed to a trace by one enrichment event. Append-only; never touches tc.span. */
public record EnrichmentRecord(TraceId traceId, SpanId spanId /* null for TRACE scope */,
                               EnrichmentSourceId sourceId, int sourceRevision,
                               AttributeKey key, AttributeValue value, KafkaCoordinates source) {
    public EnrichmentRecord {
        if (traceId == null || key == null || value == null || source == null) throw new IllegalArgumentException();
        if (sourceRevision < 1) throw new IllegalArgumentException("revision");
    }
    /** observed_at = Kafka record timestamp: deterministic on replay => part of the unique key. */
    public Instant observedAt() { return source.timestamp(); }
}
```

The idempotency key is `(source_id, kafka_partition, kafka_offset, key_id, observed_at)`. `observed_at` is included only because a PostgreSQL unique constraint on a partitioned table must contain the partition key. It is deterministic for a given record, so it does not weaken the key.

One event produces one `EnrichmentRecord` per mapping that yielded a value.

### 4.9 IndexedAttribute

```java
package ru.aasmc.tracing.collector.domain.attribute;

import java.time.Instant;

public enum OriginScope { ANY, SPAN, RESOURCE, ENRICHMENT }
public enum IndexStatus { BACKFILLING, ACTIVE, DISABLED }

/**
 * How a retained value is written into trace_digest.attributes (ADR-13).
 * SQL equivalents used by DigestCompactor: SHA256 -> encode(sha256(convert_to(v,'UTF8')),'hex'),
 * MASK -> left(v, 2) || '***'. The Java form is used by tc-query-api to transform search values.
 */
public enum DigestTransform {
    NONE, SHA256, MASK;

    private static final java.util.HexFormat HEX = java.util.HexFormat.of();

    public String apply(String v) {
        return switch (this) {
            case NONE -> v;
            case SHA256 -> {
                try {
                    var md = java.security.MessageDigest.getInstance("SHA-256");
                    yield HEX.formatHex(md.digest(v.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                } catch (java.security.NoSuchAlgorithmException e) {
                    throw new IllegalStateException(e);   // SHA-256 is mandatory in every JDK
                }
            }
            // left(v, 2) in SQL counts characters, so the Java side must not split a surrogate pair either
            case MASK -> v.substring(0, v.offsetByCodePoints(0, Math.min(2, v.codePointCount(0, v.length())))) + "***";
        };
    }

    /** EQ/IN on a SHA256 key can be answered (hash the query value); MASK destroys the value, so only NONE/SHA256. */
    public boolean searchableByEquality() { return this != MASK; }
}

public record IndexedAttribute(int keyId, AttributeKey key, OriginScope originScope, boolean numeric,
                               IndexStatus status, Instant indexedSince, boolean builtIn,
                               boolean retainInDigest, DigestTransform digestTransform) {

    public IndexedAttribute {
        if (builtIn && status == IndexStatus.DISABLED) throw new IllegalArgumentException("built-in keys cannot be disabled");
        if (digestTransform == null) digestTransform = DigestTransform.NONE;
        // A retained key MUST be indexed: compaction reads trace_attribute, never spans.
        if (retainInDigest && status == IndexStatus.DISABLED)
            throw new IllegalArgumentException("a DISABLED key cannot be retained in the digest");
        if (!retainInDigest && digestTransform != DigestTransform.NONE)
            throw new IllegalArgumentException("digestTransform is meaningful only for retained keys");
    }

    /** Not retroactive: affects compaction of trace days that are not yet DONE. */
    public IndexedAttribute retain(DigestTransform transform) {
        if (status == IndexStatus.DISABLED) throw new IllegalStateException("re-enable the key first");
        return new IndexedAttribute(keyId, key, originScope, numeric, status, indexedSince, builtIn, true, transform);
    }

    public IndexedAttribute stopRetaining() {
        return new IndexedAttribute(keyId, key, originScope, numeric, status, indexedSince, builtIn, false, DigestTransform.NONE);
    }

    public boolean writtenByIngest() { return status != IndexStatus.DISABLED; } // BACKFILLING keys are already written for new data

    public boolean acceptsOrigin(AttributeOrigin origin) {
        return switch (originScope) {
            case ANY -> origin != AttributeOrigin.DERIVED || builtIn;
            case SPAN -> origin == AttributeOrigin.SPAN;
            case RESOURCE -> origin == AttributeOrigin.RESOURCE;
            case ENRICHMENT -> origin == AttributeOrigin.ENRICHMENT;
        };
    }

    public IndexedAttribute backfillCompleted() {
        if (status != IndexStatus.BACKFILLING) throw new IllegalStateException("not backfilling");
        return new IndexedAttribute(keyId, key, originScope, numeric, IndexStatus.ACTIVE, indexedSince, builtIn,
                retainInDigest, digestTransform);
    }

    /** Disabling also stops retention: without trace_attribute rows compaction has nothing to copy. */
    public IndexedAttribute disable() {
        if (builtIn) throw new IllegalStateException("built-in");
        return new IndexedAttribute(keyId, key, originScope, numeric, IndexStatus.DISABLED, indexedSince, builtIn,
                false, DigestTransform.NONE);
    }

    /** Re-enabling starts a new backfill; indexed_since moves to now. */
    public IndexedAttribute reEnable(Instant now) {
        if (status != IndexStatus.DISABLED) throw new IllegalStateException("not disabled");
        return new IndexedAttribute(keyId, key, originScope, numeric, IndexStatus.BACKFILLING, now, builtIn,
                false, DigestTransform.NONE);
    }
}
```

```mermaid
stateDiagram-v2
    [*] --> BACKFILLING : POST /indexed-attributes (indexed_since = now)
    BACKFILLING --> ACTIVE : AttributeBackfill finished requested window
    BACKFILLING --> DISABLED : DELETE indexed attribute
    ACTIVE --> DISABLED : DELETE indexed attribute
    DISABLED --> BACKFILLING : POST again (re-enable, new backfill)
    note right of BACKFILLING
        Ingester/enricher write NEW values from the next
        config snapshot (<= 10 s). tc-query-api allows search
        but flags results partialIndex=true for windows
        older than indexed_since not yet backfilled.
    end note
    note right of DISABLED
        No new trace_attribute rows. Existing rows age out
        with partition retention (no DELETE).
        Search on the key returns 400 /problems/non-indexed-attribute.
    end note
```

Backfill progress is stored in `indexed_attribute.backfill_cursor timestamptz`, the lower bound of the oldest span partition already backfilled. This column is an **addition** to the brief's `indexed_attribute` definition. It makes `AttributeBackfill` resumable after a `tc-maintenance` leader change (UC-07, SD-09). The domain record exposes it as `Instant backfillCursor` (omitted above for brevity).

**Retained keys (ADR-13).** `retain_in_digest` and `digest_transform` are new columns of `indexed_attribute` (`boolean NOT NULL DEFAULT false`, `text NOT NULL DEFAULT 'NONE' CHECK IN ('NONE','SHA256','MASK')`). Rules:

- Setting `retainInDigest = true` on a key that is not indexed makes the control plane create the `indexed_attribute` row in BACKFILLING first (UC-07, UC-23).
- Enrichment-derived values are covered, because indexed ENRICHMENT keys are already written to `trace_attribute` (origin 3).
- The built-ins (`service.name`, `span.name`, `status`) are not flagged: the digest carries root service, root name, service ids, error count and outcome as columns.
- Typical IdP retained keys: `user_id` (SHA256 or NONE per policy), `client_id`, `auth.factor`, `sms.delivery.status`, `idp.auth_session_id`.
- The transform is applied once, at compaction. Changing it later does not rewrite compacted days, so the digest of one key may mix plain and hashed values across days. The control plane warns about this (UC-23).

Built-in rows (`service.name` with origin RESOURCE, `span.name` with origin DERIVED and written for root spans only, and `status` with origin DERIVED and written only for value `ERROR`) are seeded by `tc-schema-migrator`. They are `ACTIVE` from the start and cannot be disabled.

**Indexing rules for `trace_attribute` rows.** `AttributeIndexer` in `ingest.index` and `EnrichmentWriter` apply these rules:

- one row per `(trace_id, key_id, value token)` per batch, deduplicated in memory;
- arrays produce one row per element;
- `value_str` is the canonical token, capped at 256 chars (a btree entry must stay well below the ~2.7 kB limit; prefix search still works on truncated values);
- `value_num` is filled only when the key is registered `numeric = true` and the value is LONG or DOUBLE (or a parseable STRING);
- `ts` is the span start (SPAN and RESOURCE origin), the span start of the root (DERIVED), or `observed_at` (ENRICHMENT).

### 4.10 CorrelationType and CorrelationKey

```java
package ru.aasmc.tracing.collector.domain.correlation;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;

public enum KeyRole { PRIMARY, ALIAS }
public enum CorrelationTypeStatus { ACTIVE, DISABLED }

public record CorrelationKey(int id, int keyId, AttributeKey key, KeyRole role, String stepLabel) {
    public CorrelationKey {
        if (stepLabel != null && stepLabel.length() > 64) throw new IllegalArgumentException("stepLabel <= 64 chars");
    }
}

public record CorrelationType(int id, String name, String description, Duration mergeWindow,
                              CorrelationTypeStatus status, List<CorrelationKey> keys,
                              OutcomeRule outcomeRule /* nullable: no rule => SUCCESS unless a member errored */) {
    public static final Duration MIN_WINDOW = Duration.ofMinutes(1);
    public static final Duration MAX_WINDOW = Duration.ofDays(30);

    public CorrelationType {
        if (name == null || !name.matches("[a-z0-9][a-z0-9._-]{1,62}")) throw new IllegalArgumentException("name");
        if (mergeWindow == null) mergeWindow = Duration.ofDays(1);
        if (mergeWindow.compareTo(MIN_WINDOW) < 0 || mergeWindow.compareTo(MAX_WINDOW) > 0)
            throw new IllegalArgumentException("mergeWindow out of [1m, 30d]");
        keys = List.copyOf(keys);
        if (keys.stream().noneMatch(k -> k.role() == KeyRole.PRIMARY))
            throw new IllegalArgumentException("at least one PRIMARY key required");
        var seen = new HashSet<Integer>();
        for (CorrelationKey k : keys)
            if (!seen.add(k.keyId())) throw new IllegalArgumentException("key used twice: " + k.key());
        if (outcomeRule != null && outcomeRule.finalStepLabel() != null
                && keys.stream().noneMatch(k -> outcomeRule.finalStepLabel().equals(k.stepLabel())))
            throw new IllegalArgumentException("finalStepLabel must be the stepLabel of one of the type's keys");
    }

    public java.util.Optional<CorrelationKey> keyFor(int attributeKeyId) {
        return keys.stream().filter(k -> k.keyId() == attributeKeyId).findFirst();
    }
}
```

#### 4.10.1 OutcomeRule

Stored in the new column `correlation_type.outcome_rule jsonb NULL`, e.g. `{"outcomeKey":"business.outcome","successValues":["SUCCESS"],"failureValues":["FAILED","DENIED"],"finalStepLabel":"token-exchange"}`. All fields are optional. It is evaluated only by `DigestCompactor`, once per root operation, when the operation is compacted (§4.14).

```java
package ru.aasmc.tracing.collector.domain.correlation;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.digest.OperationOutcome;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

public record OutcomeRule(AttributeKey outcomeKey, Set<String> successValues, Set<String> failureValues,
                          String finalStepLabel) {

    public OutcomeRule {
        successValues = successValues == null ? Set.of() : Set.copyOf(successValues);
        failureValues = failureValues == null ? Set.of() : Set.copyOf(failureValues);
        if (!java.util.Collections.disjoint(successValues, failureValues))
            throw new IllegalArgumentException("a value cannot be both success and failure");
        if (outcomeKey == null && !(successValues.isEmpty() && failureValues.isEmpty()))
            throw new IllegalArgumentException("successValues/failureValues need an outcomeKey");
        if (finalStepLabel != null && (finalStepLabel.isBlank() || finalStepLabel.length() > 64))
            throw new IllegalArgumentException("finalStepLabel 1..64 chars");
        // Checked by the control plane (needs the snapshot): outcomeKey is an indexed key (read from trace_attribute).
    }

    /** What compaction knows about one member trace of the operation (from trace_digest / trace_attribute / trace_correlation). */
    public record MemberTrace(TraceId traceId, Instant startTime, int errorCount,
                              Optional<String> outcomeValue,      // raw value of outcomeKey on this trace, if any
                              Set<String> stepLabels) {           // labels of the correlation keys that linked it
        public MemberTrace { stepLabels = Set.copyOf(stepLabels); }
    }

    public record Evaluation(OperationOutcome outcome, String outcomeDetail /* raw outcomeKey value or null */) {}

    /** No rule configured: rules 2 and 4 of the order below still apply. */
    public static Evaluation evaluate(OutcomeRule ruleOrNull, List<MemberTrace> members) {
        return (ruleOrNull == null ? new OutcomeRule(null, null, null, null) : ruleOrNull).evaluate(members);
    }

    /**
     * Fixed evaluation order (ADR-13):
     * 1. outcomeKey set and a value found on any member (the latest by trace start wins):
     *    SUCCESS if in successValues, FAILED if in failureValues, else UNKNOWN; raw value kept as outcome_detail;
     * 2. else any member trace has errorCount > 0 -> FAILED;
     * 3. else finalStepLabel set: some member linked via a key with that label -> SUCCESS, otherwise INCOMPLETE;
     * 4. else -> SUCCESS.
     */
    public Evaluation evaluate(List<MemberTrace> members) {
        if (members.isEmpty()) return new Evaluation(OperationOutcome.UNKNOWN, null);
        if (outcomeKey != null) {
            Optional<String> latest = members.stream()
                    .filter(m -> m.outcomeValue().isPresent())
                    .max(Comparator.comparing(MemberTrace::startTime).thenComparing(MemberTrace::traceId))
                    .flatMap(MemberTrace::outcomeValue);
            if (latest.isPresent()) {
                String v = latest.get();
                OperationOutcome o = successValues.contains(v) ? OperationOutcome.SUCCESS
                        : failureValues.contains(v) ? OperationOutcome.FAILED
                        : OperationOutcome.UNKNOWN;
                return new Evaluation(o, v);
            }
        }
        if (members.stream().anyMatch(m -> m.errorCount() > 0)) return new Evaluation(OperationOutcome.FAILED, null);
        if (finalStepLabel != null) {
            boolean reached = members.stream().anyMatch(m -> m.stepLabels().contains(finalStepLabel));
            return new Evaluation(reached ? OperationOutcome.SUCCESS : OperationOutcome.INCOMPLETE, null);
        }
        return new Evaluation(OperationOutcome.SUCCESS, null);
    }
}
```

IdP example: `idp.login` has the rule `{"finalStepLabel":"token-exchange"}` (no `outcomeKey`, because the IdP services do not emit an outcome attribute). Operation `0193a6f2…` has T1 (`authentication`), T2 (`authentication`, `token-exchange`) and T3 (`token-exchange`), all with `error_count = 0`. Rule 1 is skipped, rule 2 does not fire, rule 3 finds `token-exchange` reached: **SUCCESS**. A login abandoned after the SMS step (T1, T2 without the code key, no T3) would be **INCOMPLETE**; one whose `idp-otp-service` span failed would be **FAILED**.

`outcome_detail` stores the raw `outcomeKey` value. The control plane therefore warns when the outcome key is classified PERSONAL or SECRET (§7.3); outcome values should be business codes, not identifiers.

The roles mean the following:

- **PRIMARY.** The value is the natural identifier of the operation (`idp.auth_session_id = as-777`). The first PRIMARY value attached to an operation becomes its `display_value`.
- **ALIAS.** The value is a secondary identifier that some traces carry instead of the primary one (`oauth.authorization_code = code-abc` on T3). It bridges traces into the same operation.

  An alias observed *before* any primary value still creates a provisional operation with `display_value = NULL`, so the result does not depend on arrival order. The provisional operation is merged later (UC-10).
- **step_label** names the business step evidenced by the key, e.g. `authentication` for the session key and `token-exchange` for the code. The timeline shows the label of the key that linked each trace.
- **merge_window** (default `1 day`). An observation can join an existing operation only if its `observed_at` lies within `[first_seen - window, last_seen + window]` of that operation. Otherwise the value is treated as **reused** (e.g. a recycled session id the next day). A new operation is created and `correlation_value` is repointed to it.
- **Hub guard.** An operation stops absorbing merges once `trace_count` reaches `tc.correlation.max-traces-per-operation` (default 10 000). This protects against keys misconfigured as correlation keys, e.g. `client_id`, which would otherwise glue millions of traces together. The event is logged and counted (`tc_correlation_hub_blocked_total`).

A single attribute key may belong to several correlation types (`UNIQUE (correlation_type_id, key_id)` only).

### 4.11 CorrelationObservation, CorrelationValue, BusinessOperation

```java
package ru.aasmc.tracing.collector.domain.correlation;

import java.time.Instant;
import java.util.UUID;
import ru.aasmc.tracing.collector.domain.attribute.AttributeOrigin;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/** Row of tc.trace_correlation (append-only). observed_at = span start (SPAN/RESOURCE) or Kafka ts (ENRICHMENT). */
public record CorrelationObservation(long id, Instant observedAt, int correlationTypeId, int correlationKeyId,
                                     String value, TraceId traceId, AttributeOrigin origin) {
    public static final int MAX_VALUE_LENGTH = 512;
    public CorrelationObservation {
        if (value == null || value.isBlank() || value.length() > MAX_VALUE_LENGTH)
            throw new IllegalArgumentException("correlation value must be 1..512 chars");
    }
}

/** Row of tc.correlation_value: current owner of a (type, value). */
public record CorrelationValue(int correlationTypeId, String value, UUID operationId, Instant operationFirstSeen,
                               Instant updatedAt) {}

/** Row of tc.business_operation. id = uuidv7() (time-ordered). */
public record BusinessOperation(UUID id, Instant firstSeen, Instant lastSeen, int correlationTypeId,
                                String displayValue, int traceCount, UUID mergedInto) {
    public BusinessOperation {
        if (id == null || id.version() != 7) throw new IllegalArgumentException("operation id must be uuidv7");
        if (lastSeen.isBefore(firstSeen)) throw new IllegalArgumentException("lastSeen < firstSeen");
        if (traceCount < 0) throw new IllegalArgumentException("traceCount");
        if (id.equals(mergedInto)) throw new IllegalArgumentException("cannot merge into itself");
    }

    public boolean isMerged() { return mergedInto != null; }

    public boolean accepts(Instant observedAt, java.time.Duration window) {
        return !observedAt.isBefore(firstSeen.minus(window)) && !observedAt.isAfter(lastSeen.plus(window));
    }

    public BusinessOperation observe(Instant observedAt, String primaryValueOrNull) {
        return new BusinessOperation(id,
                observedAt.isBefore(firstSeen) ? observedAt : firstSeen,
                observedAt.isAfter(lastSeen) ? observedAt : lastSeen,
                correlationTypeId,
                displayValue != null ? displayValue : primaryValueOrNull,
                traceCount, mergedInto);
    }

    /**
     * Survivor rule (deterministic, order-independent):
     * 1) an operation WITH a display value (has seen a PRIMARY key) beats a provisional one;
     * 2) otherwise the earlier firstSeen; 3) otherwise the smaller id.
     */
    public static BusinessOperation survivor(BusinessOperation a, BusinessOperation b) {
        if ((a.displayValue != null) != (b.displayValue != null)) return a.displayValue != null ? a : b;
        int c = a.firstSeen.compareTo(b.firstSeen);
        if (c != 0) return c < 0 ? a : b;
        return a.id.compareTo(b.id) <= 0 ? a : b;
    }

    public BusinessOperation mergedInto(UUID survivorId) {
        return new BusinessOperation(id, firstSeen, lastSeen, correlationTypeId, displayValue, traceCount, survivorId);
    }
}
```

**OperationForest: union-find.** `CorrelationResolver` in `tc-maintenance` runs every 10 s under ShedLock, so only one leader writes. Each tick it:

1. Reads `trace_correlation WHERE id > :watermark ORDER BY id LIMIT 10 000`.
2. Loads the `correlation_value` rows and operations they touch, plus, for each trace in the batch, the other observations of the same trace and type (index `(trace_id)`). That makes the "value–trace–value" edges visible.
3. Builds an in-memory disjoint-set forest whose nodes are `(type, value)` and whose edges are "occur on the same trace". It applies union by survivor rank (rule above) and path compression.
4. Persists the result:
   - each new component gets a new operation (`uuidv7()`);
   - components spanning several existing operations are **merged**: the losers get `merged_into = survivor.id`, and every `correlation_value` pointing at a loser is repointed to the survivor. Any loser that was itself a survivor of earlier merges gets its children repointed too, so the persisted forest always has **depth ≤ 1**;
   - `trace_count` of every touched survivor is recomputed exactly as `count(DISTINCT trace_id)` over its values.
5. Advances `correlation_resolver_state.last_processed_id` in the same transaction.

Invariants of the BusinessOperation aggregate:

- **I1.** A `correlation_value` row always points at a root operation (`merged_into IS NULL`).
- **I2.** A merged operation is never un-merged. The API resolves a merged id to its survivor (HTTP 200 with `mergedFrom`).
- **I3.** All values in one operation belong to the same correlation type. Different types never merge, so an `idp.login` operation and a `payment.checkout` operation stay separate even if they share traces.
- **I4.** `first_seen ≤ last_seen`, and both lie inside the union of the observations' `observed_at`.
- **I5.** The resolver is idempotent. Re-processing a row already applied changes nothing, which is what makes the gap-tolerant watermark safe.

**Note (watermark gaps).** Identity values are assigned at insert time, but transactions commit in a different order. A naive `id > watermark` could therefore skip a row that commits late. The resolver advances the watermark only up to the first id gap younger than `tc.correlation.gap-grace` (30 s). Gaps older than that are treated as rolled-back ids. Because processing is idempotent (I5), re-reading after the grace period is harmless.

```mermaid
stateDiagram-v2
    [*] --> PROVISIONAL : first observation is an ALIAS value (display_value NULL)
    [*] --> OPEN : first observation is a PRIMARY value
    PROVISIONAL --> OPEN : PRIMARY value observed on one of its traces
    PROVISIONAL --> MERGED : component joins an operation that wins survivor rule
    OPEN --> OPEN : new trace joins (trace_count++, last_seen advances)
    OPEN --> MERGED : loses survivor rule against another operation
    OPEN --> CAPPED : trace_count reaches max-traces-per-operation
    CAPPED --> CAPPED : further merges refused (metric)
    OPEN --> DIGESTED : last_seen < now - n days and all member trace days DONE (BusinessOperationCompacted)
    CAPPED --> DIGESTED : same rule
    PROVISIONAL --> DIGESTED : same rule (display_value stays NULL)
    MERGED --> [*] : monthly partition dropped (whole month older than n days and compacted), not digested separately
    DIGESTED --> [*] : FULL row dropped with its month (correlation_value rows deleted), digest kept until n + m days
    note right of MERGED
        merged_into = survivor id (depth <= 1).
        GET business-operations by id answers with the survivor.
    end note
```

`PROVISIONAL`, `OPEN` and `CAPPED` are derived states (from `display_value` and `trace_count`) and are not stored in a status column. `MERGED` is `merged_into IS NOT NULL`. `DIGESTED` means a `business_operation_digest` row with the same id exists (§4.14). The operation stays readable from the full tier until its monthly partition is dropped (effectively ≤ ~38 days with n = 7), and from the digest for n + m days after `first_seen`.

### 4.12 RetentionPolicy and ConfigRevision

```java
package ru.aasmc.tracing.collector.domain.config;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

public enum Granularity { HOUR, DAY, MONTH }
public enum RetentionTier { FULL, DIGEST }   // retention_policy.tier CHECK IN ('FULL','DIGEST')

public record RetentionPolicy(String tableName, RetentionTier tier, Duration retention, Granularity granularity,
                              Duration precreate) {
    static final Set<String> FULL_TABLES = Set.of("span", "trace_attribute", "span_link", "trace_summary",
            "trace_enrichment", "trace_correlation", "business_operation");
    static final Set<String> DIGEST_TABLES = Set.of("trace_digest", "business_operation_digest");

    public RetentionPolicy {
        boolean full = FULL_TABLES.contains(tableName), digest = DIGEST_TABLES.contains(tableName);
        if (!full && !digest) throw new IllegalArgumentException("not a partitioned table: " + tableName);
        if (tier != (full ? RetentionTier.FULL : RetentionTier.DIGEST))
            throw new IllegalArgumentException(tableName + " belongs to tier " + (full ? "FULL" : "DIGEST"));
        Duration unit = switch (granularity) {
            case HOUR -> Duration.ofHours(1); case DAY -> Duration.ofDays(1); case MONTH -> Duration.ofDays(31);
        };
        if (retention.compareTo(unit) < 0) throw new IllegalArgumentException("retention shorter than one partition");
        if (precreate.compareTo(Duration.ofDays(1)) < 0) throw new IllegalArgumentException("precreate >= 1 day");
    }

    /**
     * Cross-policy invariant, checked by the control plane on every change. Since ADR-13 every row of a tier
     * carries the same retention (set through RetentionTiers), so the older per-table rules hold trivially:
     * trace_summary >= span, trace_attribute <= span, business_operation <= trace_correlation.
     */
    public static void validateSet(Map<String, RetentionPolicy> all, RetentionTiers tiers) {
        for (RetentionPolicy p : all.values()) {
            Duration expected = p.tier() == RetentionTier.FULL ? tiers.fullRetention() : tiers.digestDropAge();
            if (p.tableName().equals("business_operation")) continue;   // MONTH grain, dropped by the drop gate
            if (!p.retention().equals(expected))
                throw new IllegalArgumentException(p.tableName() + " must have the retention of tier " + p.tier());
        }
    }
}

/**
 * Two-tier retention (ADR-13). FULL tier kept fullDays (n); DIGEST tier kept digestDays (m) AFTER the full tier,
 * i.e. a digest day is dropped when older than n + m days. Changed only via PUT /api/v1/admin/retention-policies/tiers.
 */
public record RetentionTiers(int fullDays, int digestDays) {
    public static final int MIN_FULL_DAYS = 2;

    public RetentionTiers {
        if (fullDays < MIN_FULL_DAYS)
            throw new IllegalArgumentException("fullDays >= 2 (compaction needs a whole closed day plus slack)");
        if (digestDays < 0) throw new IllegalArgumentException("digestDays >= 0 (0 = digest disabled)");
    }

    /** Seed defaults; Spring properties tc.retention.full-days / tc.retention.digest-days are read by the seed migration only. */
    public static final RetentionTiers DEFAULT = new RetentionTiers(7, 90);

    /** fullDays must exceed every correlation merge window (rounded up to days): a value may re-join an operation until then. */
    public void requireLongerThan(Duration maxMergeWindow) {
        long windowDays = (maxMergeWindow.toSeconds() + 86_399) / 86_400;   // round up
        if (fullDays <= windowDays)
            throw new IllegalArgumentException("fullDays must be > max correlation merge_window (" + windowDays + " d)");
    }

    public boolean digestEnabled() { return digestDays > 0; }
    public Duration fullRetention() { return Duration.ofDays(fullDays); }
    /** Age at which a digest partition is dropped: n + m days (default 97). */
    public Duration digestDropAge() { return Duration.ofDays((long) fullDays + digestDays); }
}

public record ConfigRevision(long value) implements Comparable<ConfigRevision> {
    public ConfigRevision { if (value < 0) throw new IllegalArgumentException(); }
    public ConfigRevision next() { return new ConfigRevision(value + 1); }
    @Override public int compareTo(ConfigRevision o) { return Long.compare(value, o.value); }
}
```

Baseline policy rows (seeded by `tc-schema-migrator` from `tc.retention.full-days=7` and `tc.retention.digest-days=90`). Before ADR-13 the values were 14 d (`span`, `trace_attribute`, `span_link`), 30 d (`trace_summary`, `trace_enrichment`) and 90 d (`trace_correlation`, `business_operation`); all full-tier tables now share n.

| table_name | tier | retention | partition_granularity | precreate |
|---|---|---|---|---|
| span | FULL | 7 days (n) | HOUR | 3 days |
| trace_attribute | FULL | 7 days (n) | HOUR | 3 days |
| span_link | FULL | 7 days (n) | DAY | 3 days |
| trace_summary | FULL | 7 days (n) | DAY | 3 days |
| trace_enrichment | FULL | 7 days (n) | DAY | 3 days |
| trace_correlation | FULL | 7 days (n) | DAY | 3 days |
| business_operation | FULL | 7 days (n); a month is dropped only when the whole month is older than n days **and** compacted (effectively ≤ ~38 days) | MONTH | 3 days (i.e. next month created ≥ 3 days before it starts) |
| trace_digest | DIGEST | 97 days (n + m) | DAY | 3 days |
| business_operation_digest | DIGEST | 97 days (n + m) | DAY | 3 days |

`correlation_value` is unpartitioned and is cleaned with its `business_operation` month (UC-16), so it is part of the full tier without a policy row. Every drop of a FULL-tier partition passes the **drop gate** (§4.14.4). Capacity of both tiers is in 04 §9.

Changing `partition_granularity` affects only partitions created *after* the change. Existing partitions keep their bounds. `PartitionManager` creates the new grain starting at the first boundary after the last existing partition.

### 4.13 DLQ record

```java
package ru.aasmc.tracing.collector.domain.dlq;

public enum DeadLetterReason {
    // otlp-traces.dlq
    MALFORMED_JSON, UNSUPPORTED_SCHEMA_VERSION, INVALID_TRACE_ID, INVALID_SPAN_ID, TRACE_ID_MISMATCH,
    FUTURE_TIMESTAMP, FRAGMENT_TOO_LARGE, INVALID_ATTRIBUTE_TYPE,
    // tc-enrichment.dlq
    NO_TRACE_CONTEXT, PAYLOAD_DECODE_ERROR, REQUIRED_FIELD_MISSING, TRANSFORM_ERROR, VALUE_TYPE_MISMATCH
}

/** Original key/value bytes are copied unchanged; diagnostics go to headers. */
public record DeadLetter(String targetTopic, DeadLetterReason reason, String detail, Long sourceId) {}
```

DLQ headers:

- `otlp-traces.dlq` carries the standard Spring `kafka_dlt-original-topic`, `kafka_dlt-original-partition`, `kafka_dlt-original-offset`, `kafka_dlt-original-timestamp`, `kafka_dlt-exception-fqcn` and `kafka_dlt-exception-message`, plus `tc-error-reason` (a `DeadLetterReason` name).
- `tc-enrichment.dlq` carries the same `kafka_dlt-*` headers plus `tc-source-id` and `tc-error`.

Size limits:

- The DLQ topics have 6 partitions, so the destination resolver returns partition `-1` (Kafka chooses) instead of Spring's default "same partition".
- Exception messages are truncated to 1 KiB, and never contain payload fragments (PII).

### 4.14 Digest tier: TraceDigest, BusinessOperationDigest and compaction (ADR-13)

The digest tier keeps a compact history for m days after the n-day full tier: per trace, *what happened* (root, duration, outcome, retained attributes, business operation); per business operation, *its steps and its outcome*. It is produced only by `DigestCompactor` (Retention context) and read only by `tc-query-api`.

#### 4.14.1 Outcomes

```java
package ru.aasmc.tracing.collector.domain.digest;

/** trace_digest.outcome smallint. */
public enum TraceOutcome {
    OK((short) 1), ERROR((short) 2);
    public final short code;
    TraceOutcome(short code) { this.code = code; }
    public static TraceOutcome ofErrorCount(int errorCount) { return errorCount > 0 ? ERROR : OK; }
}

/** business_operation_digest.outcome smallint; produced by OutcomeRule.evaluate (§4.10.1). */
public enum OperationOutcome {
    UNKNOWN((short) 0), SUCCESS((short) 1), FAILED((short) 2), INCOMPLETE((short) 3);
    public final short code;
    OperationOutcome(short code) { this.code = code; }
    public static OperationOutcome ofCode(short c) {
        for (OperationOutcome o : values()) if (o.code == c) return o;
        throw new IllegalArgumentException("unknown outcome " + c);
    }
}
```

#### 4.14.2 TraceDigest and BusinessOperationDigest

```java
package ru.aasmc.tracing.collector.domain.digest;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.UUID;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * Row of tc.trace_digest (PARTITION BY RANGE (trace_day), daily, retention n + m). PK (trace_id, trace_day).
 * attributes: retained key -> distinct values after DigestTransform, e.g. {"auth.factor":["SMS"],"user_id":["16ec52..."]}.
 */
public record TraceDigest(LocalDate traceDay, TraceId traceId, Instant startTime, long durationNanos,
                          Integer rootServiceId, Integer rootNameId, int spanCount, int errorCount,
                          TraceOutcome outcome, SortedSet<Integer> serviceIds, UUID businessOperationId,
                          Map<String, List<String>> attributes, Instant compactedAt) {
    public TraceDigest {
        if (traceDay == null || traceId == null || startTime == null) throw new IllegalArgumentException("identity");
        if (spanCount < 1 || errorCount < 0 || errorCount > spanCount) throw new IllegalArgumentException("counts");
        if (outcome != TraceOutcome.ofErrorCount(errorCount)) throw new IllegalArgumentException("outcome must follow error_count");
        if (durationNanos < 0) throw new IllegalArgumentException("duration");
        serviceIds = java.util.Collections.unmodifiableSortedSet(new java.util.TreeSet<>(serviceIds));
        attributes = Map.copyOf(attributes);           // values are already transformed; never raw PII for SHA256/MASK keys
    }
    /** Same trace_day as the full-tier trace_summary row it was compacted from (a split trace keeps 2 digest rows). */
    public boolean belongsToOperation() { return businessOperationId != null; }
}

/** One element of business_operation_digest.steps, ordered by startTime. */
public record DigestStep(String stepLabel, TraceId traceId, Instant startTime, double durationMs, TraceOutcome outcome) {}

/**
 * Row of tc.business_operation_digest (PARTITION BY RANGE (op_day), daily, retention n + m; op_day = first_seen::date UTC).
 * Only ROOT operations are digested (merged_into IS NULL); their id is the full-tier operation id, so links survive.
 */
public record BusinessOperationDigest(LocalDate opDay, UUID operationId, int correlationTypeId, String displayValue,
                                      Instant firstSeen, Instant lastSeen, int traceCount,
                                      OperationOutcome outcome, String outcomeDetail, List<DigestStep> steps,
                                      Map<String, List<String>> attributes, Instant compactedAt) {
    public BusinessOperationDigest {
        if (lastSeen.isBefore(firstSeen)) throw new IllegalArgumentException("lastSeen < firstSeen");
        if (!opDay.equals(java.time.LocalDate.ofInstant(firstSeen, java.time.ZoneOffset.UTC)))
            throw new IllegalArgumentException("op_day = first_seen::date (UTC)");
        steps = List.copyOf(steps);
        attributes = Map.copyOf(attributes);           // union of the members' retained attributes
        if (outcomeDetail != null && outcomeDetail.length() > 256) throw new IllegalArgumentException("outcome_detail <= 256");
    }
}
```

**Step label of a member trace.** A trace linked by several keys (T2 is linked by `idp.auth_session_id` and `oauth.authorization_code`) gets the label of the key declared **last** in the correlation type (keys are declared in business order), so T2's step is `token-exchange`. The outcome rule still sees all labels of a trace (`MemberTrace.stepLabels`).

**Built-ins vs retained keys.** Root service, root span name, duration, span/error counts, `service_ids` and `business_operation_id` are always columns. Only keys flagged `retain_in_digest` go into `attributes`. Keys not flagged are lost after n days; this trade-off is stated in ADR-13 (02).

#### 4.14.3 CompactionExclusion and CompactionState

```java
package ru.aasmc.tracing.collector.domain.digest;

import java.time.Instant;
import java.time.LocalDate;

/** Row of tc.compaction_exclusion: roots matching it are not digested unless the trace belongs to a business operation. */
public record CompactionExclusion(int id, String serviceName /* null = any service */, String rootNamePattern /* SQL LIKE */,
                                  String description, String createdBy, Instant createdAt) {
    public CompactionExclusion {
        if (rootNamePattern == null || rootNamePattern.isBlank() || rootNamePattern.length() > 256)
            throw new IllegalArgumentException("rootNamePattern 1..256 chars");
        if (rootNamePattern.equals("%") && serviceName == null)
            throw new IllegalArgumentException("excluding every trace: set digestDays = 0 instead");
    }
}

public enum CompactionStatus { PENDING, RUNNING, DONE, FAILED }

/** Row of tc.compaction_state; written only by the DigestCompactor leader (ShedLock digest-compactor). */
public record CompactionState(LocalDate traceDay, CompactionStatus status, Instant lastChunkEnd,
                              long tracesDigested, long tracesExcluded, long operationsDigested,
                              Instant startedAt, Instant finishedAt, String error) {

    /** Eligible when the whole day is older than n days: D + 1 day <= now - n days (fullDays from RetentionTiers). */
    public static boolean eligible(LocalDate day, Instant now, int fullDays) {
        Instant dayEnd = day.plusDays(1).atStartOfDay(java.time.ZoneOffset.UTC).toInstant();
        return !dayEnd.isAfter(now.minus(java.time.Duration.ofDays(fullDays)));
    }

    public CompactionState start(Instant now) {
        return switch (status) {
            case PENDING, FAILED -> new CompactionState(traceDay, CompactionStatus.RUNNING, lastChunkEnd,
                    tracesDigested, tracesExcluded, operationsDigested, now, null, null);
            case RUNNING -> this;   // resumed by a new leader from lastChunkEnd
            case DONE -> throw new IllegalStateException("day already compacted");
        };
    }
    public CompactionState chunkDone(Instant chunkEnd, long digested, long excluded) {
        if (status != CompactionStatus.RUNNING) throw new IllegalStateException("not running");
        return new CompactionState(traceDay, status, chunkEnd, tracesDigested + digested, tracesExcluded + excluded,
                operationsDigested, startedAt, null, null);
    }
    public CompactionState done(Instant now) {
        if (status != CompactionStatus.RUNNING) throw new IllegalStateException("not running");
        return new CompactionState(traceDay, CompactionStatus.DONE, lastChunkEnd, tracesDigested, tracesExcluded,
                operationsDigested, startedAt, now, null);
    }
    public CompactionState failed(String error) {
        return new CompactionState(traceDay, CompactionStatus.FAILED, lastChunkEnd, tracesDigested, tracesExcluded,
                operationsDigested, startedAt, null, error);
    }
}
```

```mermaid
stateDiagram-v2
    [*] --> PENDING : row inserted when trace day D becomes eligible (D + 1 day <= now - n days)
    PENDING --> RUNNING : DigestCompactor tick acquires lock digest-compactor
    RUNNING --> RUNNING : 1-hour chunk committed (last_chunk_end advances)
    RUNNING --> DONE : last chunk of D committed (TraceDayCompacted)
    RUNNING --> FAILED : chunk statement fails or times out (statement_timeout 15 min)
    FAILED --> RUNNING : next tick retries from last_chunk_end
    DONE --> [*] : row kept; drop gate reads it until the FULL partitions of D are gone
    note right of RUNNING
        Leader change mid-day: the new leader
        resumes from last_chunk_end. Re-running a
        chunk is harmless (ON CONFLICT DO NOTHING).
    end note
    note right of FAILED
        Drop gate stays closed for every FULL-tier
        partition overlapping D. tc_compaction_lag_days
        grows; alert above 1 day.
    end note
```

#### 4.14.4 Compaction and drop-gate rules

Invariants enforced by `DigestCompactor` and `PartitionManager`:

- **C1. Unit of work** is one trace day D, processed in 1-hour chunks of `trace_summary.start_time` within D. One set-based `INSERT ... SELECT` per chunk; **no span reads**.
- **C2. Read window.** Retained values are read from `trace_attribute` with `ts` in `[D − 1 h, D + 1 day + 6 h)`. Values outside it (very late enrichment) are not captured.
- **C3. Idempotent and resumable.** `ON CONFLICT DO NOTHING` on the digest PKs, progress in `compaction_state.last_chunk_end`.
- **C4. Exclusions.** A trace whose root matches a `compaction_exclusion` row is not digested unless it belongs to a business operation. It is counted in `traces_excluded`.
- **C5. Business operations** are compacted after D is DONE, for every root operation (`merged_into IS NULL`) with `last_seen < now − n days` whose member trace days are all DONE. Merged operations are represented by their survivor.
- **C6. Drop gate.** A FULL-tier partition may be dropped only if its upper bound ≤ now − n days **and** every trace day D whose read window overlaps the partition is DONE. Never drop uncompacted data. With `digestDays = 0` the gate is skipped and nothing is compacted.
- **C7. Not retroactive.** Flagging a key, changing a transform or an outcome rule affects only days not yet DONE.
- **C8. Digest is write-once.** Digest rows are never updated; they disappear only by partition drop at n + m days.

#### 4.14.5 Class diagram: digest tier

```mermaid
classDiagram
    direction LR
    class TraceDigest {
        +LocalDate traceDay
        +TraceId traceId
        +Instant startTime
        +long durationNanos
        +Integer rootServiceId
        +Integer rootNameId
        +int spanCount
        +int errorCount
        +TraceOutcome outcome
        +SortedSet~Integer~ serviceIds
        +UUID businessOperationId
        +Map attributes
    }
    class BusinessOperationDigest {
        +LocalDate opDay
        +UUID operationId
        +int correlationTypeId
        +String displayValue
        +Instant firstSeen
        +Instant lastSeen
        +int traceCount
        +OperationOutcome outcome
        +String outcomeDetail
        +List~DigestStep~ steps
        +Map attributes
    }
    class DigestStep {
        +String stepLabel
        +TraceId traceId
        +Instant startTime
        +double durationMs
        +TraceOutcome outcome
    }
    class TraceOutcome {
        <<enumeration>>
        OK
        ERROR
    }
    class OperationOutcome {
        <<enumeration>>
        UNKNOWN
        SUCCESS
        FAILED
        INCOMPLETE
    }
    class CompactionState {
        +LocalDate traceDay
        +CompactionStatus status
        +Instant lastChunkEnd
        +long tracesDigested
        +long tracesExcluded
        +long operationsDigested
    }
    class CompactionExclusion {
        +int id
        +String serviceName
        +String rootNamePattern
    }
    class OutcomeRule {
        +AttributeKey outcomeKey
        +String finalStepLabel
        +evaluate(List~MemberTrace~) Evaluation
    }
    class TraceSummary
    class BusinessOperation
    class IndexedAttribute {
        +boolean retainInDigest
        +DigestTransform digestTransform
    }
    TraceDigest --> TraceOutcome
    TraceDigest ..> TraceSummary : compacted from
    TraceDigest ..> IndexedAttribute : attributes of retained keys
    TraceDigest "*" ..> "0..1" BusinessOperationDigest : businessOperationId
    BusinessOperationDigest ..> BusinessOperation : compacted from (root only)
    BusinessOperationDigest *-- "1..*" DigestStep
    BusinessOperationDigest --> OperationOutcome
    OutcomeRule ..> OperationOutcome : produces
    CompactionState ..> TraceDigest : tracks trace day
    CompactionExclusion ..> TraceDigest : filters
```

---

## 5. Domain events

Domain events are **internal, in-process facts**. They are published with Spring's `ApplicationEventPublisher` and consumed by `@TransactionalEventListener(phase = AFTER_COMMIT)` handlers. Those handlers:

- update Micrometer metrics;
- write structured logs and audit;
- warm caches, e.g. `TraceDayResolver` after commit;
- trigger in-process reactions, e.g. `DynamicContainerManager` in `tc-enricher`.

They are **not Kafka topics**. ADR-01 forbids new topics, and the cross-service propagation they would provide is already covered by durable state plus polling:

- config changes travel through `config_revision`;
- correlation work travels through `trace_correlation` and its watermark;
- backfill travels through `indexed_attribute.status`.

```java
package ru.aasmc.tracing.collector.domain.event;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public sealed interface DomainEvent permits
        TraceFragmentIngested, FragmentDeadLettered, SpansDropped,
        EnrichmentApplied, EnrichmentEventRejected,
        CorrelationObserved, BusinessOperationCreated, TraceJoinedBusinessOperation, BusinessOperationMerged,
        EnrichmentSourceActivated, EnrichmentSourcePaused, EnrichmentSourceRevised, EnrichmentSourceDeleted,
        IndexedAttributeRequested, IndexedAttributeActivated, IndexedAttributeDisabled,
        ConfigRevisionBumped, PartitionCreated, PartitionDropped,
        TraceDayCompacted, BusinessOperationCompacted, PartitionDropDeferred, RetentionTiersChanged {
    Instant occurredAt();
}

record TraceFragmentIngested(Instant occurredAt, String traceIdHex, java.time.LocalDate traceDay,
                             int insertedSpans, int duplicateSpans, int partition, long offset) implements DomainEvent {}
record FragmentDeadLettered(Instant occurredAt, String reason, int partition, long offset) implements DomainEvent {}
record SpansDropped(Instant occurredAt, String reason, int count) implements DomainEvent {}       // EXPIRED, TRACE_TOO_LARGE
record EnrichmentApplied(Instant occurredAt, long sourceId, int revision, String traceIdHex, List<String> keys) implements DomainEvent {}
record EnrichmentEventRejected(Instant occurredAt, long sourceId, String reason, boolean deadLettered) implements DomainEvent {}
record CorrelationObserved(Instant occurredAt, int typeId, int keyId, String traceIdHex, short origin) implements DomainEvent {}
record BusinessOperationCreated(Instant occurredAt, UUID operationId, int typeId, boolean provisional) implements DomainEvent {}
record TraceJoinedBusinessOperation(Instant occurredAt, UUID operationId, String traceIdHex) implements DomainEvent {}
record BusinessOperationMerged(Instant occurredAt, UUID survivorId, List<UUID> mergedIds, int traceCount) implements DomainEvent {}
record EnrichmentSourceActivated(Instant occurredAt, long sourceId, int revision, String actor) implements DomainEvent {}
record EnrichmentSourcePaused(Instant occurredAt, long sourceId, String actor) implements DomainEvent {}
record EnrichmentSourceRevised(Instant occurredAt, long sourceId, int newRevision, String actor) implements DomainEvent {}
record EnrichmentSourceDeleted(Instant occurredAt, long sourceId, String actor) implements DomainEvent {}
record IndexedAttributeRequested(Instant occurredAt, String key, String actor) implements DomainEvent {}
record IndexedAttributeActivated(Instant occurredAt, String key, long rowsBackfilled) implements DomainEvent {}
record IndexedAttributeDisabled(Instant occurredAt, String key, String actor) implements DomainEvent {}
record ConfigRevisionBumped(Instant occurredAt, long revision) implements DomainEvent {}
record PartitionCreated(Instant occurredAt, String table, String partition) implements DomainEvent {}
record PartitionDropped(Instant occurredAt, String table, String partition) implements DomainEvent {}
record TraceDayCompacted(Instant occurredAt, java.time.LocalDate traceDay, long tracesDigested, long tracesExcluded,
                         java.time.Duration elapsed) implements DomainEvent {}
record BusinessOperationCompacted(Instant occurredAt, UUID operationId, int typeId, short outcome,
                                  int traceCount) implements DomainEvent {}
record PartitionDropDeferred(Instant occurredAt, String table, String partition,
                             java.time.LocalDate blockingTraceDay) implements DomainEvent {}   // drop gate closed
record RetentionTiersChanged(Instant occurredAt, int fullDays, int digestDays, String actor) implements DomainEvent {}
```

| Event | Raised by (service, component) | Typical consumers |
|---|---|---|
| TraceFragmentIngested | `tc-ingester` SpanBatchWriter (after commit) | metrics `tc_ingester_spans_inserted_total`, `..._duplicates_total`; TraceDayResolver cache put |
| FragmentDeadLettered / SpansDropped | `tc-ingester` error handler / SpanNormalizer | metrics, alerting |
| EnrichmentApplied / EnrichmentEventRejected | `tc-enricher` EnrichmentWriter / extractors | per-source metrics `tc_enricher_records_total{source,outcome}` |
| CorrelationObserved | `tc-ingester` CorrelationExtractor, `tc-enricher` EnrichmentWriter | metrics only (the durable fact is the `trace_correlation` row) |
| BusinessOperationCreated / TraceJoined / Merged | `tc-maintenance` CorrelationResolver | metrics, audit log |
| EnrichmentSource* / IndexedAttribute* / ConfigRevisionBumped | `tc-control-plane` services | audit log (who/when/what); `tc-maintenance` AttributeBackfill (polls status, not the event) |
| PartitionCreated / Dropped | `tc-maintenance` PartitionManager | metrics `tc_partitions_ahead_hours`, audit |
| TraceDayCompacted | `tc-maintenance` DigestCompactor (after the last chunk of a day commits) | metrics `tc_compaction_traces_total{result=digested\|excluded}`, `tc_compaction_lag_days`; log |
| BusinessOperationCompacted | `tc-maintenance` DigestCompactor (operation phase) | metrics per type and outcome; audit not required |
| PartitionDropDeferred | `tc-maintenance` PartitionManager (drop gate closed) | metric `tc_compaction_lag_days`, alert when > 1 day; log naming the blocking trace day |
| RetentionTiersChanged | `tc-control-plane` (UC-23) | audit log; PartitionManager and DigestCompactor read the new values from `retention_policy` on their next run |

Audit events from the control plane are additionally written as structured JSON log lines (`logger tc.audit`) for the organisation's log pipeline. Who changed which source or which PII key is security-relevant.

---

## 6. Mapping: wire `TraceMessage` v1 to the domain and to tables

### 6.1 Message level

| Wire field | Domain | Table.column | Rule |
|---|---|---|---|
| `schemaVersion` | none (routing only) | — | `1` gives `TraceMessageParserV1`; anything else goes to the DLQ (`UNSUPPORTED_SCHEMA_VERSION`). **Dropped** after routing. |
| `traceId` | `TraceFragment.traceId` | `span.trace_id`, `trace_summary.trace_id`, ... (uuid) | Must equal the Kafka record key and every `spanContext.traceId`. A mismatch gives `TRACE_ID_MISMATCH`. |
| (Kafka key) | validated | — | 32 lowercase hex. |
| (Kafka partition, offset, timestamp) | `KafkaCoordinates` | not stored for spans | Used for logs, the DLQ and the batch-local `frag_no`. Span idempotency comes from the PK, not from the offset. |
| `spans[]` | `TraceFragment.spans` | `span` rows | — |

### 6.2 Span level

| Wire field | Domain | Table.column | Rule |
|---|---|---|---|
| `spanId` | `Span.spanId` | `span.span_id` bigint | 16 hex to signed long. |
| `parentSpanId` | `Span.parentSpanId` | `span.parent_span_id` | `"0000000000000000"` → NULL (root). |
| `name` | `Span.name` | `span.name_id` via `span_name(service_id, name)`; `span.name_overflow` | The 5 001st distinct name of a service gives `name_id NULL` and `name_overflow = name`. |
| `kind` | `SpanKind` | `span.kind` smallint | Enum name to OTLP code (INTERNAL 1 ... CONSUMER 5). |
| `startEpochNanos` (string) | `startEpochNanos` long | `span.start_time` timestamptz | `Long.parseLong`, truncated to µs. |
| `endEpochNanos` (string) | `durationNanos = end - start` | `span.duration_ns` | Exact ns. `end < start` gives 0 plus a flag. |
| `spanContext.traceId`, `.spanId` | validated only | — | **Dropped** (duplicates). |
| `spanContext.traceFlags` (signed byte) | `TraceFlags` | `span.trace_flags` smallint | `b & 0xFF`. |
| `spanContext.sampled` | — | — | **Dropped**, derived from `trace_flags & 1`. |
| `spanContext.valid` | validated | — | `false` gives `INVALID_SPAN_ID` (poison). |
| `spanContext.remote` | — | — | **Dropped** (always false for an exported span). |
| `spanContext.traceState` (map) | `Span.traceState` | `span.trace_state` text | Serialised as W3C `k=v,k2=v2` in map order. NULL if empty. |
| `parentSpanContext.*` | `SpanFlag.PARENT_REMOTE` | `span.flags` bit 0x04 | Only `remote` is kept, which tells whether the parent was in another process. The rest is **dropped**: it duplicates `parentSpanId`/`traceId`, and the context is invalid for roots. |
| `status.code` | `StatusCode` | `span.status_code` smallint | UNSET 0, OK 1, ERROR 2. ERROR adds `trace_attribute(status=ERROR, origin DERIVED)` once per trace per batch. |
| `status.description` | `SpanStatus.message` | `span.status_message` | `""` → NULL. |
| `attributes` | `Attributes` | `span.attributes` jsonb (compact) | Indexed keys go to `trace_attribute` (origin SPAN). Correlation keys go to `trace_correlation` (origin SPAN). Strings over 4 KiB are truncated and flagged with 0x01. LONG is parsed as long. |
| `events[]` | `SpanEvent` | `span.events` jsonb | `[{"name","time","attributes","dropped"}]` (04 §5.5): `time` is the epoch-nanos string, `dropped` appears only when > 0. More than 128 events are truncated with flag 0x08 (`EVENTS_OR_LINKS_TRUNCATED`). NULL if empty. |
| `events[].totalAttributeCount` | — | — | **Dropped** (equal to size + dropped). |
| `links[]` | `SpanLinkRef` | `span.links` jsonb **and** `span_link` rows | Each link gives a `span_link(trace_id, span_id, start_time, linked_trace_id, linked_span_id, attributes)` row, written only for inserted spans. |
| `links[].spanContext.{sampled,valid,remote,traceFlags}` | — | — | **Dropped**, except `traceState` (kept in the jsonb). |
| `hasEnded` | `SpanFlag.NOT_ENDED` | `span.flags` bit 0x02 when false | Normally true. |
| `totalAttributeCount`, `totalRecordedEvents`, `totalRecordedLinks` | — | — | **Dropped**, derivable as size + dropped. |
| `droppedAttributesCount`, `droppedEventsCount`, `droppedLinksCount` | `DroppedCounts` | `span.dropped_counts` jsonb | `{"attributes":n,"events":n,"links":n}`, **only if any > 0**, else NULL. |
| `resource.schemaUrl`, `resource.attributes` | `Resource` (content hash) | `resource(id, service_id, schema_url, attributes)`; `span.resource_id`; `span.service_id` | Deduplicated. `service.name` gives the `service` dictionary entry (missing gives `unknown_service`). Indexed RESOURCE-scoped keys go to `trace_attribute` (origin RESOURCE), once per trace per batch. |
| `instrumentationScope.*` | `InstrumentationScope` | `instrumentation_scope`; `span.scope_id` | Deduplicated by content hash. |

### 6.3 Derived data

| Derived item | From | Table |
|---|---|---|
| Trace summary | inserted spans | `trace_summary` |
| `trace_day` | TraceDayResolver | `trace_summary.trace_day` |
| `service.name` index | resource | `trace_attribute(key service.name, origin RESOURCE)` |
| `span.name` index | root span name | `trace_attribute(key span.name, origin DERIVED)` |
| `status` index | any span with ERROR | `trace_attribute(key status, value ERROR, origin DERIVED)` |
| New attribute keys | first sighting of a key that is indexed or correlated, or on API demand | `attribute_key(key, value_type, first_seen)` |

`attribute_key` is a dictionary of *known* keys. To keep it small, the ingester registers only keys it needs ids for: indexed keys, correlation keys, and keys of enrichment mappings. Autocomplete over the keys of non-indexed span attributes (UC-15) therefore offers indexed keys only. That is intentional: only those are searchable without brute force.

### 6.4 Full tier to digest tier (compaction, ADR-13)

Written by `DigestCompactor` (§4.14). Wire fields reach the digest only through the full-tier tables; compaction never reads `span`.

| Digest column | Source (full tier) | Rule |
|---|---|---|
| `trace_digest.trace_day`, `trace_id`, `start_time`, `duration_ns`, `root_service_id`, `root_name_id`, `span_count`, `error_count`, `service_ids` | `trace_summary` (same columns) | Copied 1:1 per `(trace_id, trace_day)` row of chunk `[h, h + 1 h)` of day D. |
| `trace_digest.outcome` | `trace_summary.error_count` | 1 = OK (`error_count = 0`), 2 = ERROR. |
| `trace_digest.business_operation_id` | `trace_correlation` → `correlation_value` → `business_operation` (following `merged_into` to the root) | NULL if the trace belongs to no operation. If it belongs to several types, the operation of the lowest `correlation_type_id` is stored (the others still list the trace in their `steps`). |
| `trace_digest.attributes` | `trace_attribute` rows of keys with `retain_in_digest = true`, `ts` in `[D − 1 h, D + 1 day + 6 h)` | `jsonb_object_agg(key, array of DISTINCT transformed value_str)`; transform per `digest_transform`. `'{}'` if none. |
| (filter) | `compaction_exclusion` vs root service / root name | Excluded unless `business_operation_id IS NOT NULL`. |
| `trace_digest.compacted_at` | `now()` | — |
| `business_operation_digest.operation_id`, `correlation_type_id`, `display_value`, `first_seen`, `last_seen`, `trace_count` | `business_operation` (root) | `op_day = first_seen::date` (UTC). |
| `business_operation_digest.outcome`, `outcome_detail` | `OutcomeRule.evaluate` over member `trace_digest` rows + `trace_attribute(outcomeKey)` + linking `correlation_key.step_label` | Order of §4.10.1. |
| `business_operation_digest.steps` | member `trace_digest` rows ordered by `start_time`, step label from `correlation_key` | `[{"stepLabel","traceId","startTime","durationMs","outcome"}]`. |
| `business_operation_digest.attributes` | union of members' `trace_digest.attributes` | jsonb union, distinct values per key. |
| — | `span`, `span_link`, `trace_enrichment` (lineage), `resource`, `instrumentation_scope`, non-retained keys | **Dropped** after n days. Enrichment values survive only if their key is retained (they are in `trace_attribute` with origin 3). |

---

## 7. Cross-cutting domain rules

### 7.1 Enrichment never mutates spans

- `tc.span` is insert-only, with fillfactor 100 and no UPDATE statements in the code base. The schema migrator grants `tc_enricher_role` no UPDATE on `span`.
- Enrichment is stored as `EnrichmentRecord` in `trace_enrichment` and joined at read time by `trace_id` (and `span_id` for SPAN scope).
- Order independence follows directly. An enrichment event that arrives **before** the spans is stored and becomes visible when the spans arrive (UC-06, SD-07). One that arrives **after** is visible immediately (SD-08). There is no pending queue and no expiry.
- Enrichment for a trace whose spans never arrive (e.g. dropped at the producer) remains an "orphan enrichment". `GET /traces/{id}` then returns `404 TRACE_NOT_FOUND` with `"enrichmentOnly": true`, so support staff can see that the event did arrive.

### 7.2 Attribute precedence when the same key comes from a span and from enrichment

TCS **does not resolve conflicts**. It keeps every value with its origin:

1. The span attribute stays in `span.attributes`, origin SPAN.
2. The enrichment value is a `trace_enrichment` row tagged with `source_id` and `source_revision`, origin ENRICHMENT.
3. If the key is indexed, both values appear in `trace_attribute`, with origins 1 and 3. Search with `EQ` matches either, unless the key's `origin_scope` restricts it.
4. The UI shows both. The span panel lists the span value. The "Enrichment" panel lists the enrichment value as `sms.provider = smsc-1 (sms-gateway.delivery-reports r3, 2026-11-21T09:15:09Z)`. When both exist, the trace header shows a "conflicting values" badge.
5. Several enrichment values for the same key and trace (e.g. `SENT` then `DELIVERED`) are all kept. The *current* value is the one with max `(observed_at, kafka_partition, kafka_offset)`. The API returns `current` plus `history`.

### 7.3 PII classification of keys

```java
package ru.aasmc.tracing.collector.domain.attribute;

public enum PiiClass {
    NONE,       // shown to every TC_VIEWER
    PERSONAL,   // shown only with scope TC_PII; masked otherwise ("u-**")
    SECRET      // always masked in UI/API, even with TC_PII; usable for search EQ and correlation only
}

public record PiiPolicy(java.util.Map<String, PiiClass> exact, java.util.List<java.util.Map.Entry<java.util.regex.Pattern, PiiClass>> patterns) {
    public PiiClass classify(AttributeKey key) {
        PiiClass c = exact.get(key.value());
        if (c != null) return c;
        for (var e : patterns) if (e.getKey().matcher(key.value()).matches()) return e.getValue();
        return PiiClass.NONE;
    }
    public static String mask(String v) {
        if (v.length() <= 2) return "**";
        return v.substring(0, Math.min(2, v.length() / 3)) + "**";
    }
}
```

- The classification is **configuration of the Query context**, not of the data model: property `tc.security.pii.keys` in `tc-query-api`. Masking is a presentation concern and the data stays raw, because correlation and search need it.
- Baseline:
  - `user_id`, `enduser.id`, `phone`, `*.phone`, `email`, `*.email` are `PERSONAL`;
  - `oauth.authorization_code`, `*.token`, `*.password`, `*.secret` are `SECRET`.
- Masking applies to every place a value is returned: span attributes, resource attributes, enrichment values, matched attributes in search results, correlation values and business-operation display values (when the PRIMARY key is PII).
- Viewers without `TC_PII` may still search a PERSONAL key with `EQ` (they already know the value). `PREFIX` and the value-autocomplete endpoint are forbidden for them (`403`), which prevents enumeration.
- **PII in the digest tier (ADR-13).** Retained values live n + m days (default 97) instead of n, so each retained PERSONAL or SECRET key should carry a `digest_transform`: `SHA256` keeps it joinable and searchable by `EQ`/`IN` (`tc-query-api` hashes the query value with `DigestTransform.apply`), `MASK` keeps only a recognisable prefix (`u-***`) and makes the key unsearchable in the digest. Masking by `PiiClass` still applies on read: a hashed PERSONAL value is shown only with `TC_PII` (otherwise `16**`). The control plane refuses a PERSONAL key retained with `NONE` unless the admin sets `acknowledgePii = true` (`PII_RETAINED_IN_CLEAR`), and always refuses a SECRET key retained with `NONE` (`SECRET_KEY_REQUIRES_TRANSFORM`) (UC-23).
- Producers are advised to avoid SECRET values entirely, or to hash them at the source. An enrichment source can apply the `SHA256` transform; a producer can use `@TraceAttribute(key = "oauth.authorization_code", value = "...hash expression...")`. Correlation works equally well on hashes, as long as every producer applies the same hash.

### 7.4 Other invariants worth stating once

- **No self-tracing loop (ADR-12).** TCS services set `management.tracing.export.otlp.enabled=false` and do not depend on the starter. Spans from a service named `tc-*` arriving on `otlp-traces` are dropped with metric reason `SELF_TRACE`.
- **Time.** All domain times are UTC `Instant`. Partition bounds are UTC. `trace_day` is a UTC date.
- **Ids in APIs** are always lowercase hex for traces and spans, and canonical UUID text for business operations.
- **Kafka is the write-ahead log (ADR-02).** No domain state exists only in memory past a commit. Caches (TraceDayResolver, DictionaryCache, ResourceDeduplicator, the config snapshot) can always be rebuilt from PostgreSQL.
