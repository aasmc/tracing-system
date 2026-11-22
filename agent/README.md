# Trace Collector System (TCS) — Design Documentation

This is the design produced for the assignment in [trace-collector-design-prompt.md](trace-collector-design-prompt.md). TCS is a backend that:

- collects the traces that `otlp-kafka-export-tracing-starter` publishes to Kafka topic `otlp-traces` (the starter's `TraceMessage` v1 JSON);
- stores them in PostgreSQL;
- enriches them from client-defined Kafka topics;
- links traces of one business operation together;
- serves search and trace views to a React UI.

## Documents (suggested reading order)

| # | Document | Contents |
|---|---|---|
| 01 | [Technology overview](01-technology-overview.md) | OpenTelemetry (data model, Collector, W3C propagation over Kafka), Grafana Tempo 3.x (Kafka-based "Rhythm" ingest, vParquet, TraceQL), Jaeger v2, ClickHouse-based backends (OTel exporter, SigNoz, Uptrace), Zipkin, Promscale (PostgreSQL precedent), prior in-house reports, comparison matrix, and the lessons TCS borrows |
| 02 | [High-level design](02-high-level-design.md) | Requirements, load profiles, C4 context and container diagrams, data flows, ADR-01…ADR-13, Kubernetes deployment, Kafka topics, capacity summary, reliability, security, self-observability |
| 03 | [Low-level design](03-low-level-design.md) | Per-service component design of `tc-ingester`, `tc-enricher`, `tc-query-api`, `tc-control-plane`, `tc-maintenance`, `tc-schema-migrator` and `tc-ui`: Java 25 / Spring Boot 4.1 sketches, configuration, SQL, error handling, Maven layout, testing |
| 04 | [Storage design](04-storage-design.md) | PostgreSQL 18 DDL as Liquibase changelogs, partitioning and retention, write and read SQL, capacity arithmetic for profiles S/M/L, `postgresql.conf`, benchmark plan, HA/backup, roles, storage alternatives (ClickHouse, Tempo hybrid, TimescaleDB, Citus) |
| 05 | [Domain model](05-domain-model.md) | Ubiquitous language, bounded contexts, aggregates and value objects (Java records), state machines, domain events, wire-to-domain-to-table mapping |
| 06 | [Use cases](06-use-cases.md) | UC-01…UC-23 (UC-21…UC-23: compaction, digest-tier queries, retention configuration) with the Identity Provider worked example, request/response samples, traceability matrix |
| 07 | [Sequence diagrams](07-sequence-diagrams.md) | SD-00…SD-23 for ingestion, replay, DLQ, back-pressure, enrichment (before and after spans), backfill, business-operation linking, queries, retention, security |
| 08 | [ER diagrams](08-er-diagrams.md) | Configuration, dictionaries, trace data, enrichment, correlation and operational ER diagrams, with partitioning and retention per entity |
| 09 | [Implementation plan](09-implementation-plan.md) | Test-driven, layer-by-layer plan with checklist: foundation → domain → data → application → messaging → REST API → UI → E2E/performance; plain JUnit for the framework-free domain, full-context integration tests for every other layer, Testcontainers PostgreSQL 18.7 and Kafka, WireMock for OIDC and Vault; use case → test traceability |

## Design at a glance

- **Write path.** `otlp-traces` (key = traceId, so all fragments of a trace land in the same partition) → `tc-ingester` (Spring Kafka batch listener) → a single PostgreSQL transaction per poll batch → the offsets are committed. Kafka acts as the write-ahead log, as in Tempo 3.x. Idempotent writes make delivery effectively-once.
- **No Kafka Streams, no topic creation.** Plain consumers and producers are used. The only topics are `otlp-traces`, `otlp-traces.dlq` and `tc-enrichment.dlq`, all pre-created by administrators.
- **Storage.**
  - PostgreSQL 18 with time-partitioned tables: `span` and `trace_attribute` hourly; `trace_summary`, enrichment, correlation, link and digest tables daily.
  - Retention works by dropping partitions, in two tiers: full detail for n days (default 7), then `trace_digest` / `business_operation_digest` for m more days (default 90). Partitions are never dropped before they are compacted.
  - Resources and dictionaries are deduplicated.
  - Ids are binary (`uuid` trace id, `bigint` span id).
- **Search.**
  - Registered "indexed attributes" (like Tempo's dedicated columns) feed the inverted index `trace_attribute`. This serves search by user_id, client_id, auth factor, service, and so on.
  - Other keys can be searched with a bounded brute-force scan.
  - Trace-by-id goes through `trace_summary`, then the span partitions in that time range.
- **Enrichment.**
  - Enrichment sources are data, not code: a Kafka connection, a topic, a trace-context extraction strategy (default W3C `traceparent` header) and JSON Pointer field mappings.
  - `tc-enricher` starts and stops listener containers at runtime.
  - Enrichments are append-only additions joined at read time, so they may arrive before or after the spans.
- **Linking traces.** Business operation definitions declare a PRIMARY key and ALIAS keys (e.g. `idp.auth_session_id` and `oauth.authorization_code`). An asynchronous union-find resolver merges traces into a `business_operation`. Native OTel span links are indexed in both directions.
- **Services.** `tc-ingester`, `tc-enricher`, `tc-query-api`, `tc-control-plane`, `tc-maintenance`, `tc-schema-migrator` (Java 25, Spring Boot 4.1.3), plus `tc-ui` (React 19 + TypeScript).

## Baseline capacity (profile M, sampling 1.0) — see 04 §9

- **Load:**
  - 1 000 traces/s on average and 3 000/s at peak.
  - 20 spans per trace, which gives 20 000 spans/s on average and 60 000/s at peak.
  - 86.4 M traces and 1.73 B spans per day.
- **Two-tier retention (ADR-13):**
  - Full detail is kept `fullDays` = 7. Then each trace and business operation is compacted into a digest kept `digestDays` = 90 more. The digest holds outcome, business operation and retained custom attributes.
  - Both values are configurable at runtime (`PUT /api/v1/admin/retention-policies/tiers`).
- **PostgreSQL:**
  - 32 vCPU and 128 GB RAM for the primary, plus one synchronous replica (Patroni).
  - FULL tier ≈ 9.6 TB (≈ 8 effective days × 1.2 TB). DIGEST tier ≈ 6.1 TB (789 B/trace, ≈ 68 GB/day).
  - **≈ 19.7 TB per node with 25 % headroom** (17.3 TB if inline lz4 is confirmed), down from 23.5 TB. With `digestDays` = 30 it is ≈ 14.6 TB.
  - Recommendation: **20 TB of usable NVMe per node**.
  - WAL is about 2.1–2.2 TB per day.
- **Kafka:**
  - `otlp-traces` has 24 partitions and 3-day retention.
  - With zstd it needs about 1 TB/day across the cluster at RF 3.
- **Scale limits:** profile S needs ≈ 2 TB; profile L needs ≈ 99 TB. At profile L the ClickHouse or Tempo alternatives in 04 §14 are recommended.

All sizing figures are **assumptions** that must be validated with the benchmark plan in 04 §11.

## Notes

- The design supersedes the two earlier Russian-language reports (originally in `opentelemetry-demo/reports/`; at the time of writing they were located under `~/Desktop/SberID/opentelemetry-demo/reports/`). Their differences are summarised in 01 §7 and 02.
- Versions were verified on 2026-11-21: Spring Boot 4.1.3 (4.2.0 is also GA), Spring Kafka 4.1.3, PostgreSQL 18.7 (19.0 is too new), Grafana Tempo v3.1.1, OTel Collector v0.165, Jaeger v2.23.
