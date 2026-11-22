# 07 — Sequence Diagrams of the Trace Collector System (TCS)

**Related documents**

- [01 — Technology overview (Tempo, OpenTelemetry, Jaeger, ...)](01-technology-overview.md)
- [02 — High-level design (C4, ADRs)](02-high-level-design.md)
- [03 — Low-level design of the services](03-low-level-design.md)
- [04 — Storage design (PostgreSQL, capacity)](04-storage-design.md)
- [05 — Domain model](05-domain-model.md)
- [06 — Use cases](06-use-cases.md)
- **07 — Sequence diagrams (this document)**
- [08 — ER diagrams](08-er-diagrams.md)

Every diagram uses the canonical service, component, table, topic and endpoint names from the design brief and from 05/06. Diagram ids (SD-xx) are referenced from the use-case specifications in 06.

Conventions:

- `PG` is the PostgreSQL 18 **primary**, reached through PgBouncer in transaction mode unless stated otherwise.
- `PGR` is the streaming **read replica** used by `tc-query-api`.
- Kafka topics are participants named after the topic.
- Table names in messages are in schema `tc`.
- `autonumber` gives step numbers that match the numbered main scenarios in 06 as closely as practical.

| Id | Use case | Title |
|---|---|---|
| SD-00 | context | Producer path inside a microservice |
| SD-01 | UC-01 | Ingest one poll batch |
| SD-02 | UC-02 | Replay after a crash between PG commit and offset commit |
| SD-03 | UC-03 | Poison record to `otlp-traces.dlq` |
| SD-04 | UC-17 | PostgreSQL outage and back-pressure |
| SD-05 | UC-04 | Register connection and source with preview and dry-run |
| SD-06 | UC-05 | Source lifecycle to container start-stop |
| SD-07 | UC-06 | Enrichment event arrives before the spans |
| SD-08 | UC-06 | Enrichment event arrives after the spans |
| SD-09 | UC-07 | Define indexed attribute and backfill |
| SD-10 | UC-09 | IdP login T1, T2, T3 linked into one business operation |
| SD-11 | UC-10 | Alias arrives first and operations are merged |
| SD-12 | UC-11 | Get trace by id |
| SD-13 | UC-12 | Search traces |
| SD-14 | UC-13 | Bounded brute-force search |
| SD-15 | UC-14 | Business-operation timeline from the UI |
| SD-16 | UC-16 | Partition create and drop |
| SD-17 | UC-19 | UI login via OIDC and PII masking |
| SD-18 | UC-20 | Related traces |
| SD-19 | UC-18 | Replay from Kafka and DLQ re-drive |
| SD-20 | UC-15 | Autocomplete |
| SD-21 | UC-21 | Compaction of one trace day into the digest tier |
| SD-22 | UC-22 (UC-11, UC-12, UC-14) | Cross-tier search and trace-by-id fallback |
| SD-23 | UC-23 (UC-07, UC-08) | Configure retention tiers, retained attribute and outcome rule |

---

## SD-00 — Producer path inside a microservice (context)

```mermaid
sequenceDiagram
    autonumber
    participant APP as idp-auth-service<br/>controller and service code
    participant ASP as TraceAttributeAspect
    participant SPAN as OTel Span (SdkSpan)
    participant BSP as BatchSpanProcessor
    participant EXP as KafkaSpanExporter
    participant MAP as TraceMessageMapper
    participant KT as otlpKafkaTemplate
    participant K as Kafka topic otlp-traces

    APP->>SPAN: Observation / tracer starts server span POST /login/first-factor
    APP->>ASP: call method annotated @TraceAttribute(key = user_id, value = result.userId)
    ASP->>ASP: proceed(), evaluate SpEL on non-null result
    ASP->>SPAN: Span.current().setAttribute("user_id", "u-42")
    Note over APP,SPAN: other attributes: client_id, auth.factor, idp.auth_session_id<br/>(set directly or via baggage tag-fields)
    APP->>SPAN: span.end()
    SPAN->>BSP: onEnd(ReadableSpan) - enqueue (queue 2048)
    alt queue full
        BSP-->>BSP: span dropped (counted by SDK metric) - outside TCS guarantees
    end
    loop every 5 s or when 512 spans queued
        BSP->>EXP: export(Collection of SpanData, max 512)
        EXP->>EXP: group spans by traceId (LinkedHashMap)
        loop for each traceId group
            EXP->>MAP: map(traceId, spans)
            MAP-->>EXP: TraceMessage(schemaVersion=1, traceId, spans with full resource each)
            EXP->>KT: send(topic, key=traceId, value=TraceMessage)
            KT->>K: produce (idempotent, acks=all, murmur2(key) selects partition)
            alt broker ack
                K-->>KT: RecordMetadata(partition, offset)
                KT-->>EXP: future completes, result.succeed()
            else send fails after retries
                KT-->>EXP: exception, result.failExceptionally()
                Note right of EXP: spans of this group are lost (no resend)<br/>accepted producer-side loss
            end
        end
        EXP-->>BSP: CompletableResultCode.ofAll(results)
    end
```

This diagram shows how a single business request becomes one or more fragments on `otlp-traces`. Two consequences shape the whole TCS design:

- **Fragmentation.** Each 5-second batch of each service instance yields one record per trace, so a trace arrives as many partial messages.
- **Co-partitioning.** key = traceId sends all fragments of a trace to one partition, and therefore to one ingester thread.

The recommendation for producers (02) is to set `management.otlp.kafka.producer.compression-type=zstd`.

---

## SD-01 — UC-01 Ingest one poll batch

```mermaid
sequenceDiagram
    autonumber
    participant K as otlp-traces (partition p)
    participant L as TraceBatchListener<br/>ingest.kafka
    participant P as TraceMessageParser<br/>ingest.parse
    participant N as SpanNormalizer<br/>ingest.normalize
    participant RD as ResourceDeduplicator<br/>+ DictionaryCache
    participant TDR as TraceDayResolver<br/>ingest.trace
    participant IX as AttributeIndexer<br/>+ CorrelationExtractor
    participant W as SpanBatchWriter<br/>ingest.persist
    participant PG as PostgreSQL primary<br/>(via PgBouncer)

    K->>L: poll() returns up to 500 records (byte[] values)
    loop each record
        L->>P: parse(key, bytes)
        P->>P: read schemaVersion first (must be 1)
        P->>P: check traceId == record key, stream spans[]<br/>LONG via getLongValue(), nanos via Long.parseLong
        P-->>L: TraceMessageV1 (typed)
        L->>N: normalize(message, coordinates)
        N->>N: ids to uuid / bigint, "0000000000000000" parent to NULL
        N->>N: kind/status to codes, truncate > 4 KiB (flag 0x01)
        N->>N: drop spans older than span retention (metric EXPIRED)
        N-->>L: TraceFragment
    end
    L->>RD: resolve resources, scopes, services, span names
    RD->>RD: xxHash64(canonical resource), Caffeine lookup
    opt unseen ids or names
        RD->>PG: INSERT resource / instrumentation_scope / service / span_name / attribute_key ON CONFLICT DO NOTHING RETURNING
        PG-->>RD: ids (re-select on conflict)
    end
    L->>TDR: traceDay(fragment) for each fragment
    alt cache hit
        TDR-->>L: cached trace_day
    else cache miss
        TDR->>PG: SELECT trace_day FROM trace_summary WHERE trace_id=? AND trace_day IN (d-1, d, d+1)
        alt row found
            PG-->>TDR: existing trace_day
        else not found
            TDR->>TDR: UTC date of min(span start) of fragment
        end
    end
    L->>IX: candidate rows for indexed keys and correlation keys
    IX->>IX: ConfigSnapshot lookup, built-ins service.name, root span.name, status=ERROR
    IX->>IX: dedupe per (trace, key, value) in memory
    IX-->>L: attributeRows, correlationRows, linkRows
    L->>W: write(batch)
    W->>PG: BEGIN
    W->>PG: CREATE TEMP TABLE IF NOT EXISTS span_stage (...) ON COMMIT DELETE ROWS
    W->>PG: COPY span_stage FROM STDIN (FORMAT binary) - all spans + trace_day + frag_no
    W->>PG: writable CTE: INSERT INTO span SELECT FROM span_stage ON CONFLICT DO NOTHING RETURNING ...
    Note over W,PG: same statement: INSERT INTO trace_summary SELECT ... FROM inserted<br/>GROUP BY trace_id, trace_day ON CONFLICT (trace_id, trace_day) DO UPDATE<br/>LEAST/GREATEST/sum/union (merge semantics, 05 section 4.4)<br/>inserted keys also saved to temp table inserted_spans
    W->>PG: INSERT INTO trace_attribute SELECT ... JOIN inserted_spans
    W->>PG: INSERT INTO span_link SELECT ... JOIN inserted_spans
    W->>PG: INSERT INTO trace_correlation SELECT ... JOIN inserted_spans
    W->>PG: COMMIT
    PG-->>W: ok
    W-->>L: result(inserted, duplicates)
    L-->>K: listener returns, container commits offsets (AckMode BATCH)
    L->>TDR: after commit put traceId to trace_day in cache
```

**Explanation.** The batch is processed in two phases.

The first phase is CPU only: parse, normalize, deduplicate and index. It runs entirely in memory and touches the DB only for dictionary misses and summary-day lookups.

The second phase is a single transaction:

1. `COPY` into a per-connection temp table (`ON COMMIT DELETE ROWS`, so it is empty at the start of every transaction). COPY has no `ON CONFLICT`, hence the staging table.
2. One writable CTE inserts spans with `ON CONFLICT DO NOTHING` and upserts `trace_summary` **only from the rows it actually inserted**.
3. Side tables are filled from the same inserted set.

This is what makes replays harmless (SD-02). Offsets are committed only after `COMMIT` (ADR-02).

**Note.** PgBouncer in transaction mode does not support temp tables declared `ON COMMIT PRESERVE ROWS` or `ON COMMIT DELETE ROWS`, because they outlive a transaction on a pooled server connection. `tc-ingester` therefore uses a dedicated **session-mode** PgBouncer pool `tc_ingest`, and each physical connection reuses its `ON COMMIT DELETE ROWS` staging tables. That avoids creating and dropping catalogue entries on every batch. Creating the tables per transaction with `ON COMMIT DROP` works under transaction pooling, but it is the not-recommended fallback because of the catalogue churn (see 04).

---

## SD-02 — UC-02 Replay after a crash between PG commit and offset commit

```mermaid
sequenceDiagram
    autonumber
    participant K as otlp-traces partition 7
    participant GC as Kafka group coordinator<br/>group tc-ingester
    participant I1 as tc-ingester-1
    participant I2 as tc-ingester-2
    participant PG as PostgreSQL primary

    K->>I1: records offsets 1000..1499
    I1->>PG: BEGIN, COPY, writable CTE (inserts 9 812 spans), side tables
    I1->>PG: COMMIT
    PG-->>I1: ok
    Note over I1: process killed (OOM / node loss)<br/>BEFORE offset commit
    I1--xGC: commitSync(1500) never sent
    GC->>GC: session.timeout.ms (45 s) expires, rebalance
    GC->>I2: assign partition 7, committed offset = 1000
    K->>I2: records offsets 1000..1499 again
    I2->>I2: parse + normalize (identical spans)
    I2->>PG: SELECT trace_day FROM trace_summary WHERE trace_id=? AND trace_day IN (d-1, d, d+1)
    PG-->>I2: same trace_day as before (row exists)
    I2->>PG: BEGIN, COPY span_stage
    I2->>PG: INSERT INTO span ... ON CONFLICT DO NOTHING RETURNING
    PG-->>I2: 0 rows returned
    Note over I2,PG: trace_summary upsert gets empty input - counters unchanged<br/>trace_attribute, span_link, trace_correlation join inserted_spans - nothing written
    I2->>PG: COMMIT
    I2->>GC: commit offset 1500
    I2->>I2: metric tc_ingester_spans_duplicate_total += 9 812
```

**Explanation.** At-least-once delivery combined with idempotent writes gives an effectively-once effect.

The key property is that **every derived write depends on the RETURNING set of the span insert**. A replayed record therefore leaves no trace in counters, indexes or correlations. The same mechanism covers deliberate replays (UC-18) and producer-side duplicates.

---

## SD-03 — UC-03 Poison record to otlp-traces.dlq

```mermaid
sequenceDiagram
    autonumber
    participant K as otlp-traces partition 3
    participant L as TraceBatchListener
    participant P as TraceMessageParser
    participant W as SpanBatchWriter
    participant PG as PostgreSQL primary
    participant EH as DefaultErrorHandler
    participant R as DeadLetterPublishingRecoverer<br/>custom destination resolver
    participant DLQ as otlp-traces.dlq (6 partitions)

    K->>L: records offsets 88400..88899 (index 0..499)
    L->>P: parse records 0..11
    P-->>L: ok
    L->>P: parse record 12 (offset 88412)
    P--xL: TraceMessageParseException(MALFORMED_JSON)
    Note over L: records 0..11 are valid and MUST be durable<br/>before the error handler commits their offsets
    L->>W: write(records 0..11)
    W->>PG: BEGIN ... COMMIT (as SD-01)
    PG-->>W: ok
    L--xEH: throw BatchListenerFailedException("poison", cause, index=12)
    EH->>K: commit offset 88412 (records before failed index)
    EH->>EH: cause is not retryable (addNotRetryableExceptions)
    EH->>R: accept(record 12, exception)
    R->>R: resolver returns TopicPartition(otlp-traces.dlq, -1)
    R->>DLQ: send(original key bytes, original value bytes)<br/>headers kafka_dlt-original-topic/partition/offset/timestamp,<br/>kafka_dlt-exception-fqcn, kafka_dlt-exception-message (<= 1 KiB),<br/>tc-error-reason=MALFORMED_JSON
    alt DLQ ack
        DLQ-->>R: ack
        R-->>EH: recovered
        EH->>K: commit offset 88413
        EH->>K: seek to 88413 for remaining records
        K->>L: records 88413..88899 re-polled, normal SD-01 path
    else DLQ unavailable
        R--xEH: send failed
        EH->>EH: back-off and retry recovery, offset 88412 NOT committed
    end
```

**Explanation.** The listener persists the valid prefix before throwing. That step is essential, because `DefaultErrorHandler` commits the offsets of the records before the failed index.

The DLQ has fewer partitions (6) than the source (24), so the destination resolver must return partition `-1`. Spring's default would use the same partition number and point at a non-existent partition, and its default name `otlp-traces-dlt` is not a pre-created topic.

---

## SD-04 — UC-17 PostgreSQL outage and back-pressure

```mermaid
sequenceDiagram
    autonumber
    participant K as otlp-traces
    participant C as Listener container<br/>(consumer thread)
    participant L as TraceBatchListener
    participant W as SpanBatchWriter
    participant PG as PostgreSQL primary (Patroni)
    participant EH as DefaultErrorHandler<br/>ExponentialBackOff 1 s x2, max 60 s, unlimited
    participant MON as Prometheus / Alertmanager

    K->>C: poll batch B (offsets n..n+499)
    C->>L: onMessage(B)
    L->>W: write(B)
    W->>PG: getConnection / BEGIN
    PG--xW: connection refused (primary down)
    W--xL: DataAccessResourceFailureException
    L--xC: exception (not BatchListenerFailedException)
    C->>EH: handleBatch(B)
    loop until PostgreSQL is back
        EH->>C: pause consumer for back-off interval (1, 2, 4, ... 60 s)
        C->>K: poll() while paused (heartbeats only, no records, stays in group)
        EH->>L: retry batch B from memory
        L->>W: write(B)
        W--xL: still failing
        C->>MON: kafka lag grows, readiness DOWN (liveness UP)
        MON->>MON: alert TcIngesterLagHigh after 5 min
    end
    PG->>PG: Patroni promotes replica (about 30 s) or primary restarts
    EH->>L: retry batch B
    L->>W: write(B)
    W->>PG: BEGIN, COPY, CTE, COMMIT
    PG-->>W: ok
    C->>K: commit offsets n+500
    loop catch-up
        K->>C: next batches at max rate (target 60 000 spans/s)
        C->>L: process, commit
    end
    Note over K,PG: net drain about 40 000 spans/s at avg load<br/>1 h backlog (72 M spans) drained in about 30 min
    opt async failover lost last commits
        MON->>MON: runbook: rewind tc-ingester offsets by 5 min (UC-18), idempotent
    end
```

**Explanation.** DB errors never go to the DLQ and never skip data. The container stays assigned and paused, Kafka buffers (3-day retention), and producers are unaffected.

The only residual risk is an asynchronous Patroni failover that loses commits whose offsets were already committed. The runbook closes that gap with a rewind, which is safe because of SD-02.

---

## SD-05 — UC-04 Register connection and source with preview and dry-run

```mermaid
sequenceDiagram
    autonumber
    actor ADM as TC Admin
    participant UI as tc-ui
    participant CP as tc-control-plane<br/>/api/v1/admin
    participant SR as SecretResolver
    participant V as Vault / k8s secrets
    participant CK as Client Kafka sms-kafka
    participant PG as PostgreSQL primary
    participant PGR as PostgreSQL replica

    ADM->>UI: New Kafka connection form
    UI->>CP: POST /kafka-connections (name sms-kafka, SASL_SSL, SCRAM_SHA_512, secret refs)
    CP->>CP: validate KafkaConnection invariants (forbidden props, SASL needs refs)
    CP->>SR: resolve(credentialsSecretRef, truststoreSecretRef)
    SR->>V: read secret
    V-->>SR: JAAS config, truststore bytes
    CP->>CK: AdminClient.describeCluster() (timeout 10 s)
    CK-->>CP: clusterId, 3 brokers
    CP->>PG: BEGIN, INSERT kafka_connection, UPDATE config_revision +1, COMMIT
    CP-->>UI: 201 id=3, connectivity ok
    ADM->>UI: New enrichment source (definition JSON)
    UI->>CP: POST /enrichment-sources (name, connectionId 3, topic, definition)
    CP->>CP: validate SourceDefinition (mappings, pointers, SPAN needs TRACEPARENT_HEADER)
    CP->>CK: AdminClient.describeTopics(sms-gateway.delivery-reports)
    alt topic missing or DESCRIBE denied
        CK-->>CP: UnknownTopicOrPartition / TopicAuthorizationException
        CP-->>UI: 422 TOPIC_NOT_ACCESSIBLE (TCS never creates topics)
    else topic exists
        CK-->>CP: 6 partitions
        CP->>PG: BEGIN
        CP->>PG: INSERT attribute_key ON CONFLICT DO NOTHING (target keys)
        CP->>PG: INSERT enrichment_source (DRAFT, current_revision 1)
        CP->>PG: INSERT enrichment_source_revision (12, 1, definition)
        CP->>PG: UPDATE config_revision SET revision = revision + 1
        CP->>PG: COMMIT
        CP-->>UI: 201 id=12, DRAFT, consumerGroup tc-enricher.sms-gateway.delivery-reports
    end
    ADM->>UI: Preview
    UI->>CP: POST /enrichment-sources/12/preview?limit=5
    CP->>CK: new KafkaConsumer WITHOUT group.id, auto commit off, allow.auto.create.topics=false
    CP->>CK: assign(all partitions), endOffsets, seek(end - 5)
    CK-->>CP: last records with headers
    loop each record
        CP->>CP: TraceContextExtractor + FieldExtractor (same code as tc-enricher, no writes)
        CP->>PGR: SELECT 1 FROM trace_summary WHERE trace_id=? AND trace_day IN (today, yesterday)
        PGR-->>CP: traceFound true/false
    end
    CP->>CP: PiiMaskingPolicy on payload fields
    CP-->>UI: 200 records with traceContext, attributes, warnings, summary
    CP->>CK: consumer.close() (no offsets committed anywhere)
    ADM->>UI: Dry-run with sample payload + headers
    UI->>CP: POST /enrichment-sources/12/test
    CP->>CP: decode, filter, extract, transform, coerce types
    CP-->>UI: 200 outcome ENRICH / DLQ / DROP, attributes, errors, wouldWrite counts
```

**Explanation.** Every step that touches the client's cluster is read-only and group-less: describe the cluster, describe topics, and assign plus seek. Nothing in TCS can create a topic or a consumer group there before activation.

Preview and dry-run reuse the exact extraction classes of `tc-enricher` (from `tc-kafka-support`), so what the admin sees is what will be stored.

---

## SD-06 — UC-05 Source lifecycle to container start-stop

```mermaid
sequenceDiagram
    autonumber
    actor ADM as TC Admin
    participant CP as tc-control-plane
    participant PG as PostgreSQL primary
    participant REG as EnrichmentSourceRegistry<br/>enrich.registry (each tc-enricher)
    participant DCM as DynamicContainerManager<br/>enrich.kafka
    participant CF as DefaultKafkaConsumerFactory<br/>per kafka_connection
    participant C12 as Container enrichment-source-12-r1
    participant CK as Client Kafka sms-kafka

    ADM->>CP: POST /enrichment-sources/12/activate (If-Match updated_at)
    CP->>CK: re-check describeTopics + describeCluster
    CP->>PG: BEGIN
    CP->>PG: UPDATE enrichment_source SET status='ACTIVE' WHERE id=12 AND status IN ('DRAFT','PAUSED')
    CP->>PG: UPDATE config_revision SET revision = revision + 1
    CP->>PG: COMMIT
    CP->>CP: EnrichmentSourceActivated, audit log
    CP-->>ADM: 200 ACTIVE
    loop every 10 s
        REG->>PG: SELECT revision FROM config_revision WHERE id = 1
    end
    PG-->>REG: 418 (changed from 417)
    REG->>PG: load snapshot: ACTIVE sources + current revisions + connections + indexed keys + correlation keys
    REG->>REG: diff desired 12 to r1 vs running none, result START 12/r1
    REG->>DCM: start(source 12, revision 1)
    DCM->>CF: get or create for connection 3 (SecretResolver for JAAS / truststore)
    DCM->>C12: createContainer(topic), group.id tc-enricher.sms-gateway.delivery-reports,<br/>auto.offset.reset latest, batch listener, DLQ recoverer to tc-enrichment.dlq
    DCM->>C12: start()
    C12->>CK: join group, partitions assigned
    Note over REG,C12: later: PUT /enrichment-sources/12 creates revision 2
    REG->>REG: diff gives RESTART 12: r1 to r2
    REG->>DCM: restart(12, r2)
    DCM->>C12: stop() - waits for in-flight batch commit
    DCM->>DCM: unregister if registry used (unregister alone does NOT stop)
    DCM->>DCM: start enrichment-source-12-r2 (same group, continues from committed offsets)
    Note over REG,C12: later: POST /enrichment-sources/12/pause
    REG->>REG: diff gives STOP 12
    REG->>DCM: stop(12)
    DCM->>C12: stop(), offsets stay committed in client cluster
    alt container start fails (auth error, topic deleted)
        DCM-->>REG: FAILED(cause)
        REG->>REG: retry with back-off 1 to 10 min, gauge tc_enricher_source_state FAILED, alert
    end
```

**Explanation.** The control plane writes desired state and bumps `config_revision` in the same transaction. Each enricher instance converges to that state by polling and diffing, which is level-triggered.

Container ids include the revision, so a definition change is a clean stop and start, while the consumer group (named after the source) keeps the offsets. UC-08 uses the same "write config, bump `config_revision`, poll, reload snapshot" pattern for `tc-ingester` (`ConfigSnapshotPoller`).

---

## SD-07 — UC-06 Enrichment event arrives before the spans

```mermaid
sequenceDiagram
    autonumber
    participant SMS as sms-gateway (Enrichment Producer)
    participant CK as sms-gateway.delivery-reports<br/>(client cluster)
    participant C as Container enrichment-source-12-r1
    participant EX as TraceContextExtractor<br/>+ FieldExtractor
    participant EW as EnrichmentWriter<br/>enrich.persist
    participant PG as PostgreSQL primary
    participant BSP as idp-otp-service BatchSpanProcessor
    participant K as otlp-traces
    participant ING as tc-ingester

    Note over BSP: T2 spans still buffered in producer (up to 5 s)
    SMS->>CK: event messageId m-1 status DELIVERED provider smsc-1<br/>header traceparent 00-7a3c...7c8d-5f3a9b2c1d0e4f68-01
    CK->>C: batch with the record (partition 2, offset 551023, ts 09:15:32.455Z)
    C->>EX: decode JSON, apply filter /status present
    EX->>EX: traceparent: version 00, traceId T2, spanId 5f3a9b2c1d0e4f68
    EX->>EX: mappings: sms.delivery.status=DELIVERED, sms.provider=smsc-1, sms.message_id=m-1
    EX-->>C: EnrichmentRecords (SPAN scope)
    C->>EW: write(batch)
    EW->>PG: BEGIN
    EW->>PG: INSERT trace_enrichment (observed_at 09:15:32.455Z, trace_id T2, span_id, source 12, rev 1, key_id, value, partition 2, offset 551023)<br/>ON CONFLICT (source_id, kafka_partition, kafka_offset, key_id, observed_at) DO NOTHING RETURNING
    EW->>PG: INSERT trace_attribute (ts=observed_at, sms.delivery.status, DELIVERED, T2, origin 3) for returned indexed keys
    EW->>PG: COMMIT
    C->>CK: commit offset 551024 (group tc-enricher.sms-gateway.delivery-reports)
    Note over PG: no span, no trace_summary for T2 yet - nothing waits, nothing expires
    BSP->>K: T2 fragment (5 s schedule)
    K->>ING: poll
    ING->>PG: spans + trace_summary for T2 (SD-01)
    Note over PG: GET /traces/T2 now joins trace_enrichment by trace_id<br/>search sms.delivery.status=DELIVERED now finds T2 (joins trace_summary)
```

**Explanation.** Because enrichment rows are keyed only by `trace_id` (plus `span_id` for SPAN scope) and joined at read time, arrival order does not matter. No pending queue, no timer and no state store are needed. This is the main simplification over a Kafka Streams join (ADR-07).

---

## SD-08 — UC-06 Enrichment event arrives after the spans

```mermaid
sequenceDiagram
    autonumber
    participant K as otlp-traces
    participant ING as tc-ingester
    participant PG as PostgreSQL primary
    participant CK as sms-gateway.delivery-reports
    participant C as Container enrichment-source-12-r1
    participant EW as EnrichmentWriter
    participant PGR as PostgreSQL replica
    participant QA as tc-query-api

    K->>ING: T2 fragments
    ING->>PG: span, trace_summary, trace_attribute for T2 (SD-01)
    CK->>C: delivery report for T2 (late, e.g. operator delay 40 s)
    C->>EW: EnrichmentRecords
    EW->>PG: INSERT trace_enrichment ... ON CONFLICT DO NOTHING RETURNING
    EW->>PG: INSERT trace_attribute (origin 3) for indexed keys
    EW->>PG: COMMIT
    Note over PG: span and trace_summary rows are NOT touched (immutability)
    PG-->>PGR: WAL streaming (lag < 1 s)
    QA->>PGR: GET /traces/T2 - reads span + trace_enrichment
    PGR-->>QA: spans and enrichment rows
    QA->>QA: attach sms.* to span 5f3a9b2c1d0e4f68 (SPAN scope)
    opt same key also present as span attribute
        QA->>QA: keep both, origin SPAN and ENRICHMENT, flag conflictingValues
    end
    opt replay of the same client record
        C->>EW: same record again
        EW->>PG: INSERT ... ON CONFLICT DO NOTHING returns 0 rows, no trace_attribute rows
    end
```

**Explanation.** A late event is visible on the next read. The idempotency key on `trace_enrichment` makes client-side replays (rebalance, offset reset) harmless, and the side writes to `trace_attribute` and `trace_correlation` are driven by `RETURNING`, just like on the span path.

---

## SD-09 — UC-07 Define indexed attribute and backfill

```mermaid
sequenceDiagram
    autonumber
    actor ADM as TC Admin
    participant CP as tc-control-plane
    participant PG as PostgreSQL primary
    participant ING as tc-ingester<br/>ConfigSnapshotPoller
    participant BF as AttributeBackfill<br/>tc-maintenance (ShedLock)
    participant PGR as PostgreSQL replica
    participant QA as tc-query-api

    ADM->>CP: POST /indexed-attributes key auth.factor, originScope SPAN, backfillWindow P1D
    CP->>PG: BEGIN, INSERT attribute_key ON CONFLICT DO NOTHING
    CP->>PG: INSERT indexed_attribute (status BACKFILLING, indexed_since now)
    CP->>PG: UPDATE config_revision +1, COMMIT
    CP-->>ADM: 201 BACKFILLING
    ING->>PG: poll config_revision (10 s), reload snapshot
    Note over ING: from now on new spans with auth.factor produce trace_attribute rows
    loop every minute while key is BACKFILLING
        BF->>PG: lock attribute-backfill in shedlock
        BF->>PG: read indexed_attribute BACKFILLING keys + backfill_cursor
        loop hourly span partitions from indexed_since+30s back to indexed_since-1d (newest first)
            BF->>PGR: SELECT trace_id, start_time, attributes->'auth.factor' FROM span_pYYYYMMDDHH WHERE attributes ? 'auth.factor'
            PGR-->>BF: rows (streamed, fetchSize 10 000)
            BF->>BF: dedupe per (trace, value), throttle 100 MB/s scanned
            BF->>PG: COPY into trace_attribute (origin 1) in 50 000-row transactions
            BF->>PG: UPDATE indexed_attribute SET backfill_cursor = partition lower bound
        end
        BF->>PG: UPDATE indexed_attribute SET status='ACTIVE', UPDATE config_revision +1
        BF->>BF: IndexedAttributeActivated
    end
    QA->>PG: (via replica) search auth.factor in window older than backfill_cursor
    QA-->>ADM: results with partialIndex true while BACKFILLING
```

**Explanation.** Indexing a key needs no DDL. It means writing rows from now on, plus an optional throttled backfill that reads from the replica and writes to the primary.

Newest-first ordering means the most frequently searched recent windows become complete first. The `backfill_cursor` column (an addition to the brief's `indexed_attribute`) makes the job resumable after a leader change.

---

## SD-10 — UC-09 IdP login T1, T2, T3 linked into one business operation

```mermaid
sequenceDiagram
    autonumber
    participant GW as idp-gateway / idp-auth-service /<br/>idp-otp-service / idp-token-service
    participant K as otlp-traces
    participant CE as tc-ingester<br/>CorrelationExtractor + SpanBatchWriter
    participant PG as PostgreSQL primary
    participant CR as CorrelationResolver<br/>tc-maintenance (every 10 s, ShedLock)

    GW->>K: T1 fragments (POST /login/first-factor)<br/>user_id=u-42, client_id=bookstore-web, auth.factor=PASSWORD, idp.auth_session_id=as-777
    K->>CE: poll
    CE->>PG: span, trace_summary ... + INSERT trace_correlation (id 9001, type idp.login, key idp.auth_session_id, as-777, T1, origin 1)
    CR->>PG: lock correlation-resolver, SELECT last_processed_id FROM correlation_resolver_state (9000)
    CR->>PG: SELECT * FROM trace_correlation WHERE id > 9000 ORDER BY id LIMIT 10000
    PG-->>CR: row 9001
    CR->>PG: SELECT correlation_value WHERE (1, 'as-777'), other observations of T1 of type 1
    PG-->>CR: none
    CR->>CR: link T1: its value as-777 has no operation
    CR->>PG: INSERT business_operation (uuidv7 0193a6f2..., first_seen 09:15:02.130Z, display_value as-777, trace_count 1)
    CR->>PG: INSERT correlation_value (1, as-777, 0193a6f2..., first_seen)
    CR->>PG: UPDATE correlation_resolver_state SET last_processed_id = 9001, COMMIT
    GW->>K: T2 fragments (POST /login/second-factor)<br/>auth.factor=SMS, idp.auth_session_id=as-777, oauth.authorization_code=code-abc
    K->>CE: poll
    CE->>PG: INSERT trace_correlation 9050 (as-777, T2) and 9051 (code-abc, T2, key ALIAS)
    CR->>PG: next tick: rows 9050, 9051
    CR->>CR: component as-777 + code-abc via T2, existing roots = 0193a6f2 only
    CR->>CR: merge_window check: 09:15:31 within first_seen - 24h .. last_seen + 24h
    CR->>PG: INSERT correlation_value (1, code-abc, 0193a6f2...)
    CR->>PG: recount distinct traces over values = 2, UPDATE business_operation last_seen, trace_count 2
    CR->>PG: advance watermark, COMMIT
    GW->>K: T3 fragments (POST /oauth2/token)<br/>client_id=bookstore-web, oauth.authorization_code=code-abc
    K->>CE: poll
    CE->>PG: INSERT trace_correlation 9102 (code-abc, T3)
    CR->>PG: next tick: row 9102, code-abc maps to 0193a6f2
    CR->>PG: UPDATE business_operation trace_count 3, last_seen 09:15:33.044Z, COMMIT
    Note over CR,PG: operation 0193a6f2 = T1 (authentication), T2 (authentication + token-exchange), T3 (token-exchange)
```

**Explanation.** Extraction is synchronous and cheap: an append in the ingest transaction, driven by inserted spans. Resolution is asynchronous and single-leader.

The resolver only ever reads `trace_correlation` forward from its watermark, and it keeps two invariants:

- each value maps to exactly one **root** operation;
- `trace_count` is an exact recount.

The watermark advance respects the 30 s gap-grace rule (05 §4.11).

---

## SD-11 — UC-10 Alias arrives first and operations are merged

```mermaid
sequenceDiagram
    autonumber
    participant CE as tc-ingester / tc-enricher
    participant PG as PostgreSQL primary
    participant CR as CorrelationResolver
    participant QA as tc-query-api

    CE->>PG: trace_correlation (as-777, T1) - operation A created earlier (display_value as-777)
    Note over CE: T2 fragment with code-abc delayed (producer flush / partition lag)
    CE->>PG: INSERT trace_correlation 9102 (code-abc, T3, ALIAS)
    CR->>PG: tick: row 9102, correlation_value (1, code-abc) not found
    CR->>PG: INSERT business_operation B (provisional, display_value NULL, first_seen 09:15:33)
    CR->>PG: INSERT correlation_value (1, code-abc, B), COMMIT
    CE->>PG: INSERT trace_correlation 9150 (as-777, T2), 9151 (code-abc, T2)
    CR->>PG: tick: rows 9150, 9151
    CR->>CR: component via T2 touches roots A (as-777) and B (code-abc)
    CR->>CR: both within merge_window, neither capped
    CR->>CR: survivor rule: A has display value, B provisional, survivor = A
    CR->>PG: BEGIN
    CR->>PG: UPDATE business_operation SET merged_into = A WHERE id = B
    CR->>PG: UPDATE business_operation SET merged_into = A WHERE merged_into = B (depth <= 1)
    CR->>PG: UPDATE correlation_value SET operation_id = A, operation_first_seen = A.first_seen WHERE operation_id = B
    CR->>PG: recount A: distinct traces over (as-777, code-abc) = T1, T2, T3 - trace_count 3
    CR->>PG: UPDATE correlation_resolver_state, COMMIT
    CR->>CR: BusinessOperationMerged(survivor A, merged B)
    QA->>PG: (replica) GET /business-operations/B
    PG-->>QA: B.merged_into = A
    QA-->>QA: respond with A plus mergedFrom [B]
```

**Explanation.** The arrival order of fragments across partitions is not controllable. A provisional operation is created for an alias seen alone, and union-find merges it when a bridging trace appears.

The deterministic survivor rule is: has a display value, then earlier `first_seen`, then smaller id. Together with path compression it makes the final state independent of processing order.

---

## SD-12 — UC-11 Get trace by id

```mermaid
sequenceDiagram
    autonumber
    actor AN as Trace Analyst
    participant UI as tc-ui
    participant GW as API gateway
    participant TC as TraceController<br/>tc-query-api
    participant TQ as TraceQueryService
    participant CA as Caffeine caches<br/>(dictionaries, resources)
    participant PGR as PostgreSQL replica
    participant AS as TraceAssembler + PiiMaskingPolicy

    AN->>UI: open trace 7a3c1e9d2b4f60851c2d3e4f5a6b7c8d
    UI->>GW: GET /api/v1/traces/7a3c...7c8d?start=..&end=.. (Bearer JWT)
    GW->>TC: forward (JWT validated by resource server)
    TC->>TC: TraceId.parse, to uuid
    TC->>TQ: getTrace(id, hints)
    alt hints present
        TQ->>PGR: SELECT * FROM trace_summary WHERE trace_id=? AND trace_day BETWEEN date(start)-1 AND date(end)
    else no hints
        TQ->>PGR: SELECT * FROM trace_summary WHERE trace_id=? (<= 7 daily partitions of the FULL tier, PK probes)
    end
    alt no row
        TQ->>PGR: SELECT ... FROM trace_digest WHERE trace_id=? (DIGEST tier fallback, see SD-22)
        alt digest row found
            TQ-->>TC: TraceDigestView (tier DIGEST, spansExpired true)
            TC-->>UI: 200 digest only (SD-22)
        else no digest row
            TQ->>PGR: SELECT count(*) FROM trace_enrichment WHERE trace_id=?
            TQ-->>TC: not found (enrichmentOnly flag)
            TC-->>UI: 404 TRACE_NOT_FOUND
        end
    else one or more rows
        TQ->>TQ: TraceSummary.merge over rows (split trace_day)
        TQ->>PGR: SELECT ... FROM span WHERE trace_id=? AND start_time BETWEEN start_time AND end_time (hourly pruning)
        par enrichments
            TQ->>PGR: SELECT ... FROM trace_enrichment WHERE trace_id=?
        and links
            TQ->>PGR: SELECT ... FROM span_link WHERE trace_id=? / WHERE linked_trace_id=?
        and correlations
            TQ->>PGR: trace_correlation WHERE trace_id=?, then correlation_value, business_operation (root)
        and resources
            TQ->>CA: resources / scopes / service / span_name by id
            CA->>PGR: misses only
        end
        TQ->>AS: assemble(summary, spans, enrichments, links, operations)
        AS->>AS: build tree, orphans under synthetic node, dedupe span_id
        AS->>AS: Completeness (settleAfter 2 min), attach SPAN/TRACE enrichments
        AS->>AS: mask PERSONAL unless SCOPE_TC_PII, mask SECRET always
        AS-->>TC: TraceView
        TC-->>UI: 200 JSON, tier FULL (header X-TC-Replica-Lag-Ms if lag > 5 s)
    end
    UI-->>AN: waterfall view, enrichment panel, business-operation chip
```

**Explanation.** `trace_summary` acts as the trace directory. It converts a bare trace id into a time window, so span reads touch only the 1–2 hourly partitions that can contain the trace.

Without hints, the cost is at most 7 index probes on the summary, one per retained day of the FULL tier (n = 7). A miss falls back to `trace_digest` (SD-22). Side data is fetched in parallel on virtual threads (`StructuredTaskScope`).

---

## SD-13 — UC-12 Search traces

```mermaid
sequenceDiagram
    autonumber
    actor AN as Trace Analyst
    participant UI as tc-ui
    participant SC as SearchController
    participant SS as SearchService
    participant SE as SelectivityEstimator
    participant REG as IndexedAttributeRegistry (snapshot)
    participant PGR as PostgreSQL replica
    participant PM as PiiMaskingPolicy

    AN->>UI: user_id = u-42, client_id = bookstore-web, auth.factor IN SMS, PUSH, last 24 h
    UI->>SC: POST /api/v1/traces/search
    SC->>SS: search(criteria)
    SS->>SS: validate to - from <= 7 d, limit <= 500, decode cursor and verify filter hash
    SS->>REG: keys indexed? (user_id, client_id, auth.factor, service.name)
    alt key not indexed and no scanMode
        SS-->>UI: 400 /problems/non-indexed-attribute (hint BRUTE_FORCE <= 1 h, or index it)
    end
    par capped probe per predicate (virtual threads)
        SE->>PGR: count(*) FROM (SELECT 1 FROM trace_attribute WHERE key=user_id AND value_str='u-42' AND ts in window LIMIT 10001)
        PGR-->>SE: 37
    and
        SE->>PGR: same for client_id = bookstore-web
        PGR-->>SE: 10001 (capped)
    and
        SE->>PGR: same for auth.factor IN (SMS, PUSH)
        PGR-->>SE: 10001 (capped)
    end
    SE-->>SS: driving predicate = user_id
    SS->>PGR: WITH drv AS (DISTINCT trace_id from trace_attribute for user_id) JOIN trace_summary<br/>(trace_day between from-1 and to, start_time in window)<br/>AND EXISTS client_id AND EXISTS auth.factor AND EXISTS service.name<br/>ORDER BY start_time DESC, trace_id DESC LIMIT 51 (statement_timeout 10 s)
    PGR-->>SS: up to 51 rows
    SS->>PGR: matchedAttributes for page trace ids
    SS->>SS: dictionary ids to names, nextCursor if 51 rows
    opt key BACKFILLING and window older than backfill coverage
        SS->>SS: partialIndex = true
    end
    SS->>PM: mask(matchedAttributes)
    PM-->>SS: user_id u-**
    SS-->>UI: 200 traces[], nextCursor, partialIndex, stats
    UI-->>AN: results table, click opens SD-12 with start/end hints
```

**Explanation.** Search drives from the most selective indexed predicate, estimated with cheap capped index-only counts rather than planner statistics, which are unreliable for a multi-key table like `trace_attribute`. The other predicates are checked with `EXISTS` probes on the same covering index.

The final filter and the ordering are on `trace_summary.start_time`. This produces deterministic, keyset-paginated results.

---

## SD-14 — UC-13 Bounded brute-force search

```mermaid
sequenceDiagram
    autonumber
    participant UI as tc-ui
    participant SS as SearchService
    participant BH as BruteForceBulkhead<br/>(2 permits per instance)
    participant PGR as PostgreSQL replica

    UI->>SS: POST /traces/search scanMode BRUTE_FORCE, 09:00-09:30, idp.risk.score_reason = NEW_DEVICE
    SS->>SS: window <= 1 h ?
    alt window > 1 h
        SS-->>UI: 400 /problems/window-too-large
    end
    SS->>BH: tryAcquire()
    alt no permit
        BH-->>SS: false
        SS-->>UI: 429 BRUTE_FORCE_BUSY, Retry-After 10
    else permit
        loop 5-minute slices newest first, until limit reached or 30 s elapsed
            SS->>PGR: BEGIN, SET LOCAL statement_timeout 30s, SET LOCAL max_parallel_workers_per_gather 8
            SS->>PGR: SELECT DISTINCT ON (trace_id) trace_id, start_time FROM span<br/>WHERE start_time in slice AND service_id = ? AND attributes @> key/value
            PGR-->>SS: matching trace ids (parallel seq scan of hourly partition)
            SS->>PGR: COMMIT
        end
        SS->>PGR: join trace_summary for collected ids, order, page
        SS->>BH: release()
        SS-->>UI: 200 traces[], scanMode BRUTE_FORCE, complete true or false, scannedUntil
    end
```

**Explanation.** Brute force is the safety valve for non-indexed keys (ADR-05). It is limited in four ways:

- the window (≤ 1 h);
- concurrency (a bulkhead);
- the per-statement timeout;
- slicing, which turns a timeout into a partial but honest answer (`complete: false`).

It runs only on the replica.

---

## SD-15 — UC-14 Business-operation timeline from the UI

```mermaid
sequenceDiagram
    autonumber
    actor AN as Trace Analyst
    participant UI as tc-ui
    participant BO as BusinessOperationService<br/>tc-query-api
    participant PGR as PostgreSQL replica
    participant PM as PiiMaskingPolicy

    AN->>UI: lookup idp.login, value as-777 (from support ticket)
    UI->>BO: GET /api/v1/business-operations?type=idp.login&value=as-777&from&to
    BO->>PGR: SELECT id FROM business_operation_definition WHERE name = 'idp.login'
    BO->>PGR: SELECT operation_id, operation_first_seen FROM correlation_value WHERE (1, 'as-777')
    alt not found
        BO->>PGR: EXISTS trace_correlation (1, as-777) with id > last_processed_id ?
        BO-->>UI: 200 [] (pendingResolution true if exists)
    else found
        BO->>PGR: SELECT * FROM business_operation WHERE id = ? AND first_seen = ?
        opt merged_into not null
            BO->>PGR: load survivor (depth <= 1)
        end
        BO-->>UI: 200 [operation summary]
    end
    AN->>UI: open operation
    UI->>BO: GET /api/v1/business-operations/0193a6f2-6c1e-7b3a-9f10-2d4c5e6f7a80
    BO->>PGR: business_operation + all correlation_value rows of operation
    BO->>PGR: trace_correlation WHERE type = 1 AND value = ANY(as-777, code-abc)<br/>AND observed_at within first_seen - window .. last_seen + window
    PGR-->>BO: (as-777,T1,key1) (as-777,T2,key1) (code-abc,T2,key2) (code-abc,T3,key2)
    BO->>BO: distinct traces with step labels from correlation_key
    BO->>PGR: trace_summary for T1, T2, T3 (trace_day near observed_at)
    BO->>PGR: trace_attribute for display keys (auth.factor, client_id, user_id, sms.delivery.status)
    BO->>BO: order by start_time, compute gaps (T1 to T2 29 388 ms)
    BO->>PM: mask user_id (PERSONAL), code-abc (SECRET)
    BO-->>UI: 200 timeline T1, T2, T3 with stepLabels and linkedBy
    UI-->>AN: horizontal timeline, click bar opens SD-12 with hints
```

**Explanation.** The timeline is built from durable facts (`trace_correlation`) and the resolver's mapping (`correlation_value`). It therefore shows *why* each trace belongs to the operation (`linkedBy`, `stepLabels`), not just that it does.

Secret alias values are never revealed. They are still used for linking.

---

## SD-16 — UC-16 Partition create and drop

```mermaid
sequenceDiagram
    autonumber
    participant SCH as Scheduler (cron, hourly at :05)
    participant PM as PartitionManager<br/>tc-maintenance
    participant SL as shedlock table
    participant PG as PostgreSQL primary<br/>(direct DataSource, no PgBouncer)
    participant ST as StatsRefresher

    SCH->>PM: run()
    PM->>SL: lock partition-manager (lockAtMostFor 30 min)
    alt lock held elsewhere
        SL-->>PM: not acquired, skip
    else acquired
        PM->>PG: SELECT * FROM retention_policy (tier FULL n = 7 d, tier DIGEST n + m = 97 d)
        loop each table (FULL: span, trace_attribute, span_link, trace_summary, trace_enrichment, trace_correlation, business_operation, DIGEST: trace_digest, business_operation_digest)
            PM->>PG: list partitions and bounds (pg_inherits, pg_get_expr(relpartbound), inhdetachpending)
            opt detach pending from interrupted run
                PM->>PG: ALTER TABLE tc.span DETACH PARTITION tc.span_p2026110613 FINALIZE
            end
            loop missing partitions up to now + precreate (3 d)
                PM->>PG: CREATE TABLE IF NOT EXISTS tc.span_p2026112415 PARTITION OF tc.span<br/>FOR VALUES FROM (2026-11-24 15:00+00) TO (2026-11-24 16:00+00) WITH (fillfactor=100, autovacuum insert tuning)
                PM->>PM: PartitionCreated
            end
            loop partitions with upper bound <= now - retention
                opt FULL tier and digestDays > 0 (drop gate)
                    PM->>PG: SELECT trace_day, status FROM compaction_state<br/>WHERE read window [D - 1 h, D + 1 day + 6 h) overlaps the partition bounds
                    PG-->>PM: e.g. 2026-11-21 DONE
                end
                alt gate closed (an overlapping trace day not DONE)
                    PM->>PM: keep partition, PartitionDropDeferred (tc_compaction_lag_days)
                else gate open, or DIGEST tier partition older than n + m days
                    PM->>PG: SET lock_timeout = 60s
                    PM->>PG: ALTER TABLE tc.span DETACH PARTITION tc.span_p2026112114 CONCURRENTLY (autocommit, no default partition)
                    PM->>PG: DROP TABLE tc.span_p2026112114
                    PM->>PM: PartitionDropped
                end
            end
        end
        opt business_operation month dropped (whole month older than n days and all its root operations digested)
            PM->>PG: DELETE FROM correlation_value WHERE operation_first_seen < cutoff (batches of 10 000)
        end
        PM->>PM: gauge tc_partitions_ahead_hours per table (alert < 48 h)
        PM->>SL: unlock
    end
    SCH->>ST: run() after hour closes
    ST->>PG: ANALYZE tc.span_p2026112114, tc.trace_attribute_p2026112114
```

**Explanation.** Retention is entirely partition DDL: no DELETE on big tables, no vacuum debt.

The **drop gate** (ADR-13) makes PartitionManager depend on compaction: a FULL-tier partition older than n days is dropped only when every trace day whose compaction read window overlaps it is `DONE` in `compaction_state` (SD-21). If compaction lags, the data is kept and `tc_compaction_lag_days` rises; uncompacted data is never dropped. DIGEST-tier partitions have no gate and are dropped at n + m days. With `digestDays = 0` the gate is skipped.

DETACH CONCURRENTLY requires autocommit, is forbidden when a DEFAULT partition exists (there is none), and can be finalized after an interruption. Partitions are created three days ahead, so the brief ACCESS EXCLUSIVE lock of CREATE never coincides with the moment ingest needs the partition.

DDL uses a direct connection, not the transaction pooler.

---

## SD-17 — UC-19 UI login via OIDC and PII masking

```mermaid
sequenceDiagram
    autonumber
    actor U as Analyst (browser)
    participant UI as tc-ui SPA (nginx)
    participant IDP as Organisation IdP (OIDC)
    participant GW as API gateway
    participant QA as tc-query-api<br/>OAuth2 Resource Server
    participant PM as PiiMaskingPolicy
    participant AUD as audit log (tc.audit)

    U->>UI: open https://tcs.corp/
    UI->>UI: no token in memory, create code_verifier, code_challenge = S256(verifier)
    UI->>IDP: redirect /authorize client_id=tc-ui, response_type=code, scope openid profile tc.read, code_challenge
    U->>IDP: authenticate (corporate SSO)
    IDP-->>UI: redirect /callback?code=...&state=...
    UI->>IDP: POST /token code + code_verifier
    IDP-->>UI: access token (JWT, roles TC_VIEWER, scope TC_PII?), id token, refresh token
    UI->>GW: GET /api/v1/traces/T1 Authorization Bearer JWT
    GW->>QA: forward
    QA->>QA: verify signature (cached JWKS), iss, aud tc-api, exp
    QA->>QA: map roles claim to TC_VIEWER / TC_ADMIN, scope TC_PII to SCOPE_TC_PII
    alt missing or invalid token
        QA-->>UI: 401, SPA restarts login
    else no TC_VIEWER
        QA-->>UI: 403
    else authorised
        QA->>QA: load TraceView (SD-12)
        QA->>PM: mask(view, authorities)
        alt has SCOPE_TC_PII
            PM->>PM: PERSONAL values clear (user_id u-42), SECRET masked (***)
            PM->>AUD: audit record (user, traceId, keys user_id, ts)
        else viewer without TC_PII
            PM->>PM: PERSONAL masked (u-**), SECRET masked (***)
        end
        PM-->>QA: masked view + masking.maskedKeys
        QA-->>UI: 200
    end
```

**Explanation.** The SPA is a public OIDC client using Authorization Code + PKCE, and it keeps tokens in memory.

Authorisation and masking happen only on the server. The browser never receives unmasked PII it is not entitled to, and any unmasked access is audited. `tc-control-plane` uses the same resource-server configuration with `TC_ADMIN`.

---

## SD-18 — UC-20 Related traces

```mermaid
sequenceDiagram
    autonumber
    participant UI as tc-ui
    participant RT as RelatedTracesService<br/>tc-query-api
    participant PGR as PostgreSQL replica
    participant PM as PiiMaskingPolicy

    UI->>RT: GET /api/v1/traces/c0ffee00d15ea5e0123456789abcdef0/related?start&end
    par outgoing span links
        RT->>PGR: SELECT span_id, linked_trace_id, linked_span_id, attributes FROM span_link<br/>WHERE trace_id = T3 AND start_time in hint window
    and incoming span links
        RT->>PGR: SELECT trace_id, span_id, linked_span_id, start_time FROM span_link<br/>WHERE linked_trace_id = T3 (index linked_trace_id, start - 1 d .. end + 7 d)
    and business operations
        RT->>PGR: trace_correlation WHERE trace_id = T3, then correlation_value, root business_operation
        RT->>PGR: other traces of those operations (limit 50 per operation)
    end
    RT->>PGR: trace_summary for all referenced trace ids
    PGR-->>RT: summaries (missing ones marked available false)
    RT->>PM: mask link attributes, display values
    RT-->>UI: 200 outgoingLinks, incomingLinks (e.g. idp-audit-consumer), businessOperations (idp.login with T1, T2)
```

**Explanation.** The UI keeps two kinds of relation apart:

- **causal** relations from native OTel span links, indexed in both directions by `span_link`;
- **business** relations from business operation definitions.

The incoming-link query relies on the `(linked_trace_id)` index of each daily partition. Hints narrow the partitions probed.

---

## SD-19 — UC-18 Replay from Kafka and DLQ re-drive

```mermaid
sequenceDiagram
    autonumber
    actor OPS as Platform Operator
    participant K8S as Kubernetes
    participant KA as Kafka admin CLI
    participant K as otlp-traces
    participant ING as tc-ingester (Deployment)
    participant JOB as tc-ingester Job<br/>profile dlq-redrive
    participant DLQ as otlp-traces.dlq
    participant PG as PostgreSQL primary

    rect rgb(235, 245, 255)
    Note over OPS,PG: (a/b) re-ingest a time range via consumer-group offset reset
    OPS->>K8S: scale deployment tc-ingester to 0
    OPS->>KA: kafka-consumer-groups --group tc-ingester --topic otlp-traces<br/>--reset-offsets --to-datetime 2026-11-21T08:00:00.000 --execute
    KA-->>OPS: new offsets per partition
    OPS->>K8S: scale tc-ingester to 3
    K->>ING: records from 08:00 again
    ING->>PG: SD-01 path, ON CONFLICT DO NOTHING (mostly duplicates)
    ING->>ING: inserted only for missing spans, duplicate metric rises
    end
    rect rgb(245, 255, 235)
    Note over OPS,PG: (c) DLQ re-drive after parser fix (no consumer group)
    OPS->>K8S: run Job tc-ingester --spring.profiles.active=dlq-redrive --from --to --reason
    JOB->>DLQ: assign(all 6 partitions), offsetsForTimes(from), seek
    loop until record timestamp > to
        DLQ->>JOB: records with tc-error-reason header
        JOB->>JOB: filter by reason, parse with current parsers
        alt parse ok
            JOB->>PG: SpanBatchWriter (same idempotent path)
        else fails again
            JOB->>JOB: count failedAgain, log, do NOT republish
        end
    end
    JOB-->>OPS: report ok, failedAgain, byReason
    end
```

**Explanation.** Kafka retention is the replay horizon (3 d for `otlp-traces`, 14 d for the DLQ). Because all writes are idempotent, a replay never needs DB cleanup.

DLQ re-drive uses assignment, not a consumer group, so no new group or topic is introduced. Failures are not re-published, which avoids loops.

---

## SD-20 — UC-15 Autocomplete

```mermaid
sequenceDiagram
    autonumber
    participant UI as tc-ui search form
    participant AC as AutocompleteController<br/>tc-query-api
    participant CA as Caffeine (60 s)
    participant PGR as PostgreSQL replica

    UI->>AC: GET /api/v1/services
    AC->>CA: services
    alt miss
        CA->>PGR: SELECT name FROM service ORDER BY name
    end
    AC-->>UI: idp-auth-service, idp-gateway, idp-otp-service, idp-token-service
    UI->>AC: GET /api/v1/services/idp-gateway/span-names
    AC->>PGR: SELECT name FROM span_name WHERE service_id = ? ORDER BY name LIMIT 1000
    AC-->>UI: POST /login/first-factor, POST /login/second-factor, POST /oauth2/token
    UI->>AC: GET /api/v1/search/attributes?from&to
    AC->>AC: indexed keys from snapshot (status, numeric, partialIndex for window)
    AC-->>UI: auth.factor, client_id, service.name, sms.delivery.status, span.name, status, user_id
    UI->>AC: GET /api/v1/search/attributes/auth.factor/values?prefix=S&from&to&limit=20
    AC->>AC: PERSONAL key and no TC_PII ? then 403 PII_ENUMERATION_FORBIDDEN
    AC->>PGR: value_str range [S, T) on (key_id, value_str, ts) index, sample LIMIT 50000, GROUP BY, top 20
    PGR-->>AC: SMS 18233
    AC-->>UI: [SMS]
```

**Explanation.** Autocomplete reads only dictionaries and the inverted index. Value suggestions are sampled with a row cap, bounded to ≤ 24 h, and forbidden for PERSONAL keys without `TC_PII`. This prevents enumeration of user identifiers.

---

## SD-21 — UC-21 Compaction of one trace day into the digest tier

```mermaid
sequenceDiagram
    autonumber
    participant SCH as Scheduler (every 5 min)
    participant DC as DigestCompactor<br/>tc-maintenance
    participant SL as shedlock table
    participant PG as PostgreSQL primary<br/>(direct DataSource, no PgBouncer)
    participant OR as OutcomeRule.evaluate
    participant PM as PartitionManager<br/>(next hourly run)

    SCH->>DC: run() at 2026-11-29T00:05Z
    DC->>SL: lock digest-compactor
    alt lock held elsewhere
        SL-->>DC: not acquired, skip
    else acquired
        DC->>PG: load config snapshot: retained keys (user_id SHA256, client_id, auth.factor,<br/>sms.delivery.status, idp.auth_session_id NONE), compaction_exclusion, outcome rules, fullDays 7
        DC->>PG: INSERT compaction_state (2026-11-21, PENDING) ON CONFLICT DO NOTHING<br/>for every day with D + 1 day <= now - 7 d
        DC->>PG: pick oldest day not DONE: 2026-11-21, UPDATE status RUNNING, started_at now
        loop 1-hour chunks of trace_summary.start_time from last_chunk_end to 2026-11-22T00:00Z
            DC->>PG: BEGIN, SET LOCAL statement_timeout 15min
            DC->>PG: INSERT INTO trace_digest SELECT trace_summary (D, chunk)<br/>LEFT JOIN retained trace_attribute values (ts in D - 1 h .. D + 1 day + 6 h, transform applied)<br/>LEFT JOIN root business_operation via trace_correlation, correlation_value<br/>WHERE operation found OR root not in compaction_exclusion<br/>ON CONFLICT (trace_id, trace_day) DO NOTHING
            PG-->>DC: rows inserted (T1, T2, T3 in chunk 09:00-10:00)
            DC->>PG: UPDATE compaction_state SET last_chunk_end, traces_digested, traces_excluded
            DC->>PG: COMMIT
        end
        alt a chunk failed or timed out
            DC->>PG: ROLLBACK, UPDATE compaction_state SET status FAILED, error
            Note over DC,PG: next tick: FAILED to RUNNING, resume from last_chunk_end
        else all chunks committed
            DC->>PG: UPDATE compaction_state SET status DONE, finished_at now
            DC->>DC: TraceDayCompacted(2026-11-21)
            DC->>PG: SELECT root business_operation (merged_into IS NULL) with last_seen < now - 7 d,<br/>no digest yet, all member trace days DONE
            PG-->>DC: 0193a6f2 (idp.login, as-777, members T1, T2, T3)
            loop each operation
                DC->>PG: member trace_digest rows, step labels via trace_correlation and correlation_key,<br/>outcomeKey values from trace_attribute (if the rule has an outcomeKey)
                DC->>OR: evaluate(rule finalStepLabel token-exchange, members)
                OR->>OR: 1 no outcomeKey, 2 no member errors, 3 token-exchange reached by T2 and T3
                OR-->>DC: SUCCESS, outcome_detail null
                DC->>PG: INSERT INTO business_operation_digest (op_day 2026-11-21, outcome 1, steps T1 authentication,<br/>T2 token-exchange, T3 token-exchange, attributes union) ON CONFLICT DO NOTHING
                DC->>PG: UPDATE compaction_state SET operations_digested + 1
                DC->>DC: BusinessOperationCompacted(0193a6f2, SUCCESS)
            end
        end
        DC->>DC: metrics tc_compaction_lag_days, tc_compaction_traces_total, tc_compaction_chunk_seconds
        DC->>SL: unlock
    end
    PM->>PG: drop gate: compaction_state for days overlapping span_p2026112109
    PG-->>PM: 2026-11-21 DONE
    PM->>PG: DETACH CONCURRENTLY and DROP FULL-tier partitions of 2026-11-21 (SD-16)
```

**Explanation.** Compaction is a set-based batch over one closed trace day. Each 1-hour chunk is one `INSERT ... SELECT` that reads only `trace_summary`, the retained-key rows of `trace_attribute` and the correlation tables; spans are never read. Progress is stored in `compaction_state.last_chunk_end` and every insert is `ON CONFLICT DO NOTHING`, so a leader change or a failed chunk only repeats work.

Business operations are compacted after their trace days are DONE, so `steps` and the outcome see every member. The outcome rule runs in its fixed order (05 §4.10.1); for `idp.login` the final step `token-exchange` decides SUCCESS. Only after the day is DONE does the drop gate in PartitionManager let the FULL-tier partitions go.

---

## SD-22 — UC-22 Cross-tier search and trace-by-id fallback

```mermaid
sequenceDiagram
    autonumber
    actor AN as Trace Analyst
    participant UI as tc-ui
    participant TQ as TraceQueryService<br/>tc-query-api
    participant SS as SearchService
    participant BO as BusinessOperationService
    participant PGR as PostgreSQL replica
    participant PM as PiiMaskingPolicy

    rect rgb(235, 245, 255)
    Note over AN,PM: trace by id after n days (now 2026-11-30T12:00Z)
    AN->>UI: open trace 7a3c1e9d2b4f60851c2d3e4f5a6b7c8d from a support ticket
    UI->>TQ: GET /api/v1/traces/7a3c...7c8d
    TQ->>PGR: SELECT * FROM trace_summary WHERE trace_id=? (FULL tier, <= 7 daily partitions)
    PGR-->>TQ: no row (2026-11-21 partitions dropped after compaction)
    TQ->>PGR: SELECT * FROM trace_digest WHERE trace_id=? (<= n + m daily partitions, PK probes)
    PGR-->>TQ: digest row (trace_day 2026-11-21, outcome OK, attributes, business_operation_id 0193a6f2)
    TQ->>PGR: SELECT display_value, outcome FROM business_operation_digest WHERE operation_id=?
    TQ->>PM: mask(digest view)
    PM-->>TQ: user_id hash masked to 16** (PERSONAL, no TC_PII)
    TQ-->>UI: 200 tier DIGEST, spansExpired true, digest, businessOperations (SUCCESS)
    UI-->>AN: banner span details expired - digest only, no waterfall
    end

    rect rgb(245, 255, 235)
    Note over AN,PM: cross-tier search, boundary = now - 7 d = 2026-11-23T12:00Z
    AN->>UI: user_id = u-42, auth.factor IN SMS, PUSH, 2026-11-20T12:00Z .. 2026-11-27T12:00Z
    UI->>SS: POST /api/v1/traces/search
    SS->>SS: range touches FULL tier, so max 7 d: ok
    SS->>PGR: SELECT trace_day FROM compaction_state WHERE status <> DONE AND trace_day < boundary
    PGR-->>SS: none (no lag, boundary stays 2026-11-23T12:00Z)
    par FULL part 2026-11-23T12:00Z .. 2026-11-27T12:00Z
        SS->>PGR: SD-13 path on trace_attribute + trace_summary
        PGR-->>SS: 5e8d0c1b... (tier FULL)
    and DIGEST part 2026-11-20T12:00Z .. 2026-11-23T12:00Z
        SS->>SS: user_id has digest_transform SHA256: hash u-42 to 16ec525b...28a3
        SS->>PGR: SELECT FROM trace_digest WHERE start_time in part<br/>AND attributes @> user_id hash AND (auth.factor SMS OR PUSH)<br/>AND service_ids @> idp-auth-service (GIN jsonb_path_ops)
        PGR-->>SS: 7a3c1e9d... (tier DIGEST)
    end
    SS->>SS: concatenate FULL first then DIGEST, keyset cursor on (start_time, trace_id)
    SS->>PM: mask matchedAttributes
    SS-->>UI: 200 traces with tier badges, warnings []
    AN->>UI: add client_id PREFIX book
    UI->>SS: POST /api/v1/traces/search (same range, extra predicate)
    SS->>SS: PREFIX not supported on digest attributes: skip DIGEST part
    SS->>PGR: FULL part only (SD-13)
    SS-->>UI: 200 FULL results, warnings DIGEST_TIER_UNSUPPORTED_PREDICATE key client_id
    end

    rect rgb(255, 245, 235)
    Note over AN,PM: historical business operation (2026-12-15, FULL month already dropped)
    AN->>UI: lookup idp.login as-777, outcome SUCCESS
    UI->>BO: GET /api/v1/business-operations?type=idp.login&value=as-777&outcome=SUCCESS&from&to
    BO->>PGR: correlation_value (1, as-777) in FULL tier
    PGR-->>BO: none
    BO->>PGR: SELECT FROM business_operation_digest WHERE operation_definition_id=1 AND display_value='as-777' AND outcome=1
    PGR-->>BO: 0193a6f2 (tier DIGEST)
    BO-->>UI: 200 [operation, outcome SUCCESS]
    UI->>BO: GET /api/v1/business-operations/0193a6f2-6c1e-7b3a-9f10-2d4c5e6f7a80
    BO->>PGR: business_operation by id (FULL)
    PGR-->>BO: not found
    BO->>PGR: SELECT * FROM business_operation_digest WHERE operation_id=?
    BO->>BO: timeline from steps (authentication, token-exchange, token-exchange), gaps
    BO->>PM: mask attributes (user_id hash)
    BO-->>UI: 200 tier DIGEST, outcome SUCCESS, timeline
    UI-->>AN: timeline bars open trace digests
    end
```

**Explanation.** `tc-query-api` splits every time range at the tier boundary `now − n days` and answers the older part from the digest tables. The boundary moves back if a trace day before it is not yet DONE, because the drop gate still keeps that day in the FULL tier. Lookups by id try the FULL tier first and fall back to the digest, so links and bookmarks keep working for n + m days.

Values of `SHA256` keys are hashed by the API before matching. Predicates the digest cannot answer (non-retained keys, PREFIX and range operators, keys stored with MASK) skip the digest part and are reported in `warnings`, never silently ignored.

---

## SD-23 — UC-23 Configure retention tiers, retained attribute and outcome rule

```mermaid
sequenceDiagram
    autonumber
    actor ADM as TC Admin
    participant UI as tc-ui admin
    participant CP as tc-control-plane
    participant PG as PostgreSQL primary
    participant AUD as audit log (tc.audit)
    participant DC as DigestCompactor<br/>tc-maintenance
    participant PM as PartitionManager<br/>tc-maintenance

    ADM->>UI: Retention tiers form: fullDays 7, digestDays 90
    UI->>CP: PUT /api/v1/admin/retention-policies/tiers {fullDays 7, digestDays 90}
    CP->>CP: RetentionTiers: fullDays >= 2, digestDays >= 0
    CP->>PG: SELECT max(merge_window) FROM business_operation_definition (idp.login PT24H = 1 d)
    CP->>CP: fullDays 7 > 1 d: ok
    alt invalid
        CP-->>UI: 400 FULL_DAYS_TOO_SMALL / FULL_DAYS_BELOW_MERGE_WINDOW / INVALID_DIGEST_DAYS
    else valid
        CP->>PG: BEGIN, SELECT config_revision FOR UPDATE
        CP->>PG: UPDATE retention_policy SET retention = 7 days WHERE tier = 'FULL'
        CP->>PG: UPDATE retention_policy SET retention = 97 days WHERE tier = 'DIGEST'
        CP->>CP: RetentionPolicy.validateSet
        CP->>PG: UPDATE config_revision +1, COMMIT
        CP->>AUD: RetentionTiersChanged (actor, old and new values)
        CP-->>UI: 200 policies, digestDropAgeDays 97
    end

    ADM->>UI: Indexed attributes: user_id, Retain in digest, transform SHA256
    UI->>CP: PATCH /api/v1/admin/indexed-attributes/7 {retainInDigest true, digestTransform SHA256}
    CP->>CP: user_id is PERSONAL: SHA256 ok (NONE would warn, SECRET with NONE is refused)
    CP->>PG: UPDATE indexed_attribute SET retain_in_digest = true, digest_transform = 'SHA256'
    CP->>PG: UPDATE config_revision +1, COMMIT
    CP->>AUD: audit (key user_id, retain SHA256)
    CP-->>UI: 200 retainInDigest true
    ADM->>UI: Retain idp.auth_session_id (not indexed yet)
    UI->>CP: POST /api/v1/admin/indexed-attributes {key idp.auth_session_id, retainInDigest true, backfillWindow P7D}
    CP->>PG: INSERT attribute_key, INSERT indexed_attribute (BACKFILLING, retain_in_digest true), config_revision +1
    CP-->>UI: 201 BACKFILLING (backfill as SD-09)

    ADM->>UI: Business operation definition idp.login, outcome rule finalStepLabel token-exchange
    UI->>CP: PUT /api/v1/admin/business-operation-definitions/1 {..., outcomeRule {finalStepLabel token-exchange}}
    CP->>CP: BusinessOperationDefinition + OutcomeRule invariants: token-exchange is the step label of oauth.authorization_code
    alt unknown step label or outcomeKey not indexed
        CP-->>UI: 400 UNKNOWN_STEP_LABEL or OUTCOME_KEY_NOT_INDEXED
    else valid
        CP->>PG: UPDATE business_operation_definition SET outcome_rule = finalStepLabel token-exchange
        CP->>PG: UPDATE config_revision +1, COMMIT
        CP->>AUD: audit (outcome rule changed)
        CP-->>UI: 200 business operation definition with outcomeRule
    end

    Note over DC,PM: prospective only, days already DONE are never re-compacted
    DC->>PG: next tick (<= 5 min): load retained keys, transforms, outcome rules, exclusions at start of the next trace day
    PM->>PG: next hourly run: SELECT * FROM retention_policy (new durations, drop gate as SD-16)
```

**Explanation.** All digest-related settings are ordinary configuration rows changed by `tc-control-plane` in one transaction together with a `config_revision` bump, like every other config change. Retention durations are set per tier, so the FULL-tier tables can never get inconsistent retentions. The `tc-maintenance` jobs read the configuration when they start a unit of work, so each trace day is compacted with one consistent configuration and changes never rewrite days already compacted.
