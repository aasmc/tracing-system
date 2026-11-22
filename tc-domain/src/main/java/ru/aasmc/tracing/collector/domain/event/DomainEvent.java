package ru.aasmc.tracing.collector.domain.event;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * Sealed family of internal domain events (05 §5). Events carry ids and counters only, never attribute values
 * (they reach logs and the audit trail). Raised by the services listed in the 05 §5 table.
 */
public sealed interface DomainEvent {

    Instant occurredAt();

    // ---- ingestion ----

    /** {@code tc-ingester} after the batch commit. */
    record TraceFragmentIngested(
            Instant occurredAt,
            String traceIdHex,
            LocalDate traceDay,
            int insertedSpans,
            int duplicateSpans,
            int partition,
            long offset)
            implements DomainEvent {}

    /** {@code reason} is a {@code DeadLetterReason} name. */
    record FragmentDeadLettered(Instant occurredAt, String reason, int partition, long offset) implements DomainEvent {}

    /** {@code reason}: EXPIRED, TRACE_TOO_LARGE, SELF_TRACE. */
    record SpansDropped(Instant occurredAt, String reason, int count) implements DomainEvent {}

    // ---- enrichment ----

    record EnrichmentApplied(Instant occurredAt, long sourceId, int revision, String traceIdHex, List<String> keys)
            implements DomainEvent {}

    record EnrichmentEventRejected(Instant occurredAt, long sourceId, String reason, boolean deadLettered)
            implements DomainEvent {}

    // ---- correlation ----

    record CorrelationObserved(Instant occurredAt, int typeId, int keyId, String traceIdHex, short origin)
            implements DomainEvent {}

    record BusinessOperationCreated(Instant occurredAt, UUID operationId, int typeId, boolean provisional)
            implements DomainEvent {}

    record TraceJoinedBusinessOperation(Instant occurredAt, UUID operationId, String traceIdHex)
            implements DomainEvent {}

    record BusinessOperationMerged(Instant occurredAt, UUID survivorId, List<UUID> mergedIds, int traceCount)
            implements DomainEvent {}

    // ---- configuration (control plane, audited) ----

    record EnrichmentSourceActivated(Instant occurredAt, long sourceId, int revision, String actor)
            implements DomainEvent {}

    record EnrichmentSourcePaused(Instant occurredAt, long sourceId, String actor) implements DomainEvent {}

    record EnrichmentSourceRevised(Instant occurredAt, long sourceId, int newRevision, String actor)
            implements DomainEvent {}

    record EnrichmentSourceDeleted(Instant occurredAt, long sourceId, String actor) implements DomainEvent {}

    record IndexedAttributeRequested(Instant occurredAt, String key, String actor) implements DomainEvent {}

    record IndexedAttributeActivated(Instant occurredAt, String key, long rowsBackfilled) implements DomainEvent {}

    record IndexedAttributeDisabled(Instant occurredAt, String key, String actor) implements DomainEvent {}

    record ConfigRevisionBumped(Instant occurredAt, long revision) implements DomainEvent {}

    record RetentionTiersChanged(Instant occurredAt, int fullDays, int digestDays, String actor)
            implements DomainEvent {}

    // ---- retention ----

    record PartitionCreated(Instant occurredAt, String table, String partition) implements DomainEvent {}

    record PartitionDropped(Instant occurredAt, String table, String partition) implements DomainEvent {}

    /** The drop gate is closed because {@code blockingTraceDay} is not compacted (C6). */
    record PartitionDropDeferred(Instant occurredAt, String table, String partition, LocalDate blockingTraceDay)
            implements DomainEvent {}

    record TraceDayCompacted(
            Instant occurredAt, LocalDate traceDay, long tracesDigested, long tracesExcluded, Duration elapsed)
            implements DomainEvent {}

    record BusinessOperationCompacted(Instant occurredAt, UUID operationId, int typeId, short outcome, int traceCount)
            implements DomainEvent {}
}
