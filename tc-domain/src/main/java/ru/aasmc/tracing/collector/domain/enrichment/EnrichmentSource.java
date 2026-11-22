package ru.aasmc.tracing.collector.domain.enrichment;

/**
 * Root of the enrichment-source aggregate (05 §4.7.2). Lifecycle transitions and new revisions are committed
 * together with a {@code config_revision} bump by the control plane; activation pre-conditions that need I/O
 * (connection reachable, topic exists, ACLs) are checked there, not here.
 *
 * <p>Invariants: {@code name} matches {@link #NAME_PATTERN} ({@code INVALID_NAME}; 3..63 characters, V1 — deviation
 * from 05, which allowed 2); {@code topic} is a legal Kafka topic name, not {@code .} or {@code ..} ({@code
 * INVALID_TOPIC}); {@code currentRevision >= 1} ({@code INVALID_REVISION}).
 *
 * @param id source id
 * @param name unique name, part of the consumer group id
 * @param connectionId cluster connection
 * @param topic client topic
 * @param status lifecycle status
 * @param currentRevision revision processed by running containers
 * @param createdBy actor
 */
public record EnrichmentSource(
        EnrichmentSourceId id,
        String name,
        KafkaConnectionId connectionId,
        String topic,
        SourceStatus status,
        int currentRevision,
        String createdBy) {

    /** 03 §5.4 V1; also used for connection and correlation-type names. */
    public static final String NAME_PATTERN = "^[a-z0-9][a-z0-9._-]{2,62}$";

    /** 03 §5.4 V2 (plus: not {@code .} or {@code ..}). */
    public static final String TOPIC_PATTERN = "^[a-zA-Z0-9._-]{1,249}$";

    /** A new DRAFT source with revision 1. */
    public static EnrichmentSource draft(
            EnrichmentSourceId id, String name, KafkaConnectionId connectionId, String topic, String createdBy) {
        throw new UnsupportedOperationException();
    }

    /** Consumer group in the client cluster: {@code tc-enricher.<name>}, stable across revisions. */
    public String consumerGroupId() {
        throw new UnsupportedOperationException();
    }

    /** Container id: {@code enrichment-source-<id>-r<revision>}; changes with the revision. */
    public String containerId() {
        throw new UnsupportedOperationException();
    }

    /**
     * → ACTIVE (idempotent when ACTIVE).
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException with the code of
     *     {@link SourceStatus#transitionTo}
     */
    public EnrichmentSource activate() {
        throw new UnsupportedOperationException();
    }

    /** → PAUSED (idempotent when PAUSED); see {@link #activate()} for errors. */
    public EnrichmentSource pause() {
        throw new UnsupportedOperationException();
    }

    /** → DELETED (idempotent when DELETED); see {@link #activate()} for errors. */
    public EnrichmentSource delete() {
        throw new UnsupportedOperationException();
    }

    /**
     * The next revision; any non-deleted source may be revised.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code SOURCE_DELETED}
     */
    public EnrichmentSource revise() {
        throw new UnsupportedOperationException();
    }

    /**
     * 05 §4.7.2 invariant 2: an update may not change topic or connection.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code TOPIC_IMMUTABLE}
     */
    public void requireSameTopicAndConnection(String newTopic, KafkaConnectionId newConnectionId) {
        throw new UnsupportedOperationException();
    }
}
