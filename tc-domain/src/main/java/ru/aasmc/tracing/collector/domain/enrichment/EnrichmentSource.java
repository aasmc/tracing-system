package ru.aasmc.tracing.collector.domain.enrichment;

/**
 * A client Kafka topic whose events add attributes to traces.
 *
 * <p>Rules: {@code name} is 3..63 characters of {@code a-z 0-9 . _ -}, starting with a letter or digit
 * ({@code INVALID_NAME}), because it becomes part of the Kafka consumer group id; {@code topic} is a legal Kafka
 * topic name: 1..249 characters of {@code a-z A-Z 0-9 . _ -}, not {@code .} or {@code ..} ({@code INVALID_TOPIC}).
 *
 * @param id source id
 * @param name unique name
 * @param connectionId the cluster the topic lives in
 * @param topic the topic
 * @param status life-cycle status
 */
public record EnrichmentSource(
        EnrichmentSourceId id, String name, KafkaConnectionId connectionId, String topic, SourceStatus status) {

    /**
     * Moves to another status following {@link SourceStatus#transitionTo}.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_SOURCE_TRANSITION}
     */
    public EnrichmentSource moveTo(SourceStatus target) {
        throw new UnsupportedOperationException();
    }
}
