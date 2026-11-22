package ru.aasmc.tracing.collector.domain.enrichment;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.util.Set;
import java.util.regex.Pattern;
import ru.aasmc.tracing.collector.domain.Names;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * A client Kafka topic whose events add attributes to traces.
 *
 * <p>Rules: {@code name} follows {@link Names} ({@code INVALID_NAME}), because it becomes part of the Kafka consumer
 * group id; its maximum length is configured in {@link EnrichmentLimits}. {@code topic} is a legal Kafka topic name:
 * 1..249 characters of {@code a-z A-Z 0-9 . _ -}, not {@code .} or {@code ..} ({@code INVALID_TOPIC}); these are
 * Kafka's own rules.
 *
 * @param id source id
 * @param name unique name
 * @param connectionId the cluster the topic lives in
 * @param topic the topic
 * @param status life-cycle status
 */
public record EnrichmentSource(
        EnrichmentSourceId id, String name, KafkaConnectionId connectionId, String topic, SourceStatus status) {

    private static final Pattern TOPIC_CHARACTERS = Pattern.compile("[a-zA-Z0-9._-]{1,249}");
    private static final Set<String> FORBIDDEN_TOPICS = Set.of(".", "..");

    public EnrichmentSource {
        Names.requireValid(name);
        require(
                topic != null && TOPIC_CHARACTERS.matcher(topic).matches() && !FORBIDDEN_TOPICS.contains(topic),
                ViolationCode.INVALID_TOPIC,
                "not a Kafka topic name: " + topic);
    }

    /**
     * Moves to another status following {@link SourceStatus#transitionTo}.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_SOURCE_TRANSITION}
     */
    public EnrichmentSource moveTo(SourceStatus target) {
        return new EnrichmentSource(
                id, name, connectionId, topic, status.transitionTo(target).resultingStateOrThrow());
    }
}
