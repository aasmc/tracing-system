package ru.aasmc.tracing.collector.domain.enrichment;

import java.util.UUID;

/**
 * Id of a configured connection to a client's Kafka cluster.
 *
 * @param value the id
 */
public record KafkaConnectionId(UUID value) {}
