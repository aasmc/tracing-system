package ru.aasmc.tracing.collector.domain.enrichment;

/**
 * Id of a configured connection to a client's Kafka cluster.
 *
 * @param value the id
 */
public record KafkaConnectionId(long value) {}
