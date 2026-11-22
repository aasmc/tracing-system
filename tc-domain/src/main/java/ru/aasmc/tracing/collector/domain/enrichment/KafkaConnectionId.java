package ru.aasmc.tracing.collector.domain.enrichment;

/**
 * Id of a {@link KafkaConnection} ({@code kafka_connection.id}, 05 §4.6).
 *
 * @param value the id
 */
public record KafkaConnectionId(long value) {}
