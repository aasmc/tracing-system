package ru.aasmc.tracing.collector.domain.trace;

import java.time.Instant;

/**
 * Coordinates of the Kafka record a fragment or an enrichment event came from: used for idempotency of enrichment
 * rows, the DLQ headers and logs (05 §3.7).
 *
 * <p>Invariants ({@code INVALID_KAFKA_COORDINATES}): topic not blank, {@code partition >= 0}, {@code offset >= 0},
 * timestamp not null.
 *
 * @param topic topic name
 * @param partition partition
 * @param offset offset
 * @param timestamp record timestamp
 */
public record KafkaCoordinates(String topic, int partition, long offset, Instant timestamp) {}
