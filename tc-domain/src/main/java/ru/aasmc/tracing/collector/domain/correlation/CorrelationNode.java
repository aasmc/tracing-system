package ru.aasmc.tracing.collector.domain.correlation;

/**
 * A node of the union-find forest: one {@code (type, value)} (05 §4.11).
 *
 * @param correlationTypeId the type
 * @param value the correlation value
 */
public record CorrelationNode(int correlationTypeId, String value) {}
