package ru.aasmc.tracing.collector.domain.enrichment;

/**
 * Processes only events whose payload value at {@code jsonPointer} equals {@code equalsValue} (05 §4.7.1, V12).
 *
 * <p>Invariants ({@code INVALID_FILTER}): both components not null.
 *
 * @param jsonPointer where to read
 * @param equalsValue the scalar to compare with (its JSON text form)
 */
public record EventFilter(JsonPointerExpr jsonPointer, String equalsValue) {}
