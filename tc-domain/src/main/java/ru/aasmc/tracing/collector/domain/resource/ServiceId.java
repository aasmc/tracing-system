package ru.aasmc.tracing.collector.domain.resource;

/**
 * Dictionary id of a {@code service.name} ({@code tc.service}, 05 §3.5). Invariant: {@code value > 0}.
 *
 * @param value the id
 */
public record ServiceId(int value) {}
