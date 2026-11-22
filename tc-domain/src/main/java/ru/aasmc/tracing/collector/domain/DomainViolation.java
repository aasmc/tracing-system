package ru.aasmc.tracing.collector.domain;

/**
 * One broken domain rule: a {@link ViolationCode} plus a human-readable message (09 §2.1 item 1a-10).
 *
 * <p>Invariants: {@code code} is not null; {@code message} is not blank. Messages never contain attribute values
 * or payload fragments, because they reach logs and API responses (05 §7.3).
 *
 * @param code the machine-readable code
 * @param message the explanation for humans
 */
public record DomainViolation(ViolationCode code, String message) {}
