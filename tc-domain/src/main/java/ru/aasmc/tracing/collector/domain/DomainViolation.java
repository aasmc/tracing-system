package ru.aasmc.tracing.collector.domain;

/**
 * A broken business rule: a machine-readable code plus a message for humans.
 *
 * @param code what kind of rule was broken
 * @param message which value broke it and why
 */
public record DomainViolation(ViolationCode code, String message) {}
