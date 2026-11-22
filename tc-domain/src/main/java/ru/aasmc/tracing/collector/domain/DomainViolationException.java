package ru.aasmc.tracing.collector.domain;

import java.io.Serial;

/**
 * Thrown when a domain object would be created or changed into an invalid state. It extends
 * {@link IllegalArgumentException} because every such case is an invalid argument.
 */
public final class DomainViolationException extends IllegalArgumentException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final transient DomainViolation violation;

    public DomainViolationException(DomainViolation violation) {
        super(violation.code() + ": " + violation.message());
        this.violation = violation;
    }

    public DomainViolationException(ViolationCode code, String message) {
        this(new DomainViolation(code, message));
    }

    public DomainViolation violation() {
        return violation;
    }

    public ViolationCode code() {
        return violation.code();
    }
}
