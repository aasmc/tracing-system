package ru.aasmc.tracing.collector.domain;

import java.io.Serial;

/**
 * Thrown when a domain object would be created or changed into an invalid state. It extends
 * {@link IllegalArgumentException} because every such case is an invalid argument.
 */
public final class DomainViolationException extends IllegalArgumentException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final ViolationCode code;

    public DomainViolationException(DomainViolation violation) {
        this(violation.code(), violation.message());
    }

    public DomainViolationException(ViolationCode code, String message) {
        super(code + ": " + message);
        this.code = code;
    }

    /** Throws a violation with {@code code} and {@code message} unless {@code condition} holds. */
    public static void require(boolean condition, ViolationCode code, String message) {
        if (!condition) {
            throw new DomainViolationException(code, message);
        }
    }

    public ViolationCode code() {
        return code;
    }
}
