package ru.aasmc.tracing.collector.domain;

import java.io.Serial;

/**
 * Thrown by constructors and aggregate methods when a domain invariant is broken (09 §2.1 item 1a-10).
 *
 * <p>It extends {@link IllegalArgumentException}, so code written against the 05 sketches (which throw
 * {@code IllegalArgumentException}) keeps working; callers that need the code catch this type and read
 * {@link #violation()}. Policies that can legitimately say "no" return sealed results instead (e.g.
 * {@link Transition}).
 */
public final class DomainViolationException extends IllegalArgumentException {

    @Serial
    private static final long serialVersionUID = 1L;

    /** Not serialized meaningfully: the exception never leaves the JVM. */
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
