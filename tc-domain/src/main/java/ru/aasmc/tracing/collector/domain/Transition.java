package ru.aasmc.tracing.collector.domain;

/**
 * Sealed result of asking a state machine to move to another state (09 §2.1 items 1a-4, 1a-5, 1a-7).
 *
 * <p>Used by {@code SourceStatus}, {@code IndexStatus} and {@code CompactionStatus}. A transition is either a real
 * change, an idempotent no-op (the machine is already in the requested state and that is allowed), or a rejection
 * that carries the violation the control plane reports.
 *
 * @param <S> the state enum
 */
public sealed interface Transition<S extends Enum<S>> {

    /** State before the transition. */
    S from();

    /** {@code true} for {@link Changed} and {@link Unchanged}. */
    default boolean allowed() {
        throw new UnsupportedOperationException();
    }

    /**
     * State after the transition.
     *
     * @throws DomainViolationException with the carried violation if the transition was {@link Rejected}
     */
    default S targetOrThrow() {
        throw new UnsupportedOperationException();
    }

    /** The state changes from {@code from} to {@code to}; {@code from != to}. */
    record Changed<S extends Enum<S>>(S from, S to) implements Transition<S> {}

    /** The requested state equals the current one and the machine treats the request as idempotent. */
    record Unchanged<S extends Enum<S>>(S from) implements Transition<S> {}

    /** The machine does not allow {@code from -> requested}. */
    record Rejected<S extends Enum<S>>(S from, S requested, DomainViolation violation) implements Transition<S> {}
}
