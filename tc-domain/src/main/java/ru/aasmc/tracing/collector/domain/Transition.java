package ru.aasmc.tracing.collector.domain;

/**
 * Result of asking a state machine to move to another state.
 *
 * @param <S> the state type
 */
public sealed interface Transition<S extends Enum<S>> {

    /** The state before the request. */
    S from();

    /**
     * The state after the transition: the new state, or the current one if nothing changed.
     *
     * @throws DomainViolationException if the transition was rejected
     */
    default S resultingStateOrThrow() {
        throw new UnsupportedOperationException();
    }

    /** The state changes. */
    record Changed<S extends Enum<S>>(S from, S to) implements Transition<S> {}

    /** The requested state is the current state, so nothing changes. */
    record Unchanged<S extends Enum<S>>(S from) implements Transition<S> {}

    /** The transition is not allowed; {@code violation} explains why. */
    record Rejected<S extends Enum<S>>(S from, S requested, DomainViolation violation) implements Transition<S> {}
}
