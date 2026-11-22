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
        return switch (this) {
            case Changed<S> changed -> changed.to();
            case Unchanged<S> unchanged -> unchanged.from();
            case Rejected<S> rejected -> throw new DomainViolationException(rejected.violation());
        };
    }

    /**
     * The transition from {@code from} to {@code to}: {@code Unchanged} if they are equal, else {@code Changed} if the
     * step is allowed, else {@code Rejected} with {@code rejection}.
     */
    static <S extends Enum<S>> Transition<S> of(S from, S to, boolean allowed, ViolationCode rejection) {
        if (from == to) {
            return new Unchanged<>(from);
        }
        if (allowed) {
            return new Changed<>(from, to);
        }
        return new Rejected<>(from, to, new DomainViolation(rejection, from + " cannot change to " + to));
    }

    /** The state changes. */
    record Changed<S extends Enum<S>>(S from, S to) implements Transition<S> {}

    /** The requested state is the current state, so nothing changes. */
    record Unchanged<S extends Enum<S>>(S from) implements Transition<S> {}

    /** The transition is not allowed; {@code violation} explains why. */
    record Rejected<S extends Enum<S>>(S from, S requested, DomainViolation violation) implements Transition<S> {}
}
