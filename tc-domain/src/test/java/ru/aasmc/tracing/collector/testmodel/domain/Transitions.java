package ru.aasmc.tracing.collector.testmodel.domain;

import static org.assertj.core.api.Assertions.assertThat;

import ru.aasmc.tracing.collector.domain.DomainViolation;
import ru.aasmc.tracing.collector.domain.Transition;
import ru.aasmc.tracing.collector.domain.ViolationCode;

public final class Transitions {

    private Transitions() {}

    /**
     * Asserts the result of asking a state machine in {@code from} to move to {@code to}: {@code Unchanged} or
     * {@code Changed} if {@code allowed}, else {@code Rejected} with {@code rejection}, which
     * {@code resultingStateOrThrow} throws.
     */
    public static <S extends Enum<S>> void assertTransition(
            Transition<S> actual, S from, S to, boolean allowed, ViolationCode rejection) {
        if (allowed) {
            assertThat(actual)
                    .isEqualTo(from == to ? new Transition.Unchanged<>(from) : new Transition.Changed<>(from, to));
            assertThat(actual.resultingStateOrThrow()).isEqualTo(to);
            return;
        }
        assertThat(actual)
                .usingRecursiveComparison()
                .withStrictTypeChecking()
                .ignoringFields("violation.message")
                .isEqualTo(new Transition.Rejected<>(from, to, new DomainViolation(rejection, "")));
        Violations.assertViolation(rejection, actual::resultingStateOrThrow);
    }
}
