package ru.aasmc.tracing.collector.testmodel.domain;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import ru.aasmc.tracing.collector.domain.DomainViolationException;
import ru.aasmc.tracing.collector.domain.ViolationCode;

public final class Violations {

    private Violations() {}

    /** Asserts that {@code call} throws a {@link DomainViolationException} with {@code code}; reports anything else. */
    public static void assertViolation(ViolationCode code, ThrowingCallable call) {
        try {
            call.call();
        } catch (DomainViolationException e) {
            assertThat(e.code()).as("violation code (%s)", e.getMessage()).isEqualTo(code);
            return;
        } catch (Throwable other) {
            throw new AssertionError("Expected DomainViolationException " + code + " but got " + other, other);
        }
        throw new AssertionError("Expected DomainViolationException " + code + " but nothing was thrown");
    }
}
