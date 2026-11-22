package ru.aasmc.tracing.collector.domain.enrichment;

import java.util.function.Function;

/**
 * The state of reading one mapped value, step by step: not there ({@link Missing}), read so far ({@link Found}), or
 * failed with a reason ({@link Failed}).
 *
 * <p>Steps are chained with {@link #then}: a step runs only on a found value, and a missing or failed reading passes
 * through the remaining steps unchanged. Example for a LONG mapping of payload field {@code /attempts}:
 *
 * <pre>{@code
 * read /attempts   →  Found(NumberScalar 3)   then convert  →  Found(LongValue 3)
 * read /attempts   →  Found(StringScalar "x") then convert  →  Failed(TYPE_MISMATCH)
 * read /attempts   →  Missing                 then convert  →  Missing  (requiredIf(true) →  Failed(MISSING_REQUIRED_FIELD))
 * }</pre>
 *
 * @param <T> the type of the value read so far
 */
sealed interface FieldReading<T> {

    record Missing<T>() implements FieldReading<T> {}

    record Found<T>(T value) implements FieldReading<T> {}

    record Failed<T>(ExtractionFailure reason) implements FieldReading<T> {}

    static <T> FieldReading<T> found(T value) {
        return new Found<>(value);
    }

    static <T> FieldReading<T> missing() {
        return new Missing<>();
    }

    static <T> FieldReading<T> failed(ExtractionFailure reason) {
        return new Failed<>(reason);
    }

    /** Applies {@code step} to a found value; a missing or failed reading stays as it is. */
    default <R> FieldReading<R> then(Function<T, FieldReading<R>> step) {
        return switch (this) {
            case Found<T>(T value) -> step.apply(value);
            case Missing<T> _ -> new Missing<>();
            case Failed<T>(ExtractionFailure reason) -> new Failed<>(reason);
        };
    }

    /** A missing value becomes {@link ExtractionFailure#MISSING_REQUIRED_FIELD} if {@code required}. */
    default FieldReading<T> requiredIf(boolean required) {
        return required && this instanceof Missing<T> ? new Failed<>(ExtractionFailure.MISSING_REQUIRED_FIELD) : this;
    }
}
