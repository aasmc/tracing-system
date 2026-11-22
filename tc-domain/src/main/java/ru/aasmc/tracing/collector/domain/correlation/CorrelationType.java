package ru.aasmc.tracing.collector.domain.correlation;

import java.time.Duration;
import java.util.List;

/**
 * A kind of business operation whose traces are linked by shared attribute values, e.g. {@code idp.login}: the
 * first-factor, second-factor and token-exchange requests of one login are separate traces that share a session id
 * or an authorization code.
 *
 * <p>Rules: {@code name} is 3..63 characters of {@code a-z 0-9 . _ -}, starting with a letter or digit
 * ({@code INVALID_NAME}); a {@code null} {@code mergeWindow} means {@link #DEFAULT_WINDOW}, otherwise it lies in [{@link #MIN_WINDOW}, {@link #MAX_WINDOW}]
 * ({@code INVALID_MERGE_WINDOW}); at least one PRIMARY key and no attribute twice ({@code INVALID_CORRELATION_TYPE});
 * the outcome rule's {@code finalStepLabel}, if set, is the step label of one of the keys ({@code UNKNOWN_STEP_LABEL});
 * keys are an immutable copy, in business order.
 *
 * @param id type id
 * @param name unique name
 * @param mergeWindow how far in time from an operation a value may still join it; values seen later are treated as
 *     reused (e.g. a recycled session id) and start a new operation
 * @param keys the keys
 * @param outcomeRule the outcome rule, or {@code null} for {@link OutcomeRule#defaultRule()}
 */
public record CorrelationType(
        int id, String name, Duration mergeWindow, List<CorrelationKey> keys, OutcomeRule outcomeRule) {

    public static final Duration MIN_WINDOW = Duration.ofMinutes(1);
    public static final Duration MAX_WINDOW = Duration.ofDays(30);
    public static final Duration DEFAULT_WINDOW = Duration.ofDays(1);
}
