package ru.aasmc.tracing.collector.domain.correlation;

import java.util.List;
import java.util.Set;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;

/**
 * Derives how a business operation ended from its traces.
 *
 * <p>Rules ({@code INVALID_OUTCOME_RULE}): {@code null} value sets become empty; each set has at most
 * {@value #MAX_VALUES} values of 1..{@value #MAX_VALUE_LENGTH} characters; the sets do not overlap; values need an
 * {@code outcomeKey}; {@code finalStepLabel} is {@code null} or 1..64 characters.
 *
 * @param outcomeKey attribute that states the outcome, e.g. {@code login.result}, or {@code null}
 * @param successValues values of {@code outcomeKey} meaning success
 * @param failureValues values of {@code outcomeKey} meaning failure
 * @param finalStepLabel the step that only a completed operation reaches, or {@code null}
 */
public record OutcomeRule(
        AttributeKey outcomeKey, Set<String> successValues, Set<String> failureValues, String finalStepLabel) {

    public static final int MAX_VALUES = 50;
    public static final int MAX_VALUE_LENGTH = 512;

    /** The rule of a type that defines none: FAILED if any trace has an error, else SUCCESS. */
    public static OutcomeRule defaultRule() {
        throw new UnsupportedOperationException();
    }

    /**
     * Evaluates the first rule that applies:
     *
     * <ol>
     *   <li>if some trace has a value of {@code outcomeKey}, the latest such trace (by start, then larger trace id)
     *       decides: SUCCESS or FAILED if the value is in a set, else UNKNOWN; that value is the deciding value;
     *   <li>if any trace has an error: FAILED;
     *   <li>if there is a {@code finalStepLabel}: SUCCESS if some trace carries it, else INCOMPLETE;
     *   <li>otherwise SUCCESS.
     * </ol>
     *
     * Without traces the outcome is UNKNOWN.
     */
    public OutcomeEvaluation evaluate(List<MemberTrace> members) {
        throw new UnsupportedOperationException();
    }
}
