package ru.aasmc.tracing.collector.domain.correlation;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;

/**
 * Decides how a business operation ended, e.g. whether a login succeeded, was rejected or was abandoned.
 *
 * <p>The traces do not say so directly: a rejected password is usually a healthy trace that returns "denied", and an
 * abandoned login has no failing trace at all, its last step is simply missing. So each
 * {@link BusinessOperationDefinition} says how to read the outcome, with up to three optional parts that
 * {@link #evaluate} tries in order:
 *
 * <ol>
 *   <li>an attribute that states the result, with the values meaning success and failure, e.g. {@code login.result}:
 *       {@code OK} is SUCCESS, {@code DENIED} is FAILED;
 *   <li>(always) an error in any trace means FAILED;
 *   <li>the step that only a completed operation reaches, e.g. {@code token-exchange}: present means SUCCESS, absent
 *       means INCOMPLETE.
 * </ol>
 *
 * <p>Example with {@code finalStepLabel = token-exchange}: a login whose traces include one carrying the
 * authorization code (step {@code token-exchange}) and no errors is SUCCESS; a login with only the password check
 * (step {@code authentication}) is INCOMPLETE; any error makes it FAILED. A trace performs the step labels of the keys
 * it carries. The outcome is stored in the long-term digest of the operation, so it is still known after the traces
 * are deleted.
 *
 * <p>Rules ({@code INVALID_OUTCOME_RULE}): {@code null} value sets become empty; values are not blank; the sets do not
 * overlap; values need an {@code outcomeKey}; {@code finalStepLabel} is {@code null} or not blank. The number and
 * length of values and the label length are configured in {@link CorrelationLimits}.
 *
 * @param outcomeKey attribute that states the outcome, e.g. {@code login.result}, or {@code null}
 * @param successValues values of {@code outcomeKey} meaning success
 * @param failureValues values of {@code outcomeKey} meaning failure
 * @param finalStepLabel the step that only a completed operation reaches, or {@code null}
 */
public record OutcomeRule(
        AttributeKey outcomeKey, Set<String> successValues, Set<String> failureValues, String finalStepLabel) {

    /** Latest start last; of traces that started together, the larger trace id last. */
    private static final Comparator<MemberTrace> BY_START =
            Comparator.comparing(MemberTrace::startTime).thenComparing(MemberTrace::traceId);

    public OutcomeRule {
        successValues = successValues == null ? Set.of() : Set.copyOf(successValues);
        failureValues = failureValues == null ? Set.of() : Set.copyOf(failureValues);
        require(
                Stream.concat(successValues.stream(), failureValues.stream()).noneMatch(String::isBlank),
                ViolationCode.INVALID_OUTCOME_RULE,
                "blank outcome value");
        require(
                Collections.disjoint(successValues, failureValues),
                ViolationCode.INVALID_OUTCOME_RULE,
                "a value means both success and failure");
        require(
                outcomeKey != null || (successValues.isEmpty() && failureValues.isEmpty()),
                ViolationCode.INVALID_OUTCOME_RULE,
                "outcome values need an outcome key");
        require(
                finalStepLabel == null || !finalStepLabel.isBlank(),
                ViolationCode.INVALID_OUTCOME_RULE,
                "blank final step label");
    }

    /** The rule of a definition that defines none: FAILED if any trace has an error, else SUCCESS. */
    public static OutcomeRule defaultRule() {
        return new OutcomeRule(null, Set.of(), Set.of(), null);
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
     *
     * @param maxDecidingValueLength the deciding value is cut to this many characters (code points, so a character
     *     outside the Basic Multilingual Plane, such as an emoji, is never split)
     */
    public OutcomeEvaluation evaluate(List<MemberTrace> members, int maxDecidingValueLength) {
        if (members.isEmpty()) {
            return new OutcomeEvaluation(OperationOutcome.UNKNOWN, null);
        }
        Optional<String> latestOutcomeValue = members.stream()
                .filter(member -> member.outcomeValue() != null)
                .max(BY_START)
                .map(MemberTrace::outcomeValue);
        if (latestOutcomeValue.isPresent()) {
            return evaluateOutcomeValue(latestOutcomeValue.get(), maxDecidingValueLength);
        }
        if (members.stream().anyMatch(member -> member.errorCount() > 0)) {
            return new OutcomeEvaluation(OperationOutcome.FAILED, null);
        }
        if (finalStepLabel != null) {
            boolean reached =
                    members.stream().anyMatch(member -> member.stepLabels().contains(finalStepLabel));
            return new OutcomeEvaluation(reached ? OperationOutcome.SUCCESS : OperationOutcome.INCOMPLETE, null);
        }
        return new OutcomeEvaluation(OperationOutcome.SUCCESS, null);
    }

    private OutcomeEvaluation evaluateOutcomeValue(String value, int maxDecidingValueLength) {
        OperationOutcome outcome = successValues.contains(value)
                ? OperationOutcome.SUCCESS
                : failureValues.contains(value) ? OperationOutcome.FAILED : OperationOutcome.UNKNOWN;
        String decidingValue = value.codePointCount(0, value.length()) > maxDecidingValueLength
                ? value.substring(0, value.offsetByCodePoints(0, maxDecidingValueLength))
                : value;
        return new OutcomeEvaluation(outcome, decidingValue);
    }
}
