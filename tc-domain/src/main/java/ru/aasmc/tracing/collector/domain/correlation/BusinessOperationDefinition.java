package ru.aasmc.tracing.collector.domain.correlation;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.time.Duration;
import java.util.List;
import ru.aasmc.tracing.collector.domain.Names;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * The definition of one kind of business operation, written by an admin: which attribute values link its traces, how
 * far apart in time they may be, and how its outcome is judged. It is configuration with rules, not a Java type.
 *
 * <p>Example {@code idp.login}: one login is three requests, each its own trace (password check, SMS code check, token
 * exchange). The definition says:
 *
 * <ul>
 *   <li><b>keys</b>: traces carrying the same {@code idp.auth_session_id} (PRIMARY: its first value names the
 *       operation) or the same {@code oauth.authorization_code} (ALIAS: it only links) belong to one login; each key
 *       has a step label, here {@code authentication} and {@code token-exchange};
 *   <li><b>merge window</b>: one day; a session id seen again later is a reused id and starts a new login;
 *   <li><b>outcome rule</b>: a login that reached step {@code token-exchange} succeeded, one that did not was abandoned
 *       (see {@link OutcomeRule}).
 * </ul>
 *
 * <p>Each {@link BusinessOperation} belongs to one definition, and operations of different definitions never merge.
 *
 * <p>Rules: {@code name} follows {@link Names} ({@code INVALID_NAME}); {@code mergeWindow} is positive
 * ({@code INVALID_MERGE_WINDOW}); at least one PRIMARY key, no key id twice and no attribute twice
 * ({@code INVALID_OPERATION_DEFINITION});
 * the outcome rule's {@code finalStepLabel}, if set, is the step label of one of the keys ({@code UNKNOWN_STEP_LABEL});
 * keys are an immutable copy, in business order. Name length, merge-window bounds and the sizes of keys and outcome
 * rule are configured in {@link CorrelationLimits}.
 *
 * @param id id of the definition
 * @param name unique name
 * @param mergeWindow how far in time from an operation a value may still join it; values seen later are treated as
 *     reused (e.g. a recycled session id) and start a new operation
 * @param keys the keys
 * @param outcomeRule the outcome rule, or {@code null} for {@link OutcomeRule#defaultRule()}
 */
public record BusinessOperationDefinition(
        int id, String name, Duration mergeWindow, List<CorrelationKey> keys, OutcomeRule outcomeRule) {

    public BusinessOperationDefinition {
        Names.requireValid(name);
        require(
                mergeWindow != null && mergeWindow.isPositive(),
                ViolationCode.INVALID_MERGE_WINDOW,
                "merge window is not positive: " + mergeWindow);
        keys = List.copyOf(keys);
        require(
                keys.stream().anyMatch(key -> key.role() == KeyRole.PRIMARY),
                ViolationCode.INVALID_OPERATION_DEFINITION,
                "definition " + name + " has no primary key");
        require(
                keys.stream().map(CorrelationKey::id).distinct().count() == keys.size(),
                ViolationCode.INVALID_OPERATION_DEFINITION,
                "definition " + name + " uses a key id twice");
        require(
                keys.stream().map(CorrelationKey::key).distinct().count() == keys.size(),
                ViolationCode.INVALID_OPERATION_DEFINITION,
                "definition " + name + " uses an attribute twice");
        outcomeRule = outcomeRule == null ? OutcomeRule.defaultRule() : outcomeRule;
        String finalStep = outcomeRule.finalStepLabel();
        require(
                finalStep == null || keys.stream().anyMatch(key -> finalStep.equals(key.stepLabel())),
                ViolationCode.UNKNOWN_STEP_LABEL,
                "no key of definition " + name + " has the final step label " + finalStep);
    }
}
