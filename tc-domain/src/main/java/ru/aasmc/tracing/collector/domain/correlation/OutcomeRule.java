package ru.aasmc.tracing.collector.domain.correlation;

import java.util.Set;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;

/**
 * Optional rule that derives a business operation's outcome at compaction (05 §4.10.1, 03 §5.9 O1–O4), stored in
 * {@code correlation_type.outcome_rule} jsonb. Evaluated by {@link OperationOutcomeEvaluator}.
 *
 * <p>Invariants ({@code INVALID_OUTCOME_VALUES}): {@code null} value sets become empty immutable sets; at most
 * {@link #MAX_VALUES} values per set, each 1..{@link #MAX_VALUE_LENGTH} characters; the sets are disjoint; values
 * need an {@code outcomeKey}. {@code finalStepLabel} is {@code null} or 1..64 characters ({@code
 * INVALID_OUTCOME_RULE}). That the outcome key is indexed (O2) and that the label belongs to the type (O4) are
 * checked by the control plane and by {@link CorrelationType}.
 *
 * @param outcomeKey attribute carrying the business outcome, or {@code null}
 * @param successValues values meaning SUCCESS
 * @param failureValues values meaning FAILED
 * @param finalStepLabel step label whose presence means the operation completed, or {@code null}
 */
public record OutcomeRule(
        AttributeKey outcomeKey, Set<String> successValues, Set<String> failureValues, String finalStepLabel) {

    public static final int MAX_VALUES = 50;
    public static final int MAX_VALUE_LENGTH = 512;

    /** A rule with no fields: only "any error → FAILED, else SUCCESS" applies. */
    public static OutcomeRule none() {
        throw new UnsupportedOperationException();
    }
}
