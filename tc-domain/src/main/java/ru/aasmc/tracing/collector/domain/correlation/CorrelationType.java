package ru.aasmc.tracing.collector.domain.correlation;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * A named business-operation family, e.g. {@code idp.login} (05 §4.10, 03 §5.9 O4, O5).
 *
 * <p>Invariants: {@code name} matches {@code EnrichmentSource.NAME_PATTERN} ({@code INVALID_NAME}); a {@code null}
 * merge window becomes {@link #DEFAULT_WINDOW}; the window is in {@code [MIN_WINDOW, MAX_WINDOW]} ({@code
 * INVALID_MERGE_WINDOW}); at least one PRIMARY key and no attribute key twice ({@code INVALID_CORRELATION_TYPE});
 * {@code outcomeRule.finalStepLabel}, if set, is the step label of one of the keys ({@code UNKNOWN_STEP_LABEL}); keys
 * are an immutable copy in declaration (business) order. The tier-dependent bound of O5 ({@code window <= fullDays -
 * 1 day}) is {@code RetentionTiers.requireMergeWindowFits}.
 *
 * @param id {@code correlation_type.id}
 * @param name unique name
 * @param description free text
 * @param mergeWindow how far from an operation an observation may still join it
 * @param status ACTIVE or DISABLED
 * @param keys keys in business order
 * @param outcomeRule rule or {@code null} (no rule: SUCCESS unless a member errored)
 */
public record CorrelationType(
        int id,
        String name,
        String description,
        Duration mergeWindow,
        CorrelationTypeStatus status,
        List<CorrelationKey> keys,
        OutcomeRule outcomeRule) {

    public static final Duration MIN_WINDOW = Duration.ofMinutes(1);
    public static final Duration MAX_WINDOW = Duration.ofDays(30);
    public static final Duration DEFAULT_WINDOW = Duration.ofDays(1);

    /** The key of this type registered for an {@code attribute_key.id}. */
    public Optional<CorrelationKey> keyFor(int attributeKeyId) {
        throw new UnsupportedOperationException();
    }

    /**
     * Step label of a member trace linked by the given attribute keys: the label of the key declared <em>last</em>
     * among them that has a label (05 §4.14.2); empty if none has one.
     */
    public Optional<String> stepLabelOf(Collection<Integer> linkingAttributeKeyIds) {
        throw new UnsupportedOperationException();
    }

    /** The largest merge window of a set of types (input of R2); {@link Duration#ZERO} for none. */
    public static Duration maxMergeWindow(Collection<CorrelationType> types) {
        throw new UnsupportedOperationException();
    }
}
