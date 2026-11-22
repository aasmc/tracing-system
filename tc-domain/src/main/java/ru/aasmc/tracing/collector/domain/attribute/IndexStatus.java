package ru.aasmc.tracing.collector.domain.attribute;

import ru.aasmc.tracing.collector.domain.Transition;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * Life cycle of a searchable attribute key: BACKFILLING (old data is being indexed) → ACTIVE; any status →
 * DISABLED; DISABLED → BACKFILLING to enable it again. This enum knows only which status changes are allowed;
 * {@link IndexedAttribute} also refuses to disable a built-in key and a key whose values are kept in trace digests.
 */
public enum IndexStatus {
    BACKFILLING,
    ACTIVE,
    DISABLED;

    /**
     * Whether this status may change to {@code target}: {@code Unchanged} if {@code target} is the current status,
     * {@code Changed} if the step is allowed, otherwise {@code Rejected} with {@code INVALID_INDEX_TRANSITION}.
     */
    public Transition<IndexStatus> transitionTo(IndexStatus target) {
        boolean allowed = switch (this) {
            case BACKFILLING -> target == ACTIVE || target == DISABLED;
            case ACTIVE -> target == DISABLED;
            case DISABLED -> target == BACKFILLING;
        };
        return Transition.of(this, target, allowed, ViolationCode.INVALID_INDEX_TRANSITION);
    }
}
