package ru.aasmc.tracing.collector.domain.attribute;

import ru.aasmc.tracing.collector.domain.Transition;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * Life cycle of a searchable attribute key: BACKFILLING (old data is being indexed) → ACTIVE; any status →
 * DISABLED; DISABLED → BACKFILLING to enable it again. These are the steps of the status alone:
 * {@link IndexedAttribute} also refuses to disable a built-in key and a key whose values are kept in trace digests.
 */
public enum IndexStatus {
    BACKFILLING,
    ACTIVE,
    DISABLED;

    /** Asking for the current status is {@code Unchanged}; a forbidden step is rejected with {@code INVALID_INDEX_TRANSITION}. */
    public Transition<IndexStatus> transitionTo(IndexStatus target) {
        boolean allowed = switch (this) {
            case BACKFILLING -> target == ACTIVE || target == DISABLED;
            case ACTIVE -> target == DISABLED;
            case DISABLED -> target == BACKFILLING;
        };
        return Transition.of(this, target, allowed, ViolationCode.INVALID_INDEX_TRANSITION);
    }
}
