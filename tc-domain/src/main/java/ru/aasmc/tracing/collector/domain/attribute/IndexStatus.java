package ru.aasmc.tracing.collector.domain.attribute;

import ru.aasmc.tracing.collector.domain.Transition;

/**
 * Life cycle of a searchable attribute key: BACKFILLING (old data is being indexed) → ACTIVE; any status →
 * DISABLED; DISABLED → BACKFILLING to enable it again.
 */
public enum IndexStatus {
    BACKFILLING,
    ACTIVE,
    DISABLED;

    /** Asking for the current status is {@code Unchanged}; a forbidden step is rejected with {@code INVALID_INDEX_TRANSITION}. */
    public Transition<IndexStatus> transitionTo(IndexStatus target) {
        throw new UnsupportedOperationException();
    }
}
