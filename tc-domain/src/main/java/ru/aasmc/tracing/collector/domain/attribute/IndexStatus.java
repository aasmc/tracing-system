package ru.aasmc.tracing.collector.domain.attribute;

import ru.aasmc.tracing.collector.domain.Transition;

/**
 * Status of an indexed attribute (05 §4.9 state diagram).
 *
 * <p>Allowed: BACKFILLING → ACTIVE, BACKFILLING → DISABLED, ACTIVE → DISABLED, DISABLED → BACKFILLING
 * (re-enable). Requests for the current state are {@link Transition.Unchanged}. Everything else is rejected with
 * {@code INVALID_INDEX_TRANSITION}.
 */
public enum IndexStatus {
    BACKFILLING,
    ACTIVE,
    DISABLED;

    public Transition<IndexStatus> transitionTo(IndexStatus target) {
        throw new UnsupportedOperationException();
    }
}
