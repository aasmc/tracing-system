package ru.aasmc.tracing.collector.domain.enrichment;

import ru.aasmc.tracing.collector.domain.Transition;

/**
 * Lifecycle of an enrichment source (05 §4.7.2 state diagram).
 *
 * <p>Allowed: DRAFT → ACTIVE, PAUSED → ACTIVE, ACTIVE → PAUSED, DRAFT → DELETED, PAUSED → DELETED. Idempotent
 * ({@link Transition.Unchanged}): ACTIVE → ACTIVE, PAUSED → PAUSED, DELETED → DELETED. Rejected: ACTIVE → DELETED
 * ({@code SOURCE_ACTIVE}); DRAFT → PAUSED ({@code SOURCE_NOT_ACTIVE}); anything from DELETED ({@code
 * SOURCE_DELETED}); any → DRAFT ({@code SOURCE_NOT_ACTIVE} from PAUSED/ACTIVE, Unchanged from DRAFT).
 */
public enum SourceStatus {
    DRAFT,
    ACTIVE,
    PAUSED,
    DELETED;

    public Transition<SourceStatus> transitionTo(SourceStatus target) {
        throw new UnsupportedOperationException();
    }
}
