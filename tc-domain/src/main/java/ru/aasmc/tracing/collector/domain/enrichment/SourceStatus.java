package ru.aasmc.tracing.collector.domain.enrichment;

import ru.aasmc.tracing.collector.domain.Transition;

/**
 * Life cycle of an enrichment source: DRAFT → ACTIVE ⇄ PAUSED; DRAFT or PAUSED → DELETED. An ACTIVE source must be
 * paused before it is deleted, so that its consumers stop first. DELETED is final.
 */
public enum SourceStatus {
    DRAFT,
    ACTIVE,
    PAUSED,
    DELETED;

    /** Asking for the current status is {@code Unchanged}; a forbidden step is rejected with {@code INVALID_SOURCE_TRANSITION}. */
    public Transition<SourceStatus> transitionTo(SourceStatus target) {
        throw new UnsupportedOperationException();
    }
}
