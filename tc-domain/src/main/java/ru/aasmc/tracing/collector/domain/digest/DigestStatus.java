package ru.aasmc.tracing.collector.domain.digest;

import ru.aasmc.tracing.collector.domain.Transition;

/** Life cycle of digesting one day: PENDING → RUNNING → DONE, or RUNNING → FAILED → RUNNING to retry. */
public enum DigestStatus {
    PENDING,
    RUNNING,
    DONE,
    FAILED;

    /** Asking for the current status is {@code Unchanged}; a forbidden step is rejected with {@code INVALID_DIGEST_TRANSITION}. */
    public Transition<DigestStatus> transitionTo(DigestStatus target) {
        throw new UnsupportedOperationException();
    }
}
