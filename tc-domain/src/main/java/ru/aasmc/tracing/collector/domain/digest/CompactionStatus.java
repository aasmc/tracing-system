package ru.aasmc.tracing.collector.domain.digest;

import ru.aasmc.tracing.collector.domain.Transition;

/**
 * Compaction status of one trace day (05 §4.14.3 state diagram).
 *
 * <p>Allowed: PENDING → RUNNING, RUNNING → DONE, RUNNING → FAILED, FAILED → RUNNING. RUNNING → RUNNING is {@link
 * Transition.Unchanged} (a new leader resumes from {@code lastChunkEnd}). Everything else is rejected with {@code
 * INVALID_COMPACTION_TRANSITION}; DONE is terminal.
 */
public enum CompactionStatus {
    PENDING,
    RUNNING,
    DONE,
    FAILED;

    public Transition<CompactionStatus> transitionTo(CompactionStatus target) {
        throw new UnsupportedOperationException();
    }
}
