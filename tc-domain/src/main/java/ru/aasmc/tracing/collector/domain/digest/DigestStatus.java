package ru.aasmc.tracing.collector.domain.digest;

import ru.aasmc.tracing.collector.domain.Transition;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/** Life cycle of digesting one day: PENDING → RUNNING → DONE, or RUNNING → FAILED → RUNNING to retry. */
public enum DigestStatus {
    PENDING,
    RUNNING,
    DONE,
    FAILED;

    /**
     * Whether this status may change to {@code target}: {@code Unchanged} if {@code target} is the current status,
     * {@code Changed} if the step is allowed, otherwise {@code Rejected} with {@code INVALID_DIGEST_TRANSITION}.
     */
    public Transition<DigestStatus> transitionTo(DigestStatus target) {
        boolean allowed = switch (this) {
            case PENDING, FAILED -> target == RUNNING;
            case RUNNING -> target == DONE || target == FAILED;
            case DONE -> false;
        };
        return Transition.of(this, target, allowed, ViolationCode.INVALID_DIGEST_TRANSITION);
    }
}
