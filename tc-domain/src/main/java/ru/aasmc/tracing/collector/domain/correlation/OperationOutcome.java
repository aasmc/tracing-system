package ru.aasmc.tracing.collector.domain.correlation;

/** How a business operation ended. */
public enum OperationOutcome {
    /** The traces do not tell. */
    UNKNOWN,
    SUCCESS,
    FAILED,
    /** The final step never happened, e.g. the user abandoned the login. */
    INCOMPLETE
}
