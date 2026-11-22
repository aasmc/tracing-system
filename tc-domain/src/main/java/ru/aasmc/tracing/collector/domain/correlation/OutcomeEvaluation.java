package ru.aasmc.tracing.collector.domain.correlation;

/**
 * The outcome of a business operation and what decided it.
 *
 * @param outcome the outcome
 * @param decidingValue the outcome attribute's value that decided it (at most 256 characters), or {@code null}
 */
public record OutcomeEvaluation(OperationOutcome outcome, String decidingValue) {}
