package ru.aasmc.tracing.collector.domain.correlation;

import ru.aasmc.tracing.collector.domain.digest.OperationOutcome;

/**
 * Result of {@link OperationOutcomeEvaluator} (05 §4.10.1).
 *
 * @param outcome the outcome
 * @param outcomeDetail the raw outcome-key value that decided it (rule step 1), else {@code null}; at most 256
 *     characters ({@code business_operation_digest.outcome_detail})
 */
public record OutcomeEvaluation(OperationOutcome outcome, String outcomeDetail) {}
