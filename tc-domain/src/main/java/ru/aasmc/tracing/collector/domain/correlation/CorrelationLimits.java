package ru.aasmc.tracing.collector.domain.correlation;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.time.Duration;
import java.util.Set;
import ru.aasmc.tracing.collector.domain.Limits;
import ru.aasmc.tracing.collector.domain.Names;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * Configurable limits of business operation definitions and their values. All limits are positive and
 * {@code minMergeWindow <= defaultMergeWindow <= maxMergeWindow}; {@link #DEFAULT} holds the values used when none are
 * configured.
 *
 * @param maxDefinitionNameLength characters of a definition name
 * @param minMergeWindow shortest merge window a definition may have
 * @param maxMergeWindow longest merge window a definition may have
 * @param defaultMergeWindow merge window of a definition created without one
 * @param maxStepLabelLength characters of a step label
 * @param maxCorrelationValueLength characters of an observed correlation value
 * @param maxOutcomeValues values in each of an outcome rule's success and failure sets
 * @param maxOutcomeValueLength characters of one of those values
 * @param maxDecidingValueLength characters kept of the value that decided an outcome
 */
public record CorrelationLimits(
        int maxDefinitionNameLength,
        Duration minMergeWindow,
        Duration maxMergeWindow,
        Duration defaultMergeWindow,
        int maxStepLabelLength,
        int maxCorrelationValueLength,
        int maxOutcomeValues,
        int maxOutcomeValueLength,
        int maxDecidingValueLength) {

    public static final CorrelationLimits DEFAULT = new CorrelationLimits(
            63, Duration.ofMinutes(1), Duration.ofDays(30), Duration.ofDays(1), 64, 512, 50, 512, 256);

    public CorrelationLimits {
        Limits.requirePositive("maxDefinitionNameLength", maxDefinitionNameLength);
        Limits.requirePositive("minMergeWindow", minMergeWindow);
        Limits.requirePositive("maxMergeWindow", maxMergeWindow);
        Limits.requirePositive("defaultMergeWindow", defaultMergeWindow);
        if (defaultMergeWindow.compareTo(minMergeWindow) < 0 || defaultMergeWindow.compareTo(maxMergeWindow) > 0) {
            throw new IllegalArgumentException("defaultMergeWindow " + defaultMergeWindow + " is outside ["
                    + minMergeWindow + ", " + maxMergeWindow + "]");
        }
        Limits.requirePositive("maxStepLabelLength", maxStepLabelLength);
        Limits.requirePositive("maxCorrelationValueLength", maxCorrelationValueLength);
        Limits.requirePositive("maxOutcomeValues", maxOutcomeValues);
        Limits.requirePositive("maxOutcomeValueLength", maxOutcomeValueLength);
        Limits.requirePositive("maxDecidingValueLength", maxDecidingValueLength);
    }

    /**
     * Checks that the definition stays within these limits: name length, merge window, step labels and outcome values.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_NAME} for a too long name,
     *     {@code INVALID_MERGE_WINDOW} for a window outside the bounds, {@code INVALID_OPERATION_DEFINITION} for a too long
     *     step label (the outcome rule's final step is one of them), {@code INVALID_OUTCOME_RULE} for too many or too long
     *     outcome values
     */
    public void requireWithin(BusinessOperationDefinition definition) {
        Names.requireAtMost(definition.name(), maxDefinitionNameLength);
        require(
                definition.mergeWindow().compareTo(minMergeWindow) >= 0
                        && definition.mergeWindow().compareTo(maxMergeWindow) <= 0,
                ViolationCode.INVALID_MERGE_WINDOW,
                "merge window " + definition.mergeWindow() + " is outside [" + minMergeWindow + ", " + maxMergeWindow
                        + "]");
        for (CorrelationKey key : definition.keys()) {
            require(
                    key.stepLabel() == null || key.stepLabel().length() <= maxStepLabelLength,
                    ViolationCode.INVALID_OPERATION_DEFINITION,
                    "step label longer than " + maxStepLabelLength + " characters: " + key.stepLabel());
        }
        requireOutcomeValuesWithin(definition.outcomeRule().successValues());
        requireOutcomeValuesWithin(definition.outcomeRule().failureValues());
    }

    /**
     * Checks that the observed value is not longer than allowed.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_CORRELATION_VALUE} for a too
     *     long value
     */
    public void requireWithin(CorrelationObservation observation) {
        require(
                observation.value().length() <= maxCorrelationValueLength,
                ViolationCode.INVALID_CORRELATION_VALUE,
                "correlation value longer than " + maxCorrelationValueLength + " characters");
    }

    private void requireOutcomeValuesWithin(Set<String> values) {
        require(
                values.size() <= maxOutcomeValues,
                ViolationCode.INVALID_OUTCOME_RULE,
                values.size() + " outcome values, at most " + maxOutcomeValues + " allowed");
        require(
                values.stream().allMatch(value -> value.length() <= maxOutcomeValueLength),
                ViolationCode.INVALID_OUTCOME_RULE,
                "an outcome value is longer than " + maxOutcomeValueLength + " characters");
    }
}
