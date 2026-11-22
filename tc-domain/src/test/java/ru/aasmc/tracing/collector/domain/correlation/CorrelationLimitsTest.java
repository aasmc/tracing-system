package ru.aasmc.tracing.collector.domain.correlation;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.testmodel.domain.IdpScenario;

class CorrelationLimitsTest {

    private static final CorrelationLimits DEFAULTS = CorrelationLimits.DEFAULT;
    private static final AttributeKey LOGIN_RESULT = new AttributeKey("login.result");

    @Test
    @DisplayName("by default a type name has at most 63 characters; a configured limit allows more")
    void given_nameLengths_when_checked_then_withinConfiguredLimit() {
        BusinessOperationDefinition longName = typeNamed("t".repeat(64));

        assertThatNoException().isThrownBy(() -> DEFAULTS.requireWithin(typeNamed("t".repeat(63))));
        assertViolation(ViolationCode.INVALID_NAME, () -> DEFAULTS.requireWithin(longName));
        assertThatNoException().isThrownBy(() -> withTypeNameLength(64).requireWithin(longName));
    }

    @Test
    @DisplayName("by default a merge window lies between one minute and 30 days; configured bounds replace them")
    void given_mergeWindows_when_checked_then_withinConfiguredBounds() {
        BusinessOperationDefinition longWindow =
                typeWithWindow(Duration.ofDays(30).plusSeconds(1));

        assertThatNoException().isThrownBy(() -> DEFAULTS.requireWithin(typeWithWindow(Duration.ofMinutes(1))));
        assertThatNoException().isThrownBy(() -> DEFAULTS.requireWithin(typeWithWindow(Duration.ofDays(30))));
        assertViolation(
                ViolationCode.INVALID_MERGE_WINDOW,
                () -> DEFAULTS.requireWithin(typeWithWindow(Duration.ofSeconds(59))));
        assertViolation(ViolationCode.INVALID_MERGE_WINDOW, () -> DEFAULTS.requireWithin(longWindow));
        assertThatNoException()
                .isThrownBy(() -> withMaxMergeWindow(Duration.ofDays(60)).requireWithin(longWindow));
    }

    @Test
    @DisplayName("by default a step label has at most 64 characters")
    void given_stepLabelLengths_when_checked_then_atMost64() {
        assertThatNoException().isThrownBy(() -> DEFAULTS.requireWithin(typeWithFinalStep("s".repeat(64))));
        assertViolation(
                ViolationCode.INVALID_OPERATION_DEFINITION,
                () -> DEFAULTS.requireWithin(typeWithFinalStep("s".repeat(65))));
    }

    @Test
    @DisplayName("by default an outcome rule has at most 50 values per set, each of at most 512 characters")
    void given_outcomeValues_when_checked_then_atMost50Of512Characters() {
        assertThatNoException().isThrownBy(() -> DEFAULTS.requireWithin(typeWithSuccessValues(values(50, 512))));
        assertViolation(
                ViolationCode.INVALID_OUTCOME_RULE, () -> DEFAULTS.requireWithin(typeWithSuccessValues(values(51, 1))));
        assertViolation(
                ViolationCode.INVALID_OUTCOME_RULE,
                () -> DEFAULTS.requireWithin(typeWithSuccessValues(values(1, 513))));
    }

    @Test
    @DisplayName("by default a correlation value has at most 512 characters; a configured limit allows more")
    void given_valueLengths_when_checked_then_withinConfiguredLimit() {
        CorrelationObservation longValue = observationOf("v".repeat(513));

        assertThatNoException().isThrownBy(() -> DEFAULTS.requireWithin(observationOf("v".repeat(512))));
        assertViolation(ViolationCode.INVALID_CORRELATION_VALUE, () -> DEFAULTS.requireWithin(longValue));
        assertThatNoException()
                .isThrownBy(() -> withCorrelationValueLength(1024).requireWithin(longValue));
    }

    @Test
    @DisplayName("every limit is positive and the default merge window lies within the bounds")
    void given_brokenLimits_when_constructed_then_rejected() {
        assertThatIllegalArgumentException().isThrownBy(() -> withTypeNameLength(0));
        assertThatIllegalArgumentException().isThrownBy(() -> withCorrelationValueLength(0));
        assertThatIllegalArgumentException().isThrownBy(() -> withMaxMergeWindow(Duration.ofHours(1)));
    }

    private static CorrelationLimits withTypeNameLength(int maxDefinitionNameLength) {
        return new CorrelationLimits(
                maxDefinitionNameLength,
                DEFAULTS.minMergeWindow(),
                DEFAULTS.maxMergeWindow(),
                DEFAULTS.defaultMergeWindow(),
                DEFAULTS.maxStepLabelLength(),
                DEFAULTS.maxCorrelationValueLength(),
                DEFAULTS.maxOutcomeValues(),
                DEFAULTS.maxOutcomeValueLength(),
                DEFAULTS.maxDecidingValueLength());
    }

    private static CorrelationLimits withMaxMergeWindow(Duration maxMergeWindow) {
        return new CorrelationLimits(
                DEFAULTS.maxDefinitionNameLength(),
                DEFAULTS.minMergeWindow(),
                maxMergeWindow,
                DEFAULTS.defaultMergeWindow(),
                DEFAULTS.maxStepLabelLength(),
                DEFAULTS.maxCorrelationValueLength(),
                DEFAULTS.maxOutcomeValues(),
                DEFAULTS.maxOutcomeValueLength(),
                DEFAULTS.maxDecidingValueLength());
    }

    private static CorrelationLimits withCorrelationValueLength(int maxCorrelationValueLength) {
        return new CorrelationLimits(
                DEFAULTS.maxDefinitionNameLength(),
                DEFAULTS.minMergeWindow(),
                DEFAULTS.maxMergeWindow(),
                DEFAULTS.defaultMergeWindow(),
                DEFAULTS.maxStepLabelLength(),
                maxCorrelationValueLength,
                DEFAULTS.maxOutcomeValues(),
                DEFAULTS.maxOutcomeValueLength(),
                DEFAULTS.maxDecidingValueLength());
    }

    private static BusinessOperationDefinition typeNamed(String name) {
        return type(name, Duration.ofDays(1), IdpScenario.operationDefinition().keys(), null);
    }

    private static BusinessOperationDefinition typeWithWindow(Duration mergeWindow) {
        return type("idp.login", mergeWindow, IdpScenario.operationDefinition().keys(), null);
    }

    private static BusinessOperationDefinition typeWithFinalStep(String stepLabel) {
        return type(
                "idp.login",
                Duration.ofDays(1),
                List.of(primaryKey(stepLabel)),
                new OutcomeRule(null, Set.of(), Set.of(), stepLabel));
    }

    private static BusinessOperationDefinition typeWithSuccessValues(Set<String> successValues) {
        return type(
                "idp.login",
                Duration.ofDays(1),
                IdpScenario.operationDefinition().keys(),
                new OutcomeRule(LOGIN_RESULT, successValues, Set.of(), null));
    }

    private static BusinessOperationDefinition type(
            String name, Duration mergeWindow, List<CorrelationKey> keys, OutcomeRule outcomeRule) {
        return new BusinessOperationDefinition(UUID.randomUUID(), name, mergeWindow, keys, outcomeRule);
    }

    private static CorrelationKey primaryKey(String stepLabel) {
        return new CorrelationKey(
                UUID.randomUUID(), new AttributeKey("idp.auth_session_id"), KeyRole.PRIMARY, stepLabel);
    }

    /** {@code count} distinct values of {@code length} characters each. */
    private static Set<String> values(int count, int length) {
        return IntStream.range(0, count)
                .mapToObj(i -> String.format("%0" + length + "d", i))
                .collect(Collectors.toSet());
    }

    private static CorrelationObservation observationOf(String value) {
        return IdpScenario.observation(IdpScenario.T1, IdpScenario.T1_OBSERVED, IdpScenario.SESSION_KEY_ID, value);
    }

    @Test
    @DisplayName("the default merge window must lie within the configured bounds, also not below the minimum")
    void given_defaultWindowBelowMinimum_when_constructed_then_rejected() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new CorrelationLimits(
                        63, Duration.ofDays(2), Duration.ofDays(30), Duration.ofDays(1), 64, 512, 50, 512, 256));
    }
}
