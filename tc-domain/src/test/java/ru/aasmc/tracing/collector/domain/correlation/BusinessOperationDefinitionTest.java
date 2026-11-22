package ru.aasmc.tracing.collector.domain.correlation;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.testmodel.domain.IdpScenario;

class BusinessOperationDefinitionTest {

    private static final CorrelationKey SESSION_KEY =
            IdpScenario.operationDefinition().keys().get(0);
    private static final CorrelationKey CODE_KEY =
            IdpScenario.operationDefinition().keys().get(1);
    private static final Duration WINDOW = Duration.ofDays(1);
    private static final List<CorrelationKey> KEYS = List.of(SESSION_KEY, CODE_KEY);

    @ParameterizedTest
    @ValueSource(strings = {"ab", "IDP.login", "-idp", "idp login"})
    @DisplayName("a name is at least 3 lower-case letters, digits, dots, underscores or dashes")
    void given_invalidName_when_constructed_then_invalidName(String name) {
        assertViolation(ViolationCode.INVALID_NAME, () -> definition(name, WINDOW, KEYS, null));
    }

    @Test
    @DisplayName("a merge window is set and positive")
    void given_missingOrEmptyMergeWindow_when_constructed_then_invalidMergeWindow() {
        assertViolation(ViolationCode.INVALID_MERGE_WINDOW, () -> definition("idp.login", null, KEYS, null));
        assertViolation(ViolationCode.INVALID_MERGE_WINDOW, () -> definition("idp.login", Duration.ZERO, KEYS, null));
    }

    @Test
    @DisplayName("a definition has a primary key, and no key id or attribute twice")
    void given_invalidKeys_when_constructed_then_invalidOperationDefinition() {
        CorrelationKey sessionAgain =
                new CorrelationKey(3, SESSION_KEY.key(), KeyRole.ALIAS, IdpScenario.STEP_TOKEN_EXCHANGE);

        assertViolation(
                ViolationCode.INVALID_OPERATION_DEFINITION, () -> definition("idp.login", WINDOW, List.of(), null));
        assertViolation(
                ViolationCode.INVALID_OPERATION_DEFINITION,
                () -> definition("idp.login", WINDOW, List.of(CODE_KEY), null));
        assertViolation(
                ViolationCode.INVALID_OPERATION_DEFINITION,
                () -> definition("idp.login", WINDOW, List.of(SESSION_KEY, sessionAgain), null));
        CorrelationKey sameIdAsSession =
                new CorrelationKey(SESSION_KEY.id(), new AttributeKey("oauth.state"), KeyRole.ALIAS, null);
        assertViolation(
                ViolationCode.INVALID_OPERATION_DEFINITION,
                () -> definition("idp.login", WINDOW, List.of(SESSION_KEY, sameIdAsSession), null));
    }

    @Test
    @DisplayName("keys are kept in business order as an immutable copy")
    void given_mutableKeyList_when_constructed_then_immutableCopy() {
        List<CorrelationKey> source = new ArrayList<>(KEYS);

        BusinessOperationDefinition definition = definition("idp.login", WINDOW, source, null);
        source.removeLast();

        assertThat(definition.keys()).containsExactly(SESSION_KEY, CODE_KEY);
    }

    @Test
    @DisplayName("the outcome rule's final step is the step label of one of the keys")
    void given_unknownFinalStep_when_constructed_then_unknownStepLabel() {
        OutcomeRule unknownStep = new OutcomeRule(null, Set.of(), Set.of(), "sms-sent");

        assertViolation(ViolationCode.UNKNOWN_STEP_LABEL, () -> definition("idp.login", WINDOW, KEYS, unknownStep));
    }

    @Test
    @DisplayName("a key has an attribute, a role and an optional non-blank step label")
    void given_invalidKey_when_constructed_then_invalidOperationDefinition() {
        AttributeKey attribute = new AttributeKey("idp.auth_session_id");

        assertViolation(
                ViolationCode.INVALID_OPERATION_DEFINITION,
                () -> new CorrelationKey(1, attribute, KeyRole.PRIMARY, ""));
        assertViolation(
                ViolationCode.INVALID_OPERATION_DEFINITION,
                () -> new CorrelationKey(1, attribute, KeyRole.PRIMARY, "  "));
        assertViolation(
                ViolationCode.INVALID_OPERATION_DEFINITION, () -> new CorrelationKey(1, null, KeyRole.PRIMARY, null));
        assertViolation(ViolationCode.INVALID_OPERATION_DEFINITION, () -> new CorrelationKey(1, attribute, null, null));
    }

    @Test
    @DisplayName("an observation has a time, a trace and a non-blank value")
    void given_invalidObservation_when_constructed_then_invalidCorrelationValue() {
        Instant seen = IdpScenario.T1_OBSERVED;

        assertViolation(
                ViolationCode.INVALID_CORRELATION_VALUE, () -> IdpScenario.observation(IdpScenario.T1, seen, 1, " "));
        assertViolation(
                ViolationCode.INVALID_CORRELATION_VALUE, () -> IdpScenario.observation(null, seen, 1, "as-777"));
        assertViolation(
                ViolationCode.INVALID_CORRELATION_VALUE,
                () -> IdpScenario.observation(IdpScenario.T1, null, 1, "as-777"));
    }

    private static BusinessOperationDefinition definition(
            String name, Duration mergeWindow, List<CorrelationKey> keys, OutcomeRule outcomeRule) {
        return new BusinessOperationDefinition(1, name, mergeWindow, keys, outcomeRule);
    }
}
