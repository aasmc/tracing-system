package ru.aasmc.tracing.collector.domain.correlation;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.STEP_AUTHENTICATION;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.STEP_TOKEN_EXCHANGE;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.T1;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.T1_OBSERVED;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.T2;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.T2_OBSERVED;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.T3;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.T3_OBSERVED;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.trace.TraceId;
import ru.aasmc.tracing.collector.testmodel.domain.IdpScenario;

class OutcomeRuleTest {

    private static final int MAX_DECIDING_VALUE_LENGTH = CorrelationLimits.DEFAULT.maxDecidingValueLength();
    private static final AttributeKey LOGIN_RESULT = new AttributeKey("login.result");
    private static final OutcomeRule BY_RESULT =
            new OutcomeRule(LOGIN_RESULT, Set.of("OK"), Set.of("DENIED", "BLOCKED"), null);
    private static final OutcomeRule BY_FINAL_STEP =
            IdpScenario.operationDefinition().outcomeRule();

    @Test
    @DisplayName("a success value of the outcome attribute means SUCCESS")
    void given_successValue_when_evaluated_then_success() {
        List<MemberTrace> members = List.of(member(T1, T1_OBSERVED, 0, null), member(T2, T2_OBSERVED, 0, "OK"));

        assertThat(BY_RESULT.evaluate(members, MAX_DECIDING_VALUE_LENGTH))
                .isEqualTo(new OutcomeEvaluation(OperationOutcome.SUCCESS, "OK"));
    }

    @Test
    @DisplayName("the latest trace with an outcome value decides: a failure value means FAILED")
    void given_failureValueOnLatestTrace_when_evaluated_then_failed() {
        List<MemberTrace> members = List.of(member(T2, T2_OBSERVED, 0, "DENIED"), member(T1, T1_OBSERVED, 0, "OK"));

        assertThat(BY_RESULT.evaluate(members, MAX_DECIDING_VALUE_LENGTH))
                .isEqualTo(new OutcomeEvaluation(OperationOutcome.FAILED, "DENIED"));
    }

    @Test
    @DisplayName("an outcome value in neither set means UNKNOWN")
    void given_otherValue_when_evaluated_then_unknown() {
        List<MemberTrace> members = List.of(member(T1, T1_OBSERVED, 0, "MAYBE"));

        assertThat(BY_RESULT.evaluate(members, MAX_DECIDING_VALUE_LENGTH))
                .isEqualTo(new OutcomeEvaluation(OperationOutcome.UNKNOWN, "MAYBE"));
    }

    @Test
    @DisplayName("of traces that started together, the one with the larger trace id decides")
    void given_sameStart_when_evaluated_then_largerTraceIdDecides() {
        List<MemberTrace> members = List.of(member(T3, T1_OBSERVED, 0, "DENIED"), member(T1, T1_OBSERVED, 0, "OK"));

        assertThat(BY_RESULT.evaluate(members, MAX_DECIDING_VALUE_LENGTH))
                .isEqualTo(new OutcomeEvaluation(OperationOutcome.FAILED, "DENIED"));
    }

    @Test
    @DisplayName("an outcome value decides even when a trace has an error")
    void given_outcomeValueAndError_when_evaluated_then_outcomeValueWins() {
        List<MemberTrace> members = List.of(member(T1, T1_OBSERVED, 1, null), member(T2, T2_OBSERVED, 0, "OK"));

        assertThat(BY_RESULT.evaluate(members, MAX_DECIDING_VALUE_LENGTH))
                .isEqualTo(new OutcomeEvaluation(OperationOutcome.SUCCESS, "OK"));
    }

    @Test
    @DisplayName("without an outcome value, an error means FAILED")
    void given_errorWithoutOutcomeValue_when_evaluated_then_failed() {
        List<MemberTrace> members = List.of(member(T1, T1_OBSERVED, 1, null), member(T2, T2_OBSERVED, 0, null));

        assertThat(BY_RESULT.evaluate(members, MAX_DECIDING_VALUE_LENGTH))
                .isEqualTo(new OutcomeEvaluation(OperationOutcome.FAILED, null));
    }

    @Test
    @DisplayName("an error means FAILED even when the final step was reached")
    void given_errorAndFinalStep_when_evaluated_then_failed() {
        List<MemberTrace> members = List.of(
                member(T1, T1_OBSERVED, 1, null, STEP_AUTHENTICATION),
                member(T3, T3_OBSERVED, 0, null, STEP_TOKEN_EXCHANGE));

        assertThat(BY_FINAL_STEP.evaluate(members, MAX_DECIDING_VALUE_LENGTH))
                .isEqualTo(new OutcomeEvaluation(OperationOutcome.FAILED, null));
    }

    @Test
    @DisplayName("reaching the final step means SUCCESS")
    void given_finalStepReached_when_evaluated_then_success() {
        assertThat(BY_FINAL_STEP.evaluate(IdpScenario.memberTraces(), MAX_DECIDING_VALUE_LENGTH))
                .isEqualTo(new OutcomeEvaluation(OperationOutcome.SUCCESS, null));
    }

    @Test
    @DisplayName("an operation that never reached the final step is INCOMPLETE")
    void given_finalStepMissing_when_evaluated_then_incomplete() {
        List<MemberTrace> members = List.of(member(T1, T1_OBSERVED, 0, null, STEP_AUTHENTICATION));

        assertThat(BY_FINAL_STEP.evaluate(members, MAX_DECIDING_VALUE_LENGTH))
                .isEqualTo(new OutcomeEvaluation(OperationOutcome.INCOMPLETE, null));
    }

    @Test
    @DisplayName("the default rule gives SUCCESS without errors and FAILED with one")
    void given_defaultRule_when_evaluated_then_successUnlessError() {
        List<MemberTrace> withError = List.of(member(T1, T1_OBSERVED, 1, null), member(T2, T2_OBSERVED, 0, null));

        assertThat(OutcomeRule.defaultRule().evaluate(IdpScenario.memberTraces(), MAX_DECIDING_VALUE_LENGTH))
                .isEqualTo(new OutcomeEvaluation(OperationOutcome.SUCCESS, null));
        assertThat(OutcomeRule.defaultRule().evaluate(withError, MAX_DECIDING_VALUE_LENGTH))
                .isEqualTo(new OutcomeEvaluation(OperationOutcome.FAILED, null));
    }

    @Test
    @DisplayName("an operation without traces has an UNKNOWN outcome")
    void given_noTraces_when_evaluated_then_unknown() {
        assertThat(BY_RESULT.evaluate(List.of(), MAX_DECIDING_VALUE_LENGTH))
                .isEqualTo(new OutcomeEvaluation(OperationOutcome.UNKNOWN, null));
    }

    @Test
    @DisplayName("the deciding value is cut to the configured length")
    void given_longOutcomeValue_when_evaluated_then_decidingValueCut() {
        List<MemberTrace> members = List.of(member(T1, T1_OBSERVED, 0, "x".repeat(300)));

        assertThat(BY_RESULT.evaluate(members, MAX_DECIDING_VALUE_LENGTH))
                .isEqualTo(new OutcomeEvaluation(OperationOutcome.UNKNOWN, "x".repeat(256)));
        assertThat(BY_RESULT.evaluate(members, 300))
                .isEqualTo(new OutcomeEvaluation(OperationOutcome.UNKNOWN, "x".repeat(300)));
    }

    @Test
    @DisplayName("""
            Given an outcome value of emoji, each of which takes two Java chars,
            when the deciding value is cut to 3 characters,
            then 3 whole emoji are kept: an emoji is never split in half""")
    void given_emojiOutcomeValue_when_cut_then_wholeCharactersKept() {
        String emoji = "\uD83D\uDE00";
        List<MemberTrace> members = List.of(member(T1, T1_OBSERVED, 0, emoji.repeat(5)));

        assertThat(BY_RESULT.evaluate(members, 3))
                .isEqualTo(new OutcomeEvaluation(OperationOutcome.UNKNOWN, emoji.repeat(3)));
    }

    @Test
    @DisplayName("missing value sets become empty")
    void given_nullValueSets_when_constructed_then_empty() {
        OutcomeRule rule = new OutcomeRule(null, null, null, null);

        assertThat(rule.successValues()).isEmpty();
        assertThat(rule.failureValues()).isEmpty();
    }

    @Test
    @DisplayName("rejects overlapping sets, values without an outcome key, blank values and blank labels")
    void given_invalidRule_when_constructed_then_invalidOutcomeRule() {
        assertViolation(
                ViolationCode.INVALID_OUTCOME_RULE,
                () -> new OutcomeRule(LOGIN_RESULT, Set.of("OK"), Set.of("OK"), null));
        assertViolation(ViolationCode.INVALID_OUTCOME_RULE, () -> new OutcomeRule(null, Set.of("OK"), Set.of(), null));
        assertViolation(
                ViolationCode.INVALID_OUTCOME_RULE, () -> new OutcomeRule(LOGIN_RESULT, Set.of(""), Set.of(), null));
        assertViolation(ViolationCode.INVALID_OUTCOME_RULE, () -> new OutcomeRule(null, Set.of(), Set.of(), ""));
        assertViolation(
                ViolationCode.INVALID_OUTCOME_RULE, () -> new OutcomeRule(LOGIN_RESULT, Set.of(" "), Set.of(), null));
        assertViolation(ViolationCode.INVALID_OUTCOME_RULE, () -> new OutcomeRule(null, Set.of(), Set.of(), "\t"));
    }

    @Test
    @DisplayName("a member trace keeps its own copy of the step labels")
    void given_mutableStepLabels_when_memberBuilt_then_copyKept() {
        Set<String> labels = new HashSet<>(Set.of(STEP_AUTHENTICATION));
        MemberTrace member = new MemberTrace(T1, T1_OBSERVED, 0, null, labels);
        labels.add(STEP_TOKEN_EXCHANGE);

        assertThat(member.stepLabels()).containsExactly(STEP_AUTHENTICATION);
    }

    private static MemberTrace member(
            TraceId trace, Instant start, int errors, String outcomeValue, String... stepLabels) {
        return new MemberTrace(trace, start, errors, outcomeValue, Set.of(stepLabels));
    }

    @Test
    @DisplayName("the failure set follows the same rules: no blank value, and values only with an outcome key")
    void given_invalidFailureValues_when_constructed_then_invalidOutcomeRule() {
        assertViolation(
                ViolationCode.INVALID_OUTCOME_RULE, () -> new OutcomeRule(LOGIN_RESULT, Set.of(), Set.of(""), null));
        assertViolation(
                ViolationCode.INVALID_OUTCOME_RULE, () -> new OutcomeRule(null, Set.of(), Set.of("DENIED"), null));
    }
}
