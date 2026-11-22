package ru.aasmc.tracing.collector.domain.correlation;

import java.util.List;

/**
 * Derives the outcome of a business operation at compaction (05 §4.10.1, ADR-13 addendum §3). In the 05 sketch this
 * was {@code OutcomeRule.evaluate}; it is a separate stateless policy so that tests and {@code DigestCompactor}
 * instantiate it directly.
 *
 * <p>Fixed order:
 *
 * <ol>
 *   <li>the rule has an outcome key and some member carries a value: the value of the member with the latest start
 *       (ties: larger trace id) decides — SUCCESS if in {@code successValues}, FAILED if in {@code failureValues},
 *       else UNKNOWN; the raw value (cut to 256 characters) is the detail;
 *   <li>else any member has {@code errorCount > 0}: FAILED;
 *   <li>else the rule has a final step label: SUCCESS if some member carries it, else INCOMPLETE;
 *   <li>else SUCCESS.
 * </ol>
 *
 * <p>No members: UNKNOWN. A {@code null} rule behaves like {@link OutcomeRule#none()}. Thread-safe.
 */
public final class OperationOutcomeEvaluator {

    public OutcomeEvaluation evaluate(OutcomeRule ruleOrNull, List<MemberTrace> members) {
        throw new UnsupportedOperationException();
    }
}
