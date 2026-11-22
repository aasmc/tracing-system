package ru.aasmc.tracing.collector.domain.enrichment;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FieldReadingTest {

    @Test
    @DisplayName("""
            Given a found value,
            when the next step is chained,
            then the step runs on the value and its outcome (found, missing or failed) is the result""")
    void given_found_when_stepChained_then_stepRuns() {
        FieldReading<String> found = FieldReading.found("42");

        assertThat(found.then(text -> FieldReading.found(Integer.parseInt(text))))
                .isEqualTo(FieldReading.found(42));
        assertThat(found.then(_ -> FieldReading.failed(ExtractionFailure.TYPE_MISMATCH)))
                .isEqualTo(FieldReading.failed(ExtractionFailure.TYPE_MISMATCH));
    }

    @Test
    @DisplayName("""
            Given a missing or failed reading,
            when the next step is chained,
            then the step does not run and the reading passes through unchanged, keeping its failure reason""")
    void given_missingOrFailed_when_stepChained_then_passesThrough() {
        FieldReading<String> missing = FieldReading.missing();
        FieldReading<String> failed = FieldReading.failed(ExtractionFailure.TRANSFORM_FAILED);

        assertThat(missing.then(_ -> FieldReading.found("never"))).isEqualTo(FieldReading.missing());
        assertThat(failed.then(_ -> FieldReading.found("never")))
                .isEqualTo(FieldReading.failed(ExtractionFailure.TRANSFORM_FAILED));
    }

    @Test
    @DisplayName("""
            Given a required mapping,
            then a missing value becomes MISSING_REQUIRED_FIELD, while found and failed readings stay as they are;
            for an optional mapping a missing value stays missing""")
    void given_required_when_checked_then_onlyMissingBecomesFailure() {
        assertThat(FieldReading.<String>missing().requiredIf(true))
                .isEqualTo(FieldReading.failed(ExtractionFailure.MISSING_REQUIRED_FIELD));
        assertThat(FieldReading.<String>missing().requiredIf(false)).isEqualTo(FieldReading.missing());
        assertThat(FieldReading.found("x").requiredIf(true)).isEqualTo(FieldReading.found("x"));
        assertThat(FieldReading.<String>failed(ExtractionFailure.TYPE_MISMATCH).requiredIf(true))
                .isEqualTo(FieldReading.failed(ExtractionFailure.TYPE_MISMATCH));
    }
}
