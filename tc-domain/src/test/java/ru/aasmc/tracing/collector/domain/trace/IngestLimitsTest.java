package ru.aasmc.tracing.collector.domain.trace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import ru.aasmc.tracing.collector.domain.attribute.AttributeLimits;
import ru.aasmc.tracing.collector.domain.attribute.TruncatedText;
import ru.aasmc.tracing.collector.testmodel.domain.TimeEdgeCases;
import ru.aasmc.tracing.collector.testmodel.domain.TimeEdgeCases.StartCase;

class IngestLimitsTest {

    private static final IngestLimits LIMITS = IngestLimits.DEFAULT;

    @ParameterizedTest
    @CsvSource({"0, 10, 10", "49990, 10, 10", "49990, 11, 10", "50000, 1, 0", "50001, 5, 0"})
    @DisplayName("stores spans of a trace up to 50 000 and drops the rest")
    void given_storedAndIncoming_when_spansToStore_then_upToLimit(int alreadyStored, int incoming, int expected) {
        assertThat(LIMITS.spansToStore(alreadyStored, incoming)).isEqualTo(expected);
    }

    @Test
    @DisplayName("a message of exactly 8 MiB is accepted, one byte more is too large")
    void given_messageSize_when_checked_then_tooLargeAboveEightMiB() {
        assertThat(LIMITS.isFragmentTooLarge(8 * 1024 * 1024)).isFalse();
        assertThat(LIMITS.isFragmentTooLarge(8 * 1024 * 1024 + 1)).isTrue();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ru.aasmc.tracing.collector.testmodel.domain.TimeEdgeCases#startCases")
    @DisplayName("a span may start at most one day ahead and no earlier than the retention period")
    void given_spanStart_when_judged_then_expectedVerdict(StartCase startCase) {
        assertThat(LIMITS.judgeSpanStart(startCase.startEpochNanos(), TimeEdgeCases.NOW, TimeEdgeCases.RETENTION))
                .isEqualTo(startCase.expected());
    }

    @Test
    @DisplayName("an index value is cut to 512 UTF-8 bytes without splitting a character")
    void given_indexValues_when_truncated_then_atMost512Bytes() {
        String exact = "x".repeat(512);
        String multibyte = "é".repeat(255); // 510 bytes; a following 4-byte emoji does not fit

        assertThat(LIMITS.truncateIndexValue(exact)).isEqualTo(new TruncatedText(exact, false));
        assertThat(LIMITS.truncateIndexValue(exact + "y")).isEqualTo(new TruncatedText(exact, true));
        assertThat(LIMITS.truncateIndexValue(multibyte + "🌍")).isEqualTo(new TruncatedText(multibyte, true));
    }

    @ParameterizedTest
    @CsvSource({"0, 1, 1, 1", "1, 0, 1, 1", "1, 1, 0, 1", "1, 1, 1, 0"})
    @DisplayName("every limit is positive")
    void given_nonPositiveLimit_when_constructed_then_rejected(
            int spans, int fragmentBytes, int toleranceSeconds, int indexBytes) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new IngestLimits(
                        spans,
                        fragmentBytes,
                        Duration.ofSeconds(toleranceSeconds),
                        indexBytes,
                        AttributeLimits.DEFAULT));
    }
}
