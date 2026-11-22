package ru.aasmc.tracing.collector.domain.trace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.testmodel.domain.TimeEdgeCases;
import ru.aasmc.tracing.collector.testmodel.domain.TimeEdgeCases.NanosCase;

class TraceTimeTest {

    private static final LocalDate NOV_21 = LocalDate.of(2026, 11, 21);

    @ParameterizedTest(name = "{0}")
    @MethodSource("ru.aasmc.tracing.collector.testmodel.domain.TimeEdgeCases#nanosCases")
    @DisplayName("parses epoch nanos exactly and cuts them to the microseconds PostgreSQL keeps")
    void given_wireText_when_converted_then_exactAndTruncatedInstants(NanosCase nanosCase) {
        long epochNanos = EpochNanos.parse(nanosCase.text());

        assertThat(epochNanos).isEqualTo(nanosCase.epochNanos());
        assertThat(EpochNanos.toInstant(epochNanos)).isEqualTo(nanosCase.exact());
        assertThat(EpochNanos.toInstantTruncatedToMicros(epochNanos)).isEqualTo(nanosCase.truncatedToMicros());
    }

    @ParameterizedTest
    @MethodSource("ru.aasmc.tracing.collector.testmodel.domain.TimeEdgeCases#invalidNanosTexts")
    @DisplayName("rejects text that is not a decimal long")
    void given_invalidText_when_parse_then_numberFormatException(String text) {
        assertThatThrownBy(() -> EpochNanos.parse(text)).isInstanceOf(NumberFormatException.class);
    }

    @Test
    @DisplayName("the trace day is the UTC date of a time, also across midnight and before 1970")
    void given_epochNanos_when_traceDay_then_utcDate() {
        List<Long> starts = TimeEdgeCases.midnightTrace().stream()
                .map(Span::startEpochNanos)
                .toList();

        assertThat(TraceDay.ofEpochNanos(starts.get(0))).isEqualTo(new TraceDay(NOV_21));
        assertThat(TraceDay.ofEpochNanos(starts.get(1))).isEqualTo(new TraceDay(NOV_21.plusDays(1)));
        assertThat(TraceDay.ofEpochNanos(-1)).isEqualTo(new TraceDay(LocalDate.of(1969, 12, 31)));
    }

    @Test
    @DisplayName("trace days are ordered by date")
    void given_twoDays_when_compared_then_earlierFirst() {
        assertThat(new TraceDay(NOV_21)).isLessThan(new TraceDay(NOV_21.plusDays(1)));
    }

    @Test
    @DisplayName("a time range needs both ends and a start before its end")
    void given_emptyReversedOrOpenRange_when_constructed_then_invalidTimeRange() {
        Instant from = Instant.parse("2026-11-21T09:00:00Z");
        Instant to = from.plusSeconds(1);

        assertViolation(ViolationCode.INVALID_TIME_RANGE, () -> new TimeRange(from, from));
        assertViolation(ViolationCode.INVALID_TIME_RANGE, () -> new TimeRange(to, from));
        assertViolation(ViolationCode.INVALID_TIME_RANGE, () -> new TimeRange(null, to));
        assertViolation(ViolationCode.INVALID_TIME_RANGE, () -> new TimeRange(from, null));
    }
}
