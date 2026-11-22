package ru.aasmc.tracing.collector.testmodel.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import ru.aasmc.tracing.collector.domain.trace.Span;
import ru.aasmc.tracing.collector.domain.trace.SpanTimeVerdict;

/** Time edge cases: start-time limits, epoch-nanosecond conversions and a trace that crosses midnight UTC. */
public final class TimeEdgeCases {

    public static final Instant NOW = Instant.parse("2026-11-21T10:00:00Z");
    public static final Duration RETENTION = Duration.ofDays(7);

    private TimeEdgeCases() {}

    /** A span start and its verdict for {@code NOW}, {@code RETENTION} and the default one-day future tolerance. */
    public record StartCase(String name, long startEpochNanos, SpanTimeVerdict expected) {

        @Override
        public String toString() {
            return name;
        }
    }

    /** A wire text, its value, its exact instant and the instant cut to microseconds. */
    public record NanosCase(String name, String text, long epochNanos, Instant exact, Instant truncatedToMicros) {

        @Override
        public String toString() {
            return name;
        }
    }

    public static List<StartCase> startCases() {
        long futureLimit = TestTime.epochNanos(NOW.plus(Duration.ofDays(1)));
        long expiryLimit = TestTime.epochNanos(NOW.minus(RETENTION));
        return List.of(
                new StartCase("now", TestTime.epochNanos(NOW), SpanTimeVerdict.ACCEPT),
                new StartCase("exactly now + 1 day", futureLimit, SpanTimeVerdict.ACCEPT),
                new StartCase("now + 1 day + 1 ns", futureLimit + 1, SpanTimeVerdict.FUTURE),
                new StartCase("exactly now - 7 days", expiryLimit, SpanTimeVerdict.ACCEPT),
                new StartCase("now - 7 days - 1 ns", expiryLimit - 1, SpanTimeVerdict.EXPIRED));
    }

    public static List<NanosCase> nanosCases() {
        return List.of(
                nanosCase(
                        "whole milliseconds",
                        "1795252502114000000",
                        "2026-11-21T09:15:02.114Z",
                        "2026-11-21T09:15:02.114Z"),
                nanosCase(
                        "sub-microsecond digits are cut, not rounded",
                        "1795252502114999999",
                        "2026-11-21T09:15:02.114999999Z",
                        "2026-11-21T09:15:02.114999Z"),
                nanosCase(
                        "last digit survives (a double would lose it)",
                        "1795252502114000001",
                        "2026-11-21T09:15:02.114000001Z",
                        "2026-11-21T09:15:02.114Z"),
                nanosCase(
                        "before 1970 rounds down",
                        "-1",
                        "1969-12-31T23:59:59.999999999Z",
                        "1969-12-31T23:59:59.999999Z"),
                nanosCase(
                        "largest long",
                        "9223372036854775807",
                        "2262-04-11T23:47:16.854775807Z",
                        "2262-04-11T23:47:16.854775Z"));
    }

    /** Texts that are not a decimal {@code long}. */
    public static List<String> invalidNanosTexts() {
        return List.of("", "1.5e18", "1795252502114000000.0", "9223372036854775808", " 1");
    }

    /** A root at 23:59:59.900 on 2026-11-21 lasting 300 ms, and its child starting at 00:00:00.050 the next day. */
    public static List<Span> midnightTrace() {
        Span root = SpanBuilder.aSpan()
                .start(Instant.parse("2026-11-21T23:59:59.900Z"))
                .duration(Duration.ofMillis(300))
                .build();
        Span child = SpanBuilder.aChildOf(root, "0e0e0e0e0e0e0e02")
                .start(Instant.parse("2026-11-22T00:00:00.050Z"))
                .build();
        return List.of(root, child);
    }

    private static NanosCase nanosCase(String name, String text, String exact, String truncatedToMicros) {
        return new NanosCase(name, text, Long.parseLong(text), Instant.parse(exact), Instant.parse(truncatedToMicros));
    }
}
