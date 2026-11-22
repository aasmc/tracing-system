package ru.aasmc.tracing.collector.domain.digest;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;
import ru.aasmc.tracing.collector.domain.trace.TimeRange;

/**
 * Row of {@code tc.compaction_state}, one per trace day; written only by the {@code DigestCompactor} leader (05
 * §4.14.3, §4.14.4 C1–C3).
 *
 * <p>Invariants: counters {@code >= 0}; {@code lastChunkEnd}, if set, lies within the day; {@code finishedAt} only
 * when DONE; {@code error} only when FAILED. Transitions follow {@link CompactionStatus#transitionTo}; illegal ones
 * throw {@code DomainViolationException} with {@code INVALID_COMPACTION_TRANSITION}.
 *
 * @param traceDay the day
 * @param status status
 * @param lastChunkEnd end of the last committed 1-hour chunk, {@code null} before the first
 * @param tracesDigested digested traces
 * @param tracesExcluded traces skipped by exclusions
 * @param operationsDigested digested business operations
 * @param startedAt when the current run started
 * @param finishedAt when the day was DONE
 * @param error last error (truncated), when FAILED
 */
public record CompactionState(
        LocalDate traceDay,
        CompactionStatus status,
        Instant lastChunkEnd,
        long tracesDigested,
        long tracesExcluded,
        long operationsDigested,
        Instant startedAt,
        Instant finishedAt,
        String error) {

    /** Length of one compaction chunk (C1). */
    public static final Duration CHUNK = Duration.ofHours(1);

    /** A new PENDING row for an eligible day. */
    public static CompactionState pending(LocalDate traceDay) {
        throw new UnsupportedOperationException();
    }

    /** The whole day is older than n days: {@code D + 1 day <= now - fullDays}. */
    public static boolean eligible(LocalDate day, Instant now, int fullDays) {
        throw new UnsupportedOperationException();
    }

    /** C2: retained values are read from {@code trace_attribute} with {@code ts} in {@code [D - 1 h, D + 1 d + 6 h)}. */
    public static TimeRange readWindow(LocalDate day) {
        throw new UnsupportedOperationException();
    }

    /** The next chunk to compact: {@code [lastChunkEnd or D 00:00, + 1 h)}; empty when the day is covered. */
    public Optional<TimeRange> nextChunk() {
        throw new UnsupportedOperationException();
    }

    /** PENDING/FAILED → RUNNING (keeps {@code lastChunkEnd}); RUNNING → RUNNING unchanged. */
    public CompactionState start(Instant now) {
        throw new UnsupportedOperationException();
    }

    /** Records a committed chunk; only while RUNNING. */
    public CompactionState chunkDone(Instant chunkEnd, long digested, long excluded) {
        throw new UnsupportedOperationException();
    }

    /** Adds digested operations (C5); only while RUNNING or DONE. */
    public CompactionState operationsDigested(long count) {
        throw new UnsupportedOperationException();
    }

    /** RUNNING → DONE. */
    public CompactionState done(Instant now) {
        throw new UnsupportedOperationException();
    }

    /** RUNNING → FAILED with the error text cut to 1 KiB. */
    public CompactionState failed(String error) {
        throw new UnsupportedOperationException();
    }
}
