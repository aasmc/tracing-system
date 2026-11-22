package ru.aasmc.tracing.collector.domain.digest;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;
import ru.aasmc.tracing.collector.domain.trace.TimeRange;

/**
 * Progress of turning one day of full trace data into digests. The day is processed in one-hour chunks and the
 * progress is saved after each chunk, so a restarted or failed run continues where it stopped instead of starting
 * over.
 *
 * <p>Rules: {@code tracesDigested >= 0}; {@code digestedUntil}, if set, lies within the day; {@code finishedAt} is set
 * only when DONE and {@code failureReason} only when FAILED. A step that {@link DigestStatus#transitionTo} forbids
 * throws {@code INVALID_DIGEST_TRANSITION}.
 *
 * @param traceDay the day
 * @param status status
 * @param digestedUntil end of the last digested chunk, {@code null} before the first
 * @param tracesDigested traces digested so far
 * @param startedAt when the current run started
 * @param finishedAt when the whole day was digested
 * @param failureReason why the last run failed
 */
public record DigestProgress(
        LocalDate traceDay,
        DigestStatus status,
        Instant digestedUntil,
        long tracesDigested,
        Instant startedAt,
        Instant finishedAt,
        String failureReason) {

    public static final Duration CHUNK_LENGTH = Duration.ofHours(1);

    /** A day waiting to be digested. */
    public static DigestProgress pending(LocalDate traceDay) {
        throw new UnsupportedOperationException();
    }

    /** The next chunk: one hour from {@code digestedUntil} (or from midnight); empty when the whole day is done. */
    public Optional<TimeRange> nextChunk() {
        throw new UnsupportedOperationException();
    }

    /** Starts a run, or resumes a failed one, keeping the chunks already digested. */
    public DigestProgress startOrResume(Instant now) {
        throw new UnsupportedOperationException();
    }

    /** Records that a chunk ending at {@code chunkEnd} was digested, with {@code traces} traces. */
    public DigestProgress chunkDigested(Instant chunkEnd, long traces) {
        throw new UnsupportedOperationException();
    }

    public DigestProgress finish(Instant now) {
        throw new UnsupportedOperationException();
    }

    public DigestProgress fail(String reason) {
        throw new UnsupportedOperationException();
    }
}
