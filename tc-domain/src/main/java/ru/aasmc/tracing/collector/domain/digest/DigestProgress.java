package ru.aasmc.tracing.collector.domain.digest;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Optional;
import ru.aasmc.tracing.collector.domain.DomainViolationException;
import ru.aasmc.tracing.collector.domain.Limits;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.trace.TimeRange;

/**
 * Progress of turning one day of full trace data into digests. The day is processed in chunks of a configured length and the
 * progress is saved after each chunk, so a restarted or failed run continues where it stopped instead of starting
 * over.
 *
 * <p>Rules: {@code tracesDigested >= 0}; {@code digestedUntil}, if set, lies within the day; {@code finishedAt} is set
 * only when DONE and {@code failureReason} only when FAILED. A step that {@link DigestStatus#transitionTo} forbids,
 * and finishing a day that is not digested up to its end, throw {@code INVALID_DIGEST_TRANSITION}.
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

    public DigestProgress {
        if (tracesDigested < 0) {
            throw new IllegalArgumentException("negative trace count: " + tracesDigested);
        }
        if (digestedUntil != null
                && (digestedUntil.isBefore(startOf(traceDay)) || digestedUntil.isAfter(endOf(traceDay)))) {
            throw new IllegalArgumentException(digestedUntil + " is outside " + traceDay);
        }
        if (finishedAt != null && status != DigestStatus.DONE) {
            throw new IllegalArgumentException("only a DONE day has a finish time, not a " + status + " day");
        }
        if (failureReason != null && status != DigestStatus.FAILED) {
            throw new IllegalArgumentException("only a FAILED day has a failure reason, not a " + status + " day");
        }
    }

    /** A day waiting to be digested. */
    public static DigestProgress pending(LocalDate traceDay) {
        return new DigestProgress(traceDay, DigestStatus.PENDING, null, 0, null, null, null);
    }

    /**
     * The next chunk: {@code chunkLength} from {@code digestedUntil} (or from midnight), cut at the end of the day;
     * empty when the whole day is done.
     */
    public Optional<TimeRange> nextChunk(Duration chunkLength) {
        Limits.requirePositive("chunkLength", chunkLength);
        Instant from = digestedUntil != null ? digestedUntil : startOf(traceDay);
        Instant dayEnd = endOf(traceDay);
        if (!from.isBefore(dayEnd)) {
            return Optional.empty();
        }
        Instant to = from.plus(chunkLength);
        return Optional.of(new TimeRange(from, to.isBefore(dayEnd) ? to : dayEnd));
    }

    /**
     * Starts a run of a pending day, resumes a failed run, or takes over a running day whose run stopped without
     * finishing or failing (e.g. its instance crashed). The chunks already digested are kept, and {@code startedAt}
     * becomes {@code now}: the start of the run that is going on now.
     */
    public DigestProgress startOrResume(Instant now) {
        DigestStatus running = status.transitionTo(DigestStatus.RUNNING).resultingStateOrThrow();
        return new DigestProgress(traceDay, running, digestedUntil, tracesDigested, now, null, null);
    }

    /**
     * Records that the chunk ending at {@code chunkEnd} was digested, with {@code traces} traces. Progress only moves
     * forward: {@code chunkEnd} lies after the previous chunk's end, and {@code traces >= 0}.
     */
    public DigestProgress chunkDigested(Instant chunkEnd, long traces) {
        if (status != DigestStatus.RUNNING) {
            throw new DomainViolationException(
                    ViolationCode.INVALID_DIGEST_TRANSITION, "a " + status + " day cannot digest chunks");
        }
        Instant previousEnd = digestedUntil != null ? digestedUntil : startOf(traceDay);
        if (!chunkEnd.isAfter(previousEnd)) {
            throw new IllegalArgumentException("chunk end " + chunkEnd + " is not after " + previousEnd);
        }
        if (traces < 0) {
            throw new IllegalArgumentException("negative trace count of a chunk: " + traces);
        }
        return new DigestProgress(traceDay, status, chunkEnd, tracesDigested + traces, startedAt, null, null);
    }

    /** Marks the day DONE; only a day digested up to its end may finish, since DONE lets its full data be dropped. */
    public DigestProgress finish(Instant now) {
        DigestStatus done = status.transitionTo(DigestStatus.DONE).resultingStateOrThrow();
        if (!endOf(traceDay).equals(digestedUntil)) {
            throw new DomainViolationException(
                    ViolationCode.INVALID_DIGEST_TRANSITION,
                    traceDay + " is digested only until " + digestedUntil + ", so it cannot finish");
        }
        return new DigestProgress(traceDay, done, digestedUntil, tracesDigested, startedAt, now, null);
    }

    public DigestProgress fail(String reason) {
        DigestStatus failed = status.transitionTo(DigestStatus.FAILED).resultingStateOrThrow();
        return new DigestProgress(traceDay, failed, digestedUntil, tracesDigested, startedAt, null, reason);
    }

    private static Instant startOf(LocalDate day) {
        return day.atStartOfDay(ZoneOffset.UTC).toInstant();
    }

    private static Instant endOf(LocalDate day) {
        return startOf(day.plusDays(1));
    }
}
