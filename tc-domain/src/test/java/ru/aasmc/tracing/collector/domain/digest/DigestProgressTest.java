package ru.aasmc.tracing.collector.domain.digest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.trace.TimeRange;
import ru.aasmc.tracing.collector.testmodel.domain.Transitions;

class DigestProgressTest {

    private static final Duration CHUNK = Duration.ofHours(1);
    private static final LocalDate DAY = LocalDate.of(2026, 11, 21);
    private static final Instant MIDNIGHT = Instant.parse("2026-11-21T00:00:00Z");
    private static final Instant ONE_AM = MIDNIGHT.plus(CHUNK);
    private static final Instant NEXT_MIDNIGHT = Instant.parse("2026-11-22T00:00:00Z");
    private static final Instant NOW = Instant.parse("2026-11-29T03:00:00Z");

    @ParameterizedTest
    @CsvSource({
        "PENDING, PENDING, true",
        "PENDING, RUNNING, true",
        "PENDING, DONE,    false",
        "PENDING, FAILED,  false",
        "RUNNING, PENDING, false",
        "RUNNING, RUNNING, true",
        "RUNNING, DONE,    true",
        "RUNNING, FAILED,  true",
        "DONE,    PENDING, false",
        "DONE,    RUNNING, false",
        "DONE,    DONE,    true",
        "DONE,    FAILED,  false",
        "FAILED,  PENDING, false",
        "FAILED,  RUNNING, true",
        "FAILED,  DONE,    false",
        "FAILED,  FAILED,  true"
    })
    @DisplayName("a day goes from pending through running to done, and a failed run is retried")
    void given_statuses_when_transitionTo_then_allowedOrRejected(DigestStatus from, DigestStatus to, boolean allowed) {
        Transitions.assertTransition(from.transitionTo(to), from, to, allowed, ViolationCode.INVALID_DIGEST_TRANSITION);
    }

    @Test
    @DisplayName("a pending day starts with its first one-hour chunk")
    void given_pendingDay_when_started_then_firstChunkFromMidnight() {
        DigestProgress pending = DigestProgress.pending(DAY);
        DigestProgress running = pending.startOrResume(NOW);

        assertThat(pending).isEqualTo(new DigestProgress(DAY, DigestStatus.PENDING, null, 0, null, null, null));
        assertThat(running).isEqualTo(new DigestProgress(DAY, DigestStatus.RUNNING, null, 0, NOW, null, null));
        assertThat(running.nextChunk(CHUNK)).contains(new TimeRange(MIDNIGHT, ONE_AM));
    }

    @Test
    @DisplayName("each digested chunk moves the next chunk on by an hour until the day is done")
    void given_runningDay_when_chunksDigested_then_progressSavedUntilDayEnds() {
        DigestProgress progress = DigestProgress.pending(DAY).startOrResume(NOW).chunkDigested(ONE_AM, 5);

        assertThat(progress.digestedUntil()).isEqualTo(ONE_AM);
        assertThat(progress.tracesDigested()).isEqualTo(5);
        assertThat(progress.nextChunk(CHUNK)).contains(new TimeRange(ONE_AM, ONE_AM.plus(CHUNK)));

        DigestProgress lastChunkDone = progress.chunkDigested(NEXT_MIDNIGHT, 7);
        assertThat(lastChunkDone.nextChunk(CHUNK)).isEmpty();
        assertThat(lastChunkDone.finish(NOW.plusSeconds(60)))
                .isEqualTo(
                        new DigestProgress(DAY, DigestStatus.DONE, NEXT_MIDNIGHT, 12, NOW, NOW.plusSeconds(60), null));
    }

    @Test
    @DisplayName("a failed run resumes after the last digested chunk")
    void given_failedRun_when_resumed_then_keepsDigestedChunks() {
        DigestProgress failed = DigestProgress.pending(DAY)
                .startOrResume(NOW)
                .chunkDigested(ONE_AM, 5)
                .fail("database unavailable");
        Instant retry = NOW.plusSeconds(600);

        assertThat(failed.status()).isEqualTo(DigestStatus.FAILED);
        assertThat(failed.failureReason()).isEqualTo("database unavailable");
        assertThat(failed.startOrResume(retry))
                .isEqualTo(new DigestProgress(DAY, DigestStatus.RUNNING, ONE_AM, 5, retry, null, null));
    }

    @Test
    @DisplayName("refuses to finish a day that never ran and to restart a finished day")
    void given_wrongStatus_when_changed_then_invalidDigestTransition() {
        DigestProgress done = DigestProgress.pending(DAY)
                .startOrResume(NOW)
                .chunkDigested(NEXT_MIDNIGHT, 1)
                .finish(NOW);

        assertViolation(
                ViolationCode.INVALID_DIGEST_TRANSITION,
                () -> DigestProgress.pending(DAY).finish(NOW));
        assertViolation(ViolationCode.INVALID_DIGEST_TRANSITION, () -> done.startOrResume(NOW));
    }

    @Test
    @DisplayName("""
            Given a running day digested only until 01:00,
            when it is finished,
            then it is refused with INVALID_DIGEST_TRANSITION: DONE lets the day's full data be dropped,
            so a day finishes only when every chunk up to midnight is digested""")
    void given_dayNotDigestedToTheEnd_when_finished_then_invalidDigestTransition() {
        DigestProgress running = DigestProgress.pending(DAY).startOrResume(NOW);

        assertViolation(ViolationCode.INVALID_DIGEST_TRANSITION, () -> running.finish(NOW));
        assertViolation(
                ViolationCode.INVALID_DIGEST_TRANSITION,
                () -> running.chunkDigested(ONE_AM, 5).finish(NOW));
    }

    @Test
    @DisplayName("""
            Given a running day digested until 01:00,
            when a chunk ending at 01:00 or earlier is recorded, or a chunk with a negative trace count,
            then it is rejected: progress only moves forward""")
    void given_chunkNotMovingForward_when_recorded_then_rejected() {
        DigestProgress atOneAm = DigestProgress.pending(DAY).startOrResume(NOW).chunkDigested(ONE_AM, 5);

        assertThatIllegalArgumentException().isThrownBy(() -> atOneAm.chunkDigested(ONE_AM, 1));
        assertThatIllegalArgumentException().isThrownBy(() -> atOneAm.chunkDigested(MIDNIGHT.plusSeconds(1), 1));
        assertThatIllegalArgumentException().isThrownBy(() -> atOneAm.chunkDigested(ONE_AM.plus(CHUNK), -1));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> DigestProgress.pending(DAY).startOrResume(NOW).chunkDigested(MIDNIGHT, 0));
    }

    @Test
    @DisplayName("""
            Given a day left RUNNING by a run that stopped without finishing or failing (e.g. its instance crashed),
            when another instance takes it over,
            then it keeps the digested chunks and records the new start time""")
    void given_runningDayLeftBehind_when_takenOver_then_keepsChunksAndRecordsNewStart() {
        DigestProgress leftBehind =
                DigestProgress.pending(DAY).startOrResume(NOW).chunkDigested(ONE_AM, 5);
        Instant takeover = NOW.plusSeconds(900);

        assertThat(leftBehind.startOrResume(takeover))
                .isEqualTo(new DigestProgress(DAY, DigestStatus.RUNNING, ONE_AM, 5, takeover, null, null));
    }

    @Test
    @DisplayName("progress stays within the day")
    void given_progressBeyondDay_when_constructed_then_rejected() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new DigestProgress(
                        DAY, DigestStatus.RUNNING, NEXT_MIDNIGHT.plusSeconds(1), 0, NOW, null, null));
    }

    @Test
    @DisplayName("""
            Given a chunk length that does not divide the day (7 hours),
            when the progress reaches 21:00,
            then the next chunk ends at midnight (3 hours), not at 04:00 of the next day""")
    void given_chunkLengthNotDividingDay_when_lastChunk_then_cutAtMidnight() {
        Duration sevenHours = Duration.ofHours(7);
        DigestProgress at21 =
                DigestProgress.pending(DAY).startOrResume(NOW).chunkDigested(MIDNIGHT.plus(Duration.ofHours(21)), 3);

        assertThat(at21.nextChunk(sevenHours))
                .contains(new TimeRange(MIDNIGHT.plus(Duration.ofHours(21)), NEXT_MIDNIGHT));
    }

    @Test
    @DisplayName("the chunk length is a positive duration (a configuration error otherwise)")
    void given_nonPositiveChunkLength_when_nextChunk_then_rejected() {
        DigestProgress running = DigestProgress.pending(DAY).startOrResume(NOW);

        assertThatIllegalArgumentException().isThrownBy(() -> running.nextChunk(Duration.ZERO));
        assertThatIllegalArgumentException().isThrownBy(() -> running.nextChunk(Duration.ofHours(-1)));
    }

    @Test
    @DisplayName("""
            the record keeps its rules: no negative trace count, progress not before the day starts,
            a finish time only when DONE, a failure reason only when FAILED""")
    void given_inconsistentFields_when_constructed_then_rejected() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new DigestProgress(DAY, DigestStatus.RUNNING, null, -1, NOW, null, null));
        assertThatIllegalArgumentException()
                .isThrownBy(() ->
                        new DigestProgress(DAY, DigestStatus.RUNNING, MIDNIGHT.minusSeconds(1), 0, NOW, null, null));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new DigestProgress(DAY, DigestStatus.RUNNING, null, 0, NOW, NOW, null));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new DigestProgress(DAY, DigestStatus.RUNNING, null, 0, NOW, null, "broken"));
    }

    @Test
    @DisplayName("""
            only a running day digests chunks, and only a running day can fail:
            a pending or finished day refuses both with INVALID_DIGEST_TRANSITION""")
    void given_notRunning_when_chunkDigestedOrFailed_then_invalidDigestTransition() {
        DigestProgress pending = DigestProgress.pending(DAY);
        DigestProgress done =
                pending.startOrResume(NOW).chunkDigested(NEXT_MIDNIGHT, 1).finish(NOW);

        assertViolation(ViolationCode.INVALID_DIGEST_TRANSITION, () -> pending.chunkDigested(ONE_AM, 1));
        assertViolation(ViolationCode.INVALID_DIGEST_TRANSITION, () -> done.chunkDigested(NEXT_MIDNIGHT, 1));
        assertViolation(ViolationCode.INVALID_DIGEST_TRANSITION, () -> pending.fail("never started"));
    }
}
