package ru.aasmc.tracing.collector.domain.config;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.time.Duration;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * How long trace data is kept. Full data (every span) is kept {@code fullDataDays}; after that each trace is reduced to a
 * small digest (outcome, business operation, chosen attributes) that is kept {@code digestDays} more.
 *
 * <p>Rules: {@code fullDataDays >= }{@value #MIN_FULL_DATA_DAYS} ({@code FULL_DATA_DAYS_TOO_SMALL}), so yesterday's data is never
 * deleted before it is digested; {@code digestDays >= 0}, where 0 means no digest ({@code INVALID_DIGEST_DAYS}).
 *
 * @param fullDataDays days of full data
 * @param digestDays days of digest after the full data is gone
 */
public record RetentionTiers(int fullDataDays, int digestDays) {

    public static final int MIN_FULL_DATA_DAYS = 2;

    public RetentionTiers {
        require(
                fullDataDays >= MIN_FULL_DATA_DAYS,
                ViolationCode.FULL_DATA_DAYS_TOO_SMALL,
                "full data is kept at least " + MIN_FULL_DATA_DAYS + " days, not " + fullDataDays);
        require(digestDays >= 0, ViolationCode.INVALID_DIGEST_DAYS, "negative digest days: " + digestDays);
    }

    /**
     * Traces of one business operation are linked while their data is still there, so full data must outlive the
     * longest merge window by at least a day.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code FULL_DATA_DAYS_BELOW_MERGE_WINDOW} unless
     *     {@code mergeWindow <= fullDataDays - 1 day}
     */
    public void requireFullDataOutlives(Duration mergeWindow) {
        Duration longestWindow = fullDataRetention().minusDays(1);
        require(
                mergeWindow.compareTo(longestWindow) <= 0,
                ViolationCode.FULL_DATA_DAYS_BELOW_MERGE_WINDOW,
                "merge window " + mergeWindow + " is longer than " + longestWindow);
    }

    /** {@code fullDataDays} as a duration. */
    public Duration fullDataRetention() {
        return Duration.ofDays(fullDataDays);
    }
}
