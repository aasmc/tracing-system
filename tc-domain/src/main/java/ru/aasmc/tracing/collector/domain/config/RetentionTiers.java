package ru.aasmc.tracing.collector.domain.config;

import java.time.Duration;

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
    public static final RetentionTiers DEFAULT = new RetentionTiers(7, 90);

    /**
     * Traces of one business operation are linked while their data is still there, so full data must outlive the
     * longest merge window by at least a day.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code FULL_DATA_DAYS_BELOW_MERGE_WINDOW} unless
     *     {@code mergeWindow <= fullDataDays - 1 day}
     */
    public void requireFullDataOutlives(Duration mergeWindow) {
        throw new UnsupportedOperationException();
    }

    /** {@code fullDataDays} as a duration. */
    public Duration fullDataRetention() {
        throw new UnsupportedOperationException();
    }
}
