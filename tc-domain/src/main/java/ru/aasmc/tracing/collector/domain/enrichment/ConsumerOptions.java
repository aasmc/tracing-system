package ru.aasmc.tracing.collector.domain.enrichment;

/**
 * Optional per-source consumer overrides, {@code definition.consumer} (03 §4, §5.4 V13). Addition to the 05 sketch.
 *
 * <p>Invariants ({@code INVALID_CONSUMER_OPTIONS}): {@code 1 <= maxPollRecords <= 1000}; {@code 1 <= fetchMaxBytes <=
 * 16 MiB}; {@code 1 <= concurrency <= 4}; {@code 1 <= maxRecordBytes <= 4 MiB}.
 *
 * @param maxPollRecords {@code max.poll.records}
 * @param fetchMaxBytes {@code fetch.max.bytes}
 * @param concurrency listener container concurrency
 * @param maxRecordBytes larger records go to the DLQ ({@code PAYLOAD_TOO_LARGE})
 */
public record ConsumerOptions(int maxPollRecords, int fetchMaxBytes, int concurrency, int maxRecordBytes) {

    public static final int MAX_POLL_RECORDS = 1_000;
    public static final int MAX_FETCH_BYTES = 16 * 1024 * 1024;
    public static final int MAX_CONCURRENCY = 4;
    public static final int MAX_RECORD_BYTES = 4 * 1024 * 1024;

    /** 03 §4 defaults: 200 records, 8 MiB, concurrency 1, 1 MiB records. */
    public static final ConsumerOptions DEFAULT = new ConsumerOptions(200, 8 * 1024 * 1024, 1, 1024 * 1024);
}
