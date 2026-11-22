package ru.aasmc.tracing.collector.domain.dlq;

/**
 * Diagnostics of one dead-lettered record; the original key and value bytes are copied unchanged (05 §4.13).
 *
 * <p>Invariants: reason not null; {@code detail} is at most {@link #MAX_DETAIL_CHARS} characters and never contains
 * payload fragments (PII); {@code sourceId} only for {@link DeadLetterReason.Topic#ENRICHMENT}.
 *
 * @param reason why
 * @param detail short explanation
 * @param sourceId enrichment source id, or {@code null}
 */
public record DeadLetter(DeadLetterReason reason, String detail, Long sourceId) {

    /** 1 KiB. */
    public static final int MAX_DETAIL_CHARS = 1024;

    /** Cuts {@code detail} to {@link #MAX_DETAIL_CHARS}. */
    public static DeadLetter of(DeadLetterReason reason, String detail, Long sourceId) {
        throw new UnsupportedOperationException();
    }
}
