package ru.aasmc.tracing.collector.domain.trace;

import java.util.List;

/**
 * One {@code TraceMessage} after parsing and normalisation; transient, lives for one poll batch (05 §3.7).
 *
 * <p>Invariants: {@code encodedBytes <= MAX_BYTES} ({@code FRAGMENT_TOO_LARGE}); every span has the fragment's trace
 * id ({@code TRACE_ID_MISMATCH}); spans are copied into an immutable list. An empty span list is legal: it is what is
 * left after all spans were dropped as expired, and the record is still acknowledged.
 *
 * @param traceId the trace (= Kafka record key)
 * @param spans normalised spans
 * @param source record coordinates
 * @param encodedBytes size of the record value
 */
public record TraceFragment(TraceId traceId, List<Span> spans, KafkaCoordinates source, int encodedBytes) {

    /** 8 MiB. */
    public static final int MAX_BYTES = 8 * 1024 * 1024;

    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    /**
     * Minimum span start.
     *
     * @throws java.util.NoSuchElementException if the fragment is empty
     */
    public long minStartNanos() {
        throw new UnsupportedOperationException();
    }

    /**
     * Maximum span end.
     *
     * @throws java.util.NoSuchElementException if the fragment is empty
     */
    public long maxEndNanos() {
        throw new UnsupportedOperationException();
    }
}
