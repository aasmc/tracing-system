package ru.aasmc.tracing.collector.domain.dlq;

/**
 * Why a record was copied to a DLQ topic; written to header {@code tc-error-reason} (05 §4.13).
 *
 * <p>Addition to 05: {@code PAYLOAD_TOO_LARGE} (03 §4, per-source {@code maxRecordBytes}).
 */
public enum DeadLetterReason {
    // otlp-traces.dlq
    MALFORMED_JSON(Topic.TRACES),
    UNSUPPORTED_SCHEMA_VERSION(Topic.TRACES),
    INVALID_TRACE_ID(Topic.TRACES),
    INVALID_SPAN_ID(Topic.TRACES),
    TRACE_ID_MISMATCH(Topic.TRACES),
    FUTURE_TIMESTAMP(Topic.TRACES),
    FRAGMENT_TOO_LARGE(Topic.TRACES),
    INVALID_ATTRIBUTE_TYPE(Topic.TRACES),
    // tc-enrichment.dlq
    NO_TRACE_CONTEXT(Topic.ENRICHMENT),
    PAYLOAD_DECODE_ERROR(Topic.ENRICHMENT),
    PAYLOAD_TOO_LARGE(Topic.ENRICHMENT),
    REQUIRED_FIELD_MISSING(Topic.ENRICHMENT),
    TRANSFORM_ERROR(Topic.ENRICHMENT),
    VALUE_TYPE_MISMATCH(Topic.ENRICHMENT);

    /** The two pre-created DLQ topics (ADR-01). */
    public enum Topic {
        TRACES("otlp-traces.dlq"),
        ENRICHMENT("tc-enrichment.dlq");

        private final String defaultName;

        Topic(String defaultName) {
            this.defaultName = defaultName;
        }

        /** Default topic name; deployments may override it by configuration. */
        public String defaultName() {
            return defaultName;
        }
    }

    private final Topic topic;

    DeadLetterReason(Topic topic) {
        this.topic = topic;
    }

    public Topic topic() {
        return topic;
    }
}
