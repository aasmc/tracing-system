package ru.aasmc.tracing.collector.domain.enrichment;

/** Kafka {@code sasl.mechanism} (05 §4.6, 03 §5.4 V14). */
public enum SaslMechanism {
    PLAIN("PLAIN"),
    SCRAM_SHA_256("SCRAM-SHA-256"),
    SCRAM_SHA_512("SCRAM-SHA-512"),
    OAUTHBEARER("OAUTHBEARER");

    private final String kafkaName;

    SaslMechanism(String kafkaName) {
        this.kafkaName = kafkaName;
    }

    /** The value of the Kafka client property. */
    public String kafkaName() {
        return kafkaName;
    }

    /**
     * Maps the Kafka name ({@code SCRAM-SHA-256}) or the enum name.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_CONNECTION}
     */
    public static SaslMechanism fromKafkaName(String name) {
        throw new UnsupportedOperationException();
    }
}
