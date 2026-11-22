package ru.aasmc.tracing.collector.domain.enrichment;

/** Kafka {@code security.protocol} (05 §4.6). */
public enum SecurityProtocol {
    PLAINTEXT,
    SSL,
    SASL_PLAINTEXT,
    SASL_SSL;

    public boolean usesSasl() {
        throw new UnsupportedOperationException();
    }
}
