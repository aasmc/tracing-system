package ru.aasmc.tracing.collector.testmodel.domain;

import java.time.Instant;
import java.util.UUID;
import ru.aasmc.tracing.collector.domain.correlation.BusinessOperation;

/**
 * Builds business operations. Defaults: business operation definition 1, display value {@code value-1}, one trace, seen
 * once, not merged.
 */
public final class BusinessOperationBuilder {

    private UUID id =
            DomainIds.uuidV7Sequence(Instant.parse("2026-11-21T09:00:00Z")).get();
    private Instant firstSeen = Instant.parse("2026-11-21T09:00:00Z");
    private Instant lastSeen = firstSeen;
    private String displayValue = "value-1";
    private int traceCount = 1;

    public static BusinessOperationBuilder anOperation() {
        return new BusinessOperationBuilder();
    }

    public BusinessOperationBuilder id(UUID value) {
        id = value;
        return this;
    }

    public BusinessOperationBuilder seen(Instant first, Instant last) {
        firstSeen = first;
        lastSeen = last;
        return this;
    }

    /** {@code null} makes the operation provisional. */
    public BusinessOperationBuilder displayValue(String value) {
        displayValue = value;
        return this;
    }

    public BusinessOperationBuilder traceCount(int value) {
        traceCount = value;
        return this;
    }

    public BusinessOperation build() {
        return new BusinessOperation(id, firstSeen, lastSeen, 1, displayValue, traceCount, null);
    }
}
