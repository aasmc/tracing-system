package ru.aasmc.tracing.collector.testmodel.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue;
import ru.aasmc.tracing.collector.domain.resource.InstrumentationScopeId;
import ru.aasmc.tracing.collector.domain.resource.ResourceId;
import ru.aasmc.tracing.collector.domain.trace.DroppedCounts;
import ru.aasmc.tracing.collector.domain.trace.Span;
import ru.aasmc.tracing.collector.domain.trace.SpanFlag;
import ru.aasmc.tracing.collector.domain.trace.SpanId;
import ru.aasmc.tracing.collector.domain.trace.SpanKind;
import ru.aasmc.tracing.collector.domain.trace.SpanStatus;
import ru.aasmc.tracing.collector.domain.trace.StatusCode;
import ru.aasmc.tracing.collector.domain.trace.TraceFlags;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * Builds valid spans. Defaults: a 1 ms SERVER root span {@code GET /} of {@code test-service} at
 * 2026-11-21T09:00:00Z with status UNSET and no attributes, events, links or flags.
 */
public final class SpanBuilder {

    private TraceId traceId = DomainIds.traceId("0af7651916cd43dd8448eb211c80319c");
    private SpanId spanId = DomainIds.spanId("b7ad6b7169203331");
    private SpanId parentSpanId;
    private String name = "GET /";
    private SpanKind kind = SpanKind.SERVER;
    private long startEpochNanos = TestTime.epochNanos("2026-11-21T09:00:00Z");
    private long durationNanos = Duration.ofMillis(1).toNanos();
    private SpanStatus status = SpanStatus.UNSET;
    private String serviceName = "test-service";
    private final AttributesBuilder attributes = AttributesBuilder.attributes();
    private final Set<SpanFlag> collectorFlags = EnumSet.noneOf(SpanFlag.class);

    public static SpanBuilder aSpan() {
        return new SpanBuilder();
    }

    /** A child of {@code parent}: same trace and service, INTERNAL, starting with the parent. */
    public static SpanBuilder aChildOf(Span parent, String spanIdHex) {
        return aSpan().traceId(parent.traceId())
                .spanId(spanIdHex)
                .parent(parent.spanId())
                .kind(SpanKind.INTERNAL)
                .start(TestTime.instantOf(parent.startEpochNanos()))
                .service(parent.serviceName());
    }

    public SpanBuilder traceId(TraceId value) {
        traceId = value;
        return this;
    }

    public SpanBuilder spanId(String hex) {
        spanId = DomainIds.spanId(hex);
        return this;
    }

    public SpanBuilder parent(SpanId value) {
        parentSpanId = value;
        return this;
    }

    public SpanBuilder name(String value) {
        name = value;
        return this;
    }

    public SpanBuilder kind(SpanKind value) {
        kind = value;
        return this;
    }

    public SpanBuilder start(Instant value) {
        startEpochNanos = TestTime.epochNanos(value);
        return this;
    }

    public SpanBuilder duration(Duration value) {
        durationNanos = value.toNanos();
        return this;
    }

    public SpanBuilder error() {
        status = new SpanStatus(StatusCode.ERROR, "failed");
        return this;
    }

    public SpanBuilder service(String value) {
        serviceName = value;
        return this;
    }

    public SpanBuilder attribute(String key, String value) {
        attributes.put(key, value);
        return this;
    }

    public SpanBuilder attribute(String key, AttributeValue value) {
        attributes.put(key, value);
        return this;
    }

    public SpanBuilder collectorFlag(SpanFlag value) {
        collectorFlags.add(value);
        return this;
    }

    public Span build() {
        return new Span(
                traceId,
                spanId,
                parentSpanId,
                name,
                kind,
                startEpochNanos,
                durationNanos,
                status,
                new ResourceId(serviceName.hashCode()),
                serviceName,
                new InstrumentationScopeId(1),
                new TraceFlags(1),
                null,
                attributes.build(),
                List.of(),
                List.of(),
                DroppedCounts.NONE,
                Set.copyOf(collectorFlags));
    }
}
