package ru.aasmc.tracing.collector.domain.enrichment;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.aasmc.tracing.collector.testmodel.domain.TraceContextVectors.SPAN;
import static ru.aasmc.tracing.collector.testmodel.domain.TraceContextVectors.TRACE;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.enrichment.TraceContextSource.PayloadField;
import ru.aasmc.tracing.collector.domain.enrichment.TraceContextSource.TraceIdHeader;
import ru.aasmc.tracing.collector.domain.enrichment.TraceContextSource.TraceparentHeader;
import ru.aasmc.tracing.collector.domain.trace.TraceFlags;
import ru.aasmc.tracing.collector.domain.trace.Traceparent;
import ru.aasmc.tracing.collector.testmodel.domain.DomainIds;
import ru.aasmc.tracing.collector.testmodel.domain.IdpScenario;
import ru.aasmc.tracing.collector.testmodel.domain.InMemoryEnrichmentEvent;
import ru.aasmc.tracing.collector.testmodel.domain.TraceContextVectors.Vector;

class TraceContextSourceTest {

    private static final TraceContext TRACE_AND_SPAN =
            new TraceContext(DomainIds.traceId(TRACE), DomainIds.spanId(SPAN));
    private static final TraceContext TRACE_ONLY = new TraceContext(DomainIds.traceId(TRACE), null);

    @ParameterizedTest(name = "{0}")
    @MethodSource("ru.aasmc.tracing.collector.testmodel.domain.TraceContextVectors#traceparents")
    @DisplayName("parses W3C traceparent values: version ff is invalid, future versions may append fields")
    void given_traceparent_when_parsed_then_expectedFieldsOrEmpty(Vector vector) {
        Optional<Traceparent> expected = vector.valid()
                ? Optional.of(new Traceparent(
                        vector.version(),
                        DomainIds.traceId(TRACE),
                        DomainIds.spanId(SPAN),
                        new TraceFlags(vector.flags())))
                : Optional.empty();

        assertThat(Traceparent.parse(vector.value())).isEqualTo(expected);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ru.aasmc.tracing.collector.testmodel.domain.TraceContextVectors#traceparents")
    @DisplayName("a traceparent header names the trace and the span")
    void given_traceparentHeader_when_extracted_then_traceAndSpan(Vector vector) {
        EnrichmentEvent event = vector.value() == null
                ? InMemoryEnrichmentEvent.anEvent()
                : InMemoryEnrichmentEvent.anEvent().withHeader("traceparent", vector.value());
        Optional<TraceContext> expected = vector.valid() ? Optional.of(TRACE_AND_SPAN) : Optional.empty();

        assertThat(new TraceparentHeader(null).extract(event)).isEqualTo(expected);
    }

    @Test
    @DisplayName("the SMS delivery report points at the T2 span that sent the SMS")
    void given_smsDeliveryReport_when_extracted_then_smsSpanOfT2() {
        EnrichmentEvent deliveryReport =
                InMemoryEnrichmentEvent.anEvent().withHeader("traceparent", IdpScenario.SMS_TRACEPARENT);

        assertThat(new TraceparentHeader(null).extract(deliveryReport))
                .contains(new TraceContext(IdpScenario.T2, DomainIds.spanId("5f3a9b2c1d0e4f68")));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = " ")
    @DisplayName("a traceparent header without a name reads the traceparent header")
    void given_noHeaderName_when_constructed_then_defaultHeader(String headerName) {
        assertThat(new TraceparentHeader(headerName).headerName()).isEqualTo(TraceparentHeader.DEFAULT_HEADER);
    }

    @Test
    @DisplayName("a source reads only the place it names")
    void given_valueElsewhere_when_extracted_then_empty() {
        String traceparent = "00-" + TRACE + "-" + SPAN + "-01";

        assertThat(new TraceparentHeader("X-Parent")
                        .extract(InMemoryEnrichmentEvent.anEvent().withHeader("X-Parent", traceparent)))
                .contains(TRACE_AND_SPAN);
        assertThat(new TraceparentHeader("X-Parent")
                        .extract(InMemoryEnrichmentEvent.anEvent().withHeader("traceparent", traceparent)))
                .isEmpty();
        assertThat(new TraceIdHeader("X-Trace-Id")
                        .extract(InMemoryEnrichmentEvent.anEvent().withHeader("X-Other", TRACE)))
                .isEmpty();
        assertThat(new PayloadField(new JsonPointer("/trace"))
                        .extract(InMemoryEnrichmentEvent.anEvent().withPayload("/other", TRACE)))
                .isEmpty();
    }

    @Test
    @DisplayName("a trace-id header holds a bare trace id in either case and names no span")
    void given_bareTraceId_when_traceIdHeaderExtracted_then_traceOnly() {
        TraceIdHeader source = new TraceIdHeader("X-Trace-Id");

        assertThat(source.providesSpanId()).isFalse();
        assertThat(source.extract(traceIdHeader(TRACE))).contains(TRACE_ONLY);
        assertThat(source.extract(traceIdHeader(TRACE.toUpperCase(Locale.ROOT))))
                .contains(TRACE_ONLY);
        assertThat(source.extract(traceIdHeader(TRACE.substring(1)))).isEmpty();
        assertThat(source.extract(traceIdHeader("0".repeat(32)))).isEmpty();
        assertThat(source.extract(InMemoryEnrichmentEvent.anEvent())).isEmpty();
    }

    @Test
    @DisplayName("a payload field holds a bare trace id or a traceparent as a string; only the trace is used")
    void given_payloadValue_when_payloadFieldExtracted_then_traceOnly() {
        PayloadField source = new PayloadField(new JsonPointer("/trace"));

        assertThat(source.providesSpanId()).isFalse();
        assertThat(source.extract(traceField(TRACE))).contains(TRACE_ONLY);
        assertThat(source.extract(traceField("00-" + TRACE + "-" + SPAN + "-01")))
                .contains(TRACE_ONLY);
        assertThat(source.extract(traceField("ff-" + TRACE + "-" + SPAN + "-01")))
                .isEmpty();
        assertThat(source.extract(InMemoryEnrichmentEvent.anEvent()
                        .withPayload("/trace", new JsonScalar.NumberScalar(BigDecimal.ONE))))
                .isEmpty();
        assertThat(source.extract(InMemoryEnrichmentEvent.anEvent())).isEmpty();
    }

    @Test
    @DisplayName("only a traceparent header can name a span")
    void given_traceparentHeader_when_asked_then_providesSpanId() {
        assertThat(new TraceparentHeader(null).providesSpanId()).isTrue();
    }

    @Test
    @DisplayName("a trace-id header needs a name and a payload field needs a pointer")
    void given_missingLocation_when_constructed_then_invalidTraceContext() {
        assertViolation(ViolationCode.INVALID_TRACE_CONTEXT, () -> new TraceIdHeader(" "));
        assertViolation(ViolationCode.INVALID_TRACE_CONTEXT, () -> new TraceIdHeader(null));
        assertViolation(ViolationCode.INVALID_TRACE_CONTEXT, () -> new PayloadField(null));
    }

    private static EnrichmentEvent traceIdHeader(String value) {
        return InMemoryEnrichmentEvent.anEvent().withHeader("X-Trace-Id", value);
    }

    private static EnrichmentEvent traceField(String value) {
        return InMemoryEnrichmentEvent.anEvent().withPayload("/trace", value);
    }
}
