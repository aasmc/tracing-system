package ru.aasmc.tracing.collector.domain.trace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static ru.aasmc.tracing.collector.testmodel.domain.SpanBuilder.aChildOf;
import static ru.aasmc.tracing.collector.testmodel.domain.SpanBuilder.aSpan;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.StringValue;
import ru.aasmc.tracing.collector.testmodel.domain.AttributesBuilder;
import ru.aasmc.tracing.collector.testmodel.domain.DomainIds;

class SpanTest {

    @Test
    @DisplayName("a span without a parent is a root, and ends after its duration")
    void given_rootAndChild_when_asked_then_rootAndEnd() {
        Span root = aSpan().start(Instant.parse("2026-11-21T09:00:00Z"))
                .duration(Duration.ofMillis(5))
                .build();
        Span child = aChildOf(root, "0e0e0e0e0e0e0e02").build();

        assertThat(root.isRoot()).isTrue();
        assertThat(child.isRoot()).isFalse();
        assertThat(root.endEpochNanos()).isEqualTo(root.startEpochNanos() + 5_000_000);
    }

    @Test
    @DisplayName("a missing name becomes empty")
    void given_nullName_when_built_then_emptyName() {
        assertThat(aSpan().name(null).build().name()).isEmpty();
    }

    @Test
    @DisplayName("a cut attribute value is flagged on the span")
    void given_truncatedAttribute_when_built_then_attributeTruncatedFlag() {
        Span span = aSpan().attribute("payload", new StringValue("x", true)).build();

        assertThat(span.collectorFlags()).contains(SpanFlag.ATTRIBUTE_TRUNCATED);
    }

    @Test
    @DisplayName("rejects a missing trace id, a span that is its own parent and a negative duration")
    void given_brokenSpan_when_built_then_invalidSpan() {
        assertViolation(ViolationCode.INVALID_SPAN, () -> aSpan().traceId(null).build());
        assertViolation(
                ViolationCode.INVALID_SPAN,
                () -> aSpan().spanId("0e0e0e0e0e0e0e01")
                        .parent(DomainIds.spanId("0e0e0e0e0e0e0e01"))
                        .build());
        assertViolation(
                ViolationCode.INVALID_SPAN,
                () -> aSpan().duration(Duration.ofNanos(-1)).build());
    }

    @Test
    @DisplayName("rejects broken span parts: negative dropped counts, events or links without ids, flags above a byte")
    void given_brokenSpanParts_when_constructed_then_invalidSpan() {
        assertViolation(ViolationCode.INVALID_SPAN, () -> new DroppedCounts(-1, 0, 0));
        assertViolation(ViolationCode.INVALID_SPAN, () -> new SpanEvent(null, 0, AttributesBuilder.empty(), 0));
        assertViolation(ViolationCode.INVALID_SPAN, () -> new SpanEvent("exception", 0, AttributesBuilder.empty(), -1));
        assertViolation(
                ViolationCode.INVALID_SPAN,
                () -> new SpanLink(null, DomainIds.spanId("0e0e0e0e0e0e0e01"), null, AttributesBuilder.empty()));
        assertViolation(ViolationCode.INVALID_SPAN, () -> new TraceFlags(256));
        assertViolation(ViolationCode.INVALID_SPAN, () -> new TraceFlags(-1));
    }

    @Test
    @DisplayName("only status ERROR is an error")
    void given_statuses_when_isError_then_onlyErrorIsTrue() {
        assertThat(new SpanStatus(StatusCode.ERROR, "failed").isError()).isTrue();
        assertThat(new SpanStatus(StatusCode.OK, null).isError()).isFalse();
        assertThat(SpanStatus.UNSET.isError()).isFalse();
    }

    @Test
    @DisplayName("""
            rejects more broken span parts: an event or a link without attributes, a link without a span id,
            and negative dropped events or links""")
    void given_moreBrokenSpanParts_when_constructed_then_invalidSpan() {
        TraceId trace = DomainIds.traceId("0af7651916cd43dd8448eb211c80319c");
        SpanId span = DomainIds.spanId("0e0e0e0e0e0e0e01");

        assertViolation(ViolationCode.INVALID_SPAN, () -> new SpanEvent("exception", 0, null, 0));
        assertViolation(ViolationCode.INVALID_SPAN, () -> new SpanLink(trace, null, null, AttributesBuilder.empty()));
        assertViolation(ViolationCode.INVALID_SPAN, () -> new SpanLink(trace, span, null, null));
        assertViolation(ViolationCode.INVALID_SPAN, () -> new DroppedCounts(0, -1, 0));
        assertViolation(ViolationCode.INVALID_SPAN, () -> new DroppedCounts(0, 0, -1));
    }

    @Test
    @DisplayName("""
            a span keeps immutable copies of its events, links and collector flags:
            changing the lists and sets it was built from does not change it""")
    void given_mutableCollections_when_spanBuilt_then_immutableCopies() {
        Span template = aSpan().build();
        List<SpanEvent> events = new ArrayList<>(List.of(new SpanEvent("retry", 1, AttributesBuilder.empty(), 0)));
        Set<SpanFlag> flags = EnumSet.of(SpanFlag.NOT_ENDED);

        Span span = new Span(
                template.traceId(),
                template.spanId(),
                null,
                template.name(),
                template.kind(),
                template.startEpochNanos(),
                template.durationNanos(),
                template.status(),
                template.resourceId(),
                template.serviceName(),
                template.instrumentationScopeId(),
                template.traceFlags(),
                null,
                template.attributes(),
                events,
                List.of(),
                DroppedCounts.NONE,
                flags);
        events.clear();
        flags.add(SpanFlag.PARENT_REMOTE);

        assertThat(span.events()).hasSize(1);
        assertThat(span.collectorFlags()).containsExactly(SpanFlag.NOT_ENDED);
        assertThatThrownBy(() -> span.events().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> span.collectorFlags().clear()).isInstanceOf(UnsupportedOperationException.class);
    }
}
