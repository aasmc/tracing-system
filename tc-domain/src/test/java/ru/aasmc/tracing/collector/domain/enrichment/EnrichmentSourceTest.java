package ru.aasmc.tracing.collector.domain.enrichment;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.testmodel.domain.Transitions;

class EnrichmentSourceTest {

    private static final String NAME = "sms-gateway.delivery-reports";
    private static final String TOPIC = "sms.Delivery_Reports-1";

    @ParameterizedTest
    @CsvSource({
        "DRAFT,   DRAFT,   true",
        "DRAFT,   ACTIVE,  true",
        "DRAFT,   PAUSED,  false",
        "DRAFT,   DELETED, true",
        "ACTIVE,  DRAFT,   false",
        "ACTIVE,  ACTIVE,  true",
        "ACTIVE,  PAUSED,  true",
        "ACTIVE,  DELETED, false",
        "PAUSED,  DRAFT,   false",
        "PAUSED,  ACTIVE,  true",
        "PAUSED,  PAUSED,  true",
        "PAUSED,  DELETED, true",
        "DELETED, DRAFT,   false",
        "DELETED, ACTIVE,  false",
        "DELETED, PAUSED,  false",
        "DELETED, DELETED, true"
    })
    @DisplayName("a source is activated from draft, paused and resumed, and deleted only when not active")
    void given_statuses_when_transitionTo_then_allowedOrRejected(SourceStatus from, SourceStatus to, boolean allowed) {
        Transitions.assertTransition(from.transitionTo(to), from, to, allowed, ViolationCode.INVALID_SOURCE_TRANSITION);
    }

    @Test
    @DisplayName("moving a source changes only its status; an active source must be paused before it is deleted")
    void given_activeSource_when_moved_then_pausedOrRejected() {
        EnrichmentSource active = source(NAME, TOPIC, SourceStatus.ACTIVE);

        assertThat(active.moveTo(SourceStatus.PAUSED)).isEqualTo(source(NAME, TOPIC, SourceStatus.PAUSED));
        assertViolation(ViolationCode.INVALID_SOURCE_TRANSITION, () -> active.moveTo(SourceStatus.DELETED));
    }

    @ParameterizedTest
    @ValueSource(strings = {"ab", "Sms-gateway", "-sms", "sms gateway", "sms/gateway"})
    @DisplayName(
            "a name is at least 3 lower-case letters, digits, dots, underscores or dashes, starting with a letter or digit")
    void given_invalidName_when_constructed_then_invalidName(String name) {
        assertViolation(ViolationCode.INVALID_NAME, () -> source(name, TOPIC, SourceStatus.DRAFT));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", ".", "..", "sms reports", "sms/reports"})
    @DisplayName("a topic is a legal Kafka topic name")
    void given_invalidTopic_when_constructed_then_invalidTopic(String topic) {
        assertViolation(ViolationCode.INVALID_TOPIC, () -> source(NAME, topic, SourceStatus.DRAFT));
    }

    @Test
    @DisplayName("a topic may have up to 249 characters")
    void given_topicLengths_when_constructed_then_atMost249() {
        assertThat(source(NAME, "t".repeat(249), SourceStatus.DRAFT).topic()).hasSize(249);
        assertViolation(ViolationCode.INVALID_TOPIC, () -> source(NAME, "t".repeat(250), SourceStatus.DRAFT));
    }

    private static EnrichmentSource source(String name, String topic, SourceStatus status) {
        return new EnrichmentSource(new EnrichmentSourceId(1), name, new KafkaConnectionId(1), topic, status);
    }

    @Test
    @DisplayName("a source needs a name and a topic")
    void given_missingNameOrTopic_when_constructed_then_rejected() {
        assertViolation(ViolationCode.INVALID_NAME, () -> source(null, TOPIC, SourceStatus.DRAFT));
        assertViolation(ViolationCode.INVALID_TOPIC, () -> source(NAME, null, SourceStatus.DRAFT));
    }
}
