package ru.aasmc.tracing.testinfra.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import ru.aasmc.tracing.testinfra.KafkaProbe;
import ru.aasmc.tracing.testinfra.KafkaTopicsFixture;
import ru.aasmc.tracing.testinfra.TcContainers;
import ru.aasmc.tracing.testinfra.TcContainersConfiguration;
import ru.aasmc.tracing.testinfra.TestIds;

@InfraIT
class KafkaContainersIT {

    @Autowired
    KafkaAdmin kafkaAdmin;

    @Value("${" + TcContainersConfiguration.FOREIGN_KAFKA_PROPERTY + "}")
    String foreignBootstrapServers;

    @Test
    @DisplayName("0-T2 both clusters are reachable and distinct; the context points at the TCS cluster")
    void given_twoClusters_when_describing_then_reachableAndDistinct() throws Exception {
        String tcsClusterId = clusterId(TcContainers.kafka().getBootstrapServers());
        String foreignClusterId = clusterId(TcContainers.foreignKafka().getBootstrapServers());

        assertThat(tcsClusterId).isNotBlank().isNotEqualTo(foreignClusterId);
        assertThat(kafkaAdmin.getConfigurationProperties().get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG))
                .asString()
                .contains(TcContainers.kafka().getBootstrapServers().replace("PLAINTEXT://", ""));
        assertThat(foreignBootstrapServers)
                .isEqualTo(TcContainers.foreignKafka().getBootstrapServers());
    }

    @Test
    @DisplayName("0-T2 topic auto-creation is disabled: producing to an unknown topic fails and creates nothing")
    void given_autoCreateDisabled_when_producingToUnknownTopic_then_failsAndNoTopic() {
        String unknown = TestIds.topic("unknown");
        KafkaProbe probe = KafkaProbe.tcs();

        try (KafkaProducer<byte[], byte[]> producer =
                probe.producer(Map.of(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3_000))) {
            assertThatThrownBy(() -> producer.send(new ProducerRecord<>(unknown, new byte[] {1}))
                            .get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TimeoutException.class)
                    .hasMessageContaining("not present in metadata");
        }
        assertThat(KafkaTopicsFixture.tcs().listTopics()).doesNotContain(unknown);
    }

    @Test
    @DisplayName("0-T2 KafkaTopicsFixture creates the standard topics and per-test topics; KafkaProbe round-trips")
    void given_fixture_when_creatingTopics_then_topicsExistAndRecordsRoundTrip() {
        KafkaTopicsFixture.tcs().createStandardTopics().createStandardTopics(); // idempotent
        String enrichmentTopic = TestIds.topic("sms-gateway.delivery-reports");
        KafkaTopicsFixture.foreign().createTopic(enrichmentTopic, 2);

        assertThat(KafkaTopicsFixture.tcs().listTopics())
                .contains(
                        KafkaTopicsFixture.OTLP_TRACES,
                        KafkaTopicsFixture.OTLP_TRACES_DLQ,
                        KafkaTopicsFixture.ENRICHMENT_DLQ)
                .doesNotContain(enrichmentTopic);
        assertThat(KafkaTopicsFixture.foreign().listTopics()).contains(enrichmentTopic);

        String traceparent = "00-" + TestIds.traceId() + "-" + TestIds.spanId() + "-01";
        KafkaProbe.foreign()
                .send(
                        enrichmentTopic,
                        "k".getBytes(StandardCharsets.UTF_8),
                        "{\"status\":\"DELIVERED\"}".getBytes(StandardCharsets.UTF_8),
                        List.of(new RecordHeader("traceparent", traceparent.getBytes(StandardCharsets.UTF_8))));

        List<ConsumerRecord<byte[], byte[]>> records =
                KafkaProbe.foreign().readFromBeginning(enrichmentTopic, 1, Duration.ofSeconds(30));
        assertThat(records).hasSize(1);
        assertThat(new String(
                        records.getFirst().headers().lastHeader("traceparent").value(), StandardCharsets.UTF_8))
                .isEqualTo(traceparent);
    }

    private static String clusterId(String bootstrapServers) throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            return admin.describeCluster().clusterId().get(30, TimeUnit.SECONDS);
        }
    }
}
