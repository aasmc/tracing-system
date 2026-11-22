package ru.aasmc.tracing.testinfra;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * Creates topics with {@link Admin}, exactly as platform admins do. TCS itself never creates topics.
 */
public final class KafkaTopicsFixture {

    public static final String OTLP_TRACES = "otlp-traces";
    public static final String OTLP_TRACES_DLQ = "otlp-traces.dlq";
    public static final String ENRICHMENT_DLQ = "tc-enrichment.dlq";
    /** Fewer partitions than production (24) keep tests fast; the key = traceId routing is the same. */
    public static final int TEST_PARTITIONS = 3;

    private static final long TIMEOUT_SECONDS = 30;

    private final String bootstrapServers;

    private KafkaTopicsFixture(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /** The TCS cluster. */
    public static KafkaTopicsFixture tcs() {
        return new KafkaTopicsFixture(TcContainers.kafka().getBootstrapServers());
    }

    /** The foreign (client) cluster. */
    public static KafkaTopicsFixture foreign() {
        return new KafkaTopicsFixture(TcContainers.foreignKafka().getBootstrapServers());
    }

    /** {@code otlp-traces}, {@code otlp-traces.dlq}, {@code tc-enrichment.dlq}; already existing topics are kept. */
    public KafkaTopicsFixture createStandardTopics() {
        return createTopics(List.of(
                new NewTopic(OTLP_TRACES, TEST_PARTITIONS, (short) 1),
                new NewTopic(OTLP_TRACES_DLQ, TEST_PARTITIONS, (short) 1),
                new NewTopic(ENRICHMENT_DLQ, TEST_PARTITIONS, (short) 1)));
    }

    public KafkaTopicsFixture createTopic(String name, int partitions) {
        return createTopics(List.of(new NewTopic(name, Optional.of(partitions), Optional.empty())));
    }

    public KafkaTopicsFixture createTopic(String name, int partitions, Map<String, String> configs) {
        return createTopics(List.of(new NewTopic(name, partitions, (short) 1).configs(configs)));
    }

    public Set<String> listTopics() {
        try (Admin admin = admin()) {
            return admin.listTopics().names().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("listTopics failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private KafkaTopicsFixture createTopics(Collection<NewTopic> topics) {
        try (Admin admin = admin()) {
            for (var entry : admin.createTopics(topics).values().entrySet()) {
                try {
                    entry.getValue().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof TopicExistsException)) {
                        throw new IllegalStateException("Cannot create topic " + entry.getKey(), e);
                    }
                }
            }
        } catch (TimeoutException e) {
            throw new IllegalStateException("createTopics timed out", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        return this;
    }

    private Admin admin() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }
}
