package ru.aasmc.tracing.testinfra;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Test-side Kafka client: sends raw bytes with headers, reads a topic from the beginning, and inspects
 * consumer groups and committed offsets. It never creates topics; use {@link KafkaTopicsFixture}.
 */
public final class KafkaProbe {

    private static final long TIMEOUT_SECONDS = 30;

    private final String bootstrapServers;

    private KafkaProbe(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public static KafkaProbe tcs() {
        return new KafkaProbe(TcContainers.kafka().getBootstrapServers());
    }

    public static KafkaProbe foreign() {
        return new KafkaProbe(TcContainers.foreignKafka().getBootstrapServers());
    }

    public static KafkaProbe of(String bootstrapServers) {
        return new KafkaProbe(bootstrapServers);
    }

    public String bootstrapServers() {
        return bootstrapServers;
    }

    /** Sends one record and waits for the acknowledgement. {@code key} may be {@code null}. */
    public RecordMetadata send(String topic, byte[] key, byte[] value, List<Header> headers) {
        try (KafkaProducer<byte[], byte[]> producer = producer(Map.of())) {
            return producer.send(new ProducerRecord<>(topic, null, key, value, headers))
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("send to " + topic + " failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** A producer with test defaults ({@code acks=all}); {@code overrides} win. Caller closes it. */
    public KafkaProducer<byte[], byte[]> producer(Map<String, Object> overrides) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.putAll(overrides);
        return new KafkaProducer<>(props);
    }

    /**
     * Reads {@code topic} from the beginning with a throw-away group until {@code expected} records were read or
     * {@code timeout} elapsed, and returns what was read. The group does not commit offsets.
     */
    public List<ConsumerRecord<byte[], byte[]>> readFromBeginning(String topic, int expected, Duration timeout) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG,
                "tc-test-probe-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class);
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            while (records.size() < expected && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
        }
        return records;
    }

    /** Ids of all consumer groups known to the cluster. */
    public Set<String> consumerGroups() {
        try (Admin admin = admin()) {
            return admin
                    .listGroups(ListGroupsOptions.forConsumerGroups())
                    .all()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .stream()
                    .map(GroupListing::groupId)
                    .collect(Collectors.toSet());
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("listGroups failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** Committed offsets of {@code groupId}; empty if the group has committed nothing. */
    public Map<TopicPartition, Long> committedOffsets(String groupId) {
        try (Admin admin = admin()) {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return offsets.entrySet().stream()
                    .filter(e -> e.getValue() != null)
                    .collect(Collectors.toMap(
                            Map.Entry::getKey, e -> e.getValue().offset()));
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("listConsumerGroupOffsets failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private Admin admin() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }
}
