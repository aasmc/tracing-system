package ru.aasmc.tracing.collector.domain.enrichment;

import java.util.Map;
import java.util.Set;

/**
 * How to reach a client's Kafka cluster (05 §4.6, 03 §5.4 V14, V15).
 *
 * <p>Invariants: {@code name} matches {@link EnrichmentSource#NAME_PATTERN} ({@code INVALID_NAME});
 * {@code bootstrapServers} not blank; SASL protocols need a mechanism and a credentials reference, non-SASL protocols
 * have no mechanism ({@code INVALID_CONNECTION}); every client property is on {@link #ALLOWED_CLIENT_PROPERTIES} and
 * not on {@link #FORBIDDEN_CLIENT_PROPERTIES} ({@code FORBIDDEN_CLIENT_PROPERTY}); the property map is an immutable
 * copy. Whether PLAINTEXT is allowed is deployment policy ({@code tc.admin.allow-plaintext}) and is checked by the
 * control plane.
 *
 * @param id connection id
 * @param name unique name
 * @param bootstrapServers comma-separated host:port list
 * @param securityProtocol security protocol
 * @param saslMechanism SASL mechanism or {@code null}
 * @param credentials reference to SASL credentials or {@code null}
 * @param truststore reference to a truststore or {@code null}
 * @param clientProperties extra client properties from the allow-list
 */
public record KafkaConnection(
        KafkaConnectionId id,
        String name,
        String bootstrapServers,
        SecurityProtocol securityProtocol,
        SaslMechanism saslMechanism,
        SecretRef credentials,
        SecretRef truststore,
        Map<String, String> clientProperties) {

    /** Properties TCS owns; never accepted from a client (05 §4.6, V15). Keys ending in {@code .deserializer} too. */
    public static final Set<String> FORBIDDEN_CLIENT_PROPERTIES = Set.of(
            "bootstrap.servers",
            "security.protocol",
            "sasl.mechanism",
            "sasl.jaas.config",
            "group.id",
            "enable.auto.commit",
            "key.deserializer",
            "value.deserializer",
            "allow.auto.create.topics",
            "interceptor.classes",
            "ssl.keystore.password",
            "ssl.truststore.password");

    /** 03 §5.4 V15 allow-list. */
    public static final Set<String> ALLOWED_CLIENT_PROPERTIES = Set.of(
            "client.dns.lookup",
            "request.timeout.ms",
            "ssl.endpoint.identification.algorithm",
            "socket.connection.setup.timeout.ms",
            "socket.connection.setup.timeout.max.ms",
            "metadata.max.age.ms",
            "reconnect.backoff.ms",
            "reconnect.backoff.max.ms",
            "retry.backoff.ms",
            "connections.max.idle.ms",
            "receive.buffer.bytes",
            "send.buffer.bytes",
            "ssl.protocol",
            "ssl.enabled.protocols");

    /** {@code true} if a client may set this property (allowed and not forbidden). */
    public static boolean isAllowedClientProperty(String key) {
        throw new UnsupportedOperationException();
    }
}
