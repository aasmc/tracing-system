package ru.aasmc.tracing.testinfra;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Connects the application context to the JVM-wide containers of {@link TcContainers} (09 §0.3 T2, T3, T7).
 *
 * <p>The PostgreSQL and Kafka service connections are registered only when the module has the matching Spring Boot
 * module on its classpath ({@code spring-boot-jdbc}, {@code spring-boot-kafka}); otherwise {@code @ServiceConnection}
 * would fail because no connection-details factory exists (e.g. {@code tc-query-api} has no Kafka).
 *
 * <p>The container beans have no destroy method: the containers outlive any single context and are shared by all
 * cached contexts of the JVM.
 */
@TestConfiguration(proxyBeanMethods = false)
@Import({TcContainersConfiguration.Postgres.class, TcContainersConfiguration.Kafka.class})
public class TcContainersConfiguration {

    /** Bootstrap servers of the "foreign" client cluster used by enrichment-source tests. */
    public static final String FOREIGN_KAFKA_PROPERTY = "tc.test.foreign-kafka.bootstrap-servers";

    /**
     * Set to {@code false} by modules whose context must log in as a specific role instead of the container
     * superuser ({@code tc-schema-migrator} logs in as {@code tc_migrator}); they register
     * {@code spring.datasource.*} themselves. The container is still started and migrated.
     */
    public static final String POSTGRES_SERVICE_CONNECTION_PROPERTY = "tc.test.postgres.service-connection";

    @TestConfiguration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.boot.jdbc.autoconfigure.JdbcConnectionDetails")
    @ConditionalOnProperty(name = POSTGRES_SERVICE_CONNECTION_PROPERTY, havingValue = "true", matchIfMissing = true)
    static class Postgres {

        @Bean(destroyMethod = "")
        @ServiceConnection
        PostgreSQLContainer tcPostgres() {
            return TcContainers.postgres();
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails")
    static class Kafka {

        @Bean(destroyMethod = "")
        @ServiceConnection
        KafkaContainer tcKafka() {
            return TcContainers.kafka();
        }

        @Bean
        DynamicPropertyRegistrar foreignKafkaProperties() {
            return registry -> registry.add(
                    FOREIGN_KAFKA_PROPERTY, () -> TcContainers.foreignKafka().getBootstrapServers());
        }
    }
}
