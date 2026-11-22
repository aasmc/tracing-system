package ru.aasmc.tracing.testinfra;

import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * JVM-wide container singletons, so all test contexts of a JVM share one PostgreSQL and two Kafka clusters. Each container is started on first access and lives until the JVM
 * exits (Ryuk removes it). PostgreSQL is returned only after the infrastructure-as-code bootstrap and the real
 * Liquibase changelogs have been applied ({@link LiquibaseMigrations}).
 *
 * <p>Images come from the system properties {@code tc.image.postgres} and {@code tc.image.kafka}, which the parent
 * POM sets for failsafe; the defaults below are used when a test is started from an IDE.
 */
public final class TcContainers {

    public static final String DATABASE = "tracing";
    public static final String DEFAULT_POSTGRES_IMAGE = "postgres:18.7";
    public static final String DEFAULT_KAFKA_IMAGE = "apache/kafka:4.2.2";
    /** Fixed (not random) KRaft cluster ids, so a reused container keeps its identity across runs. */
    public static final String TCS_CLUSTER_ID = "xJ82BNEoRzi9yBc7iE4nyg";

    public static final String FOREIGN_CLUSTER_ID = "JmaysuU3R8mCofFozI8eRA";

    private TcContainers() {}

    /** The TCS database, migrated to the current changelog. The JDBC user is the container superuser. */
    public static PostgreSQLContainer postgres() {
        return PostgresHolder.INSTANCE;
    }

    /** The TCS Kafka cluster ({@code otlp-traces}, DLQs). Topic auto-creation is disabled. */
    public static KafkaContainer kafka() {
        return KafkaHolder.INSTANCE;
    }

    /** A second, "foreign" cluster that plays a client's cluster for enrichment sources. */
    public static KafkaContainer foreignKafka() {
        return ForeignKafkaHolder.INSTANCE;
    }

    private static final class PostgresHolder {
        static final PostgreSQLContainer INSTANCE = startPostgres();
    }

    private static final class KafkaHolder {
        static final KafkaContainer INSTANCE = startKafka(TCS_CLUSTER_ID);
    }

    private static final class ForeignKafkaHolder {
        static final KafkaContainer INSTANCE = startKafka(FOREIGN_CLUSTER_ID);
    }

    @SuppressWarnings("resource") // closed by Ryuk at JVM exit
    private static PostgreSQLContainer startPostgres() {
        PostgreSQLContainer container = new PostgreSQLContainer(image("tc.image.postgres", DEFAULT_POSTGRES_IMAGE))
                .withDatabaseName(DATABASE)
                .withSharedMemorySize(1024L * 1024 * 1024)
                .withCommand("postgres", "-c", "max_connections=200")
                .withReuse(true);
        container.start();
        LiquibaseMigrations.applyOnce(container);
        return container;
    }

    @SuppressWarnings("resource") // closed by Ryuk at JVM exit
    private static KafkaContainer startKafka(String clusterId) {
        KafkaContainer container = new KafkaContainer(image("tc.image.kafka", DEFAULT_KAFKA_IMAGE))
                // the image's default CLUSTER_ID is a constant: without this both clusters report the same id
                .withEnv("CLUSTER_ID", clusterId)
                // topics are created by platform admins only (TCS never creates topics, so auto-creation is off)
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                .withReuse(true);
        container.start();
        return container;
    }

    private static DockerImageName image(String property, String defaultImage) {
        return DockerImageName.parse(System.getProperty(property, defaultImage));
    }
}
