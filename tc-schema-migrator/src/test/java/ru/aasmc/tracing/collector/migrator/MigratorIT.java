package ru.aasmc.tracing.collector.migrator;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistrar;
import ru.aasmc.tracing.testinfra.LiquibaseMigrations;
import ru.aasmc.tracing.testinfra.TcContainers;
import ru.aasmc.tracing.testinfra.TcContainersConfiguration;
import ru.aasmc.tracing.testinfra.TcIntegrationTest;

/**
 * The module variant of {@link TcIntegrationTest} (09 §0.3 T6). The migrator logs in as {@code tc_migrator}, like in
 * production, so the PostgreSQL service connection (container superuser) is switched off and the datasource is
 * registered here; everything else comes from the module's real {@code application.yaml}.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@TcIntegrationTest(
        properties = {
            TcContainersConfiguration.POSTGRES_SERVICE_CONNECTION_PROPERTY + "=false",
            // spike 0-11: the real master changelog plus throw-away convention checks (test resources)
            "spring.liquibase.change-log=classpath:db/spike/spike-master.yaml"
        })
@Import(MigratorIT.MigratorDataSource.class)
public @interface MigratorIT {

    @TestConfiguration(proxyBeanMethods = false)
    class MigratorDataSource {

        @Bean
        DynamicPropertyRegistrar migratorDataSourceProperties() {
            return registry -> {
                registry.add(
                        "spring.datasource.url", () -> TcContainers.postgres().getJdbcUrl());
                registry.add("spring.datasource.username", () -> LiquibaseMigrations.MIGRATOR_USER);
                registry.add(
                        "spring.datasource.password",
                        () -> LiquibaseMigrations.passwordOf(LiquibaseMigrations.MIGRATOR_USER));
            };
        }
    }
}
