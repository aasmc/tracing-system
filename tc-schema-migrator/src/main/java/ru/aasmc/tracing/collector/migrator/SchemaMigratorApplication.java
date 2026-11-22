package ru.aasmc.tracing.collector.migrator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Entry point of {@code tc-schema-migrator} (Runs the Liquibase changelogs of schema tc as a Job (03 §7)). */
@SpringBootApplication
public class SchemaMigratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(SchemaMigratorApplication.class, args);
    }
}
