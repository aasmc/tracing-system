package ru.aasmc.tracing.collector.maintenance;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Entry point of {@code tc-maintenance} (Partitions, retention, correlation resolver, digest compaction (03 §6)). */
@SpringBootApplication
public class MaintenanceApplication {

    public static void main(String[] args) {
        SpringApplication.run(MaintenanceApplication.class, args);
    }
}
