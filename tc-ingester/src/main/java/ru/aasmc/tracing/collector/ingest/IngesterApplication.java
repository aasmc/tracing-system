package ru.aasmc.tracing.collector.ingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Entry point of {@code tc-ingester} (consumes otlp-traces and writes traces to PostgreSQL). */
@SpringBootApplication
public class IngesterApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngesterApplication.class, args);
    }
}
