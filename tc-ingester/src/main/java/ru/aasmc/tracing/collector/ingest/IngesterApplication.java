package ru.aasmc.tracing.collector.ingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Entry point of {@code tc-ingester} (Consumes otlp-traces and writes traces to PostgreSQL (03 §2)). */
@SpringBootApplication
public class IngesterApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngesterApplication.class, args);
    }
}
