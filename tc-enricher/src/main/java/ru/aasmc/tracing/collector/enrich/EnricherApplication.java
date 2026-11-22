package ru.aasmc.tracing.collector.enrich;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Entry point of {@code tc-enricher} (Consumes client enrichment topics defined at runtime (03 §3)). */
@SpringBootApplication
public class EnricherApplication {

    public static void main(String[] args) {
        SpringApplication.run(EnricherApplication.class, args);
    }
}
