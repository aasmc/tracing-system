package ru.aasmc.tracing.collector.query;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Entry point of {@code tc-query-api} (Read-only search and trace API (03 §4)). */
@SpringBootApplication
public class QueryApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(QueryApiApplication.class, args);
    }
}
