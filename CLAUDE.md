# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Layout

The root is a git repo (branch `main`); `opentelemetry-demo/` and `otlp-kafka-export-tracing-starter/` are git-ignored. It holds the Trace Collector System (TCS) reactor plus two independent Maven projects (Spring Boot 4.1.x, Java 25, Jackson 3 under `tools.jackson.*`):

- `pom.xml` + `tc-*/` — the TCS reactor (`ru.aasmc:trace-collector`), the backend that consumes the `otlp-traces` topic. Design: `agent/01…08`; implementation plan and progress: `agent/09-implementation-plan.md` (Phase 0 and Phase 1 (domain) done, Phase 2 (data) in progress: slices 2d-1 (schema, grants), 2d-2a (dictionaries) and 2d-2b (config revision, retention tiers, snapshot) done; results in §1.6, §2.6, §3.6). The two projects below are **not** reactor modules.

- `otlp-kafka-export-tracing-starter/` — a Spring Boot starter library that replaces the standard OTLP HTTP/gRPC trace export with export of spans to Kafka as JSON.
- `opentelemetry-demo/` — a minimal web app that consumes the starter (as `ru.aasmc:otlp-kafka-export-tracing-starter:0.0.1-SNAPSHOT` from the local Maven repo), plus `docker-compose.yml` for a single-node KRaft Kafka on `localhost:9092`.
- `agent/` — the English TCS design (01…08) and the TDD implementation plan (09), which supersede the earlier Russian-language reports (formerly `opentelemetry-demo/reports/`).

## Commands

```bash
# TCS reactor (root). Needs Docker: every *IT starts Testcontainers (postgres:18.7, apache/kafka:4.2.2)
mvn verify                                             # surefire *Test (tc-domain), failsafe *IT, Spotless check, JaCoCo
mvn spotless:apply                                     # format (Palantir Java Format); verify fails on unformatted code
mvn verify -pl tc-test-infra -Dit.test=WireMockIT      # single integration test class
```

TCS testing rules (09 §0.3): `tc-domain` tests are plain JUnit (`*Test`, no Spring); every other test is a full-context `@SpringBootTest` named `*IT`, using the module's variant of `@TcIntegrationTest` from `tc-test-infra` (`ru.aasmc.tracing.testinfra`); no `@MockitoBean`; PostgreSQL/Kafka only via Testcontainers, external HTTP only via WireMock. Compiler runs with `-Werror`.

Code rules: write only what a current test or caller needs (KISS, DRY, YAGNI). Javadoc must be self-contained — never cite ADRs, design-doc sections, use cases or plan items; keep test ids out of `@DisplayName`.

Only `opentelemetry-demo` has a Maven wrapper; the starter is built with a system `mvn` (or `../opentelemetry-demo/mvnw -f pom.xml`).

```bash
# Starter: build, test, and install into ~/.m2 (required before the demo sees starter changes)
cd otlp-kafka-export-tracing-starter && mvn install
mvn test -Dtest=KafkaSpanExporterTests                 # single test class
mvn test -Dtest=KafkaSpanExporterTests#methodName      # single test method

# Demo
cd opentelemetry-demo
docker compose up -d                                   # local Kafka
./mvnw spring-boot:run                                 # GET http://localhost:8080/hello produces a trace
./mvnw test
```

Starter tests do not need a running broker (they point at an unreachable bootstrap server / mock the template).

## Starter architecture

All code is in `ru.aasmc.tracing.starter`. The flow spans several files:

1. **`OtlpKafkaTracingEnvironmentListener`** (registered via `META-INF/spring.factories`, runs at `ApplicationEnvironmentPreparedEvent`) — when `management.otlp.tracing.export.enabled` is true (default), adds a lowest-precedence property source that sets `management.tracing.export.otlp.enabled=false` (disabling Boot's built-in OTLP exporter) and copies `management.otlp.tracing.sampling.probability` into `management.tracing.sampling.probability`.
2. **`OtlpKafkaTracingAutoConfiguration`** (registered via `AutoConfiguration.imports`) — creates a *dedicated* producer stack, separate from any application `KafkaTemplate`: `otlpKafkaProducerFactory` → `otlpKafkaTemplate` → `KafkaSpanExporter` → `BatchSpanProcessor`. Beans are gated on the same `export.enabled` property and are overridable by bean name / type. Boot's OpenTelemetry auto-config picks up the `BatchSpanProcessor` bean.
3. **`OtlpKafkaTracingProperties`** — prefix `management.otlp`. Note the custom namespace: `management.otlp.kafka.*` is a full nested `KafkaProperties` (independent of `spring.kafka.*`), defaulting to idempotent producer, `acks=all`, max retries; `management.otlp.topic` (default `otlp-traces`); `management.otlp.tracing.export.*` tunes the batch processor.
4. **`KafkaSpanExporter`** — groups each batch by traceId and sends one Kafka record per trace (key = traceId), so a trace may arrive as multiple partial messages across batches.
5. **`TraceMessageMapper` / `TraceMessage`** — the wire contract (`schemaVersion = 1`). Epoch nanos are serialized as strings; attributes are `{type, value}` pairs. Changing these records changes the contract the backend designs in `reports/` depend on — bump `SCHEMA_VERSION` for breaking changes.
6. **`@TraceAttribute(key, value)` + `TraceAttributeAspect`** — on a Spring bean method, evaluates a SpEL expression against the non-null return value (root object, also `#result`) and sets it as an attribute on `Span.current()`. Evaluation failures are logged, never thrown.
