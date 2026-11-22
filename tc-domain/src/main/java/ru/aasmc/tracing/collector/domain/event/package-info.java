/**
 * Internal, in-process domain events (05 §5). Published with Spring's {@code ApplicationEventPublisher} by the
 * services and consumed after commit; never Kafka topics (ADR-01).
 */
package ru.aasmc.tracing.collector.domain.event;
