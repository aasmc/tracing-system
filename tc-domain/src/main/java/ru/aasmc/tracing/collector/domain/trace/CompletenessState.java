package ru.aasmc.tracing.collector.domain.trace;

/** Derived, never stored state of a trace in the full tier (05 §4.5). */
public enum CompletenessState {
    /** Fragments may still arrive: {@code lastIngestedAt + settleAfter >= now}. */
    RECEIVING,
    /** No fragment for longer than {@code settleAfter}. */
    SETTLED
}
