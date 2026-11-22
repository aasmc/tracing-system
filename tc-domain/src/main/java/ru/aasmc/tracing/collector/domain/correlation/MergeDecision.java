package ru.aasmc.tracing.collector.domain.correlation;

/** Whether two operations that turned out to share a value are merged. */
public sealed interface MergeDecision {

    /** {@code absorbed} is merged into {@code survivor}. */
    record Merge(BusinessOperation survivor, BusinessOperation absorbed) implements MergeDecision {}

    /**
     * Not merged: the union would exceed the size limit. Usually a value shared by unrelated traces, such as a
     * constant placeholder, which would otherwise glue everything into one giant operation.
     */
    record SizeLimitExceeded(BusinessOperation survivor, BusinessOperation other, int combinedTraceCount)
            implements MergeDecision {}
}
