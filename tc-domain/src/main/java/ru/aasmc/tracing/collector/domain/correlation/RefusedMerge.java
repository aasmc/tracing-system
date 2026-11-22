package ru.aasmc.tracing.collector.domain.correlation;

/**
 * A merge that was not made because the merged operation would hold more traces than the size limit allows. Usually a
 * value shared by unrelated traces, such as a constant placeholder, which would otherwise glue everything into one giant
 * operation.
 *
 * @param survivor the operation that would have absorbed {@code other}
 * @param other the operation that would have been merged into {@code survivor}
 * @param combinedTraceCount the traces the merged operation would have held
 */
public record RefusedMerge(BusinessOperation survivor, BusinessOperation other, int combinedTraceCount) {}
