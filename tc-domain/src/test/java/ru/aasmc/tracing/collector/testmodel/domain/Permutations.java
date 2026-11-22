package ru.aasmc.tracing.collector.testmodel.domain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/** All orders and random splits of small lists, for checking that results do not depend on arrival order. */
public final class Permutations {

    private Permutations() {}

    /** All orders of {@code items}. */
    public static <T> List<List<T>> allOrders(List<T> items) {
        List<List<T>> result = new ArrayList<>();
        permute(new ArrayList<>(items), 0, result);
        return result;
    }

    /** {@code items}, in order, cut into consecutive non-empty chunks at points chosen by {@code seed}. */
    public static <T> List<List<T>> randomSplits(List<T> items, long seed) {
        Random random = new Random(seed);
        List<List<T>> chunks = new ArrayList<>();
        List<T> current = new ArrayList<>();
        for (T item : items) {
            if (!current.isEmpty() && random.nextBoolean()) {
                chunks.add(current);
                current = new ArrayList<>();
            }
            current.add(item);
        }
        if (!current.isEmpty()) {
            chunks.add(current);
        }
        return chunks;
    }

    private static <T> void permute(List<T> items, int k, List<List<T>> result) {
        if (k == items.size()) {
            result.add(List.copyOf(items));
            return;
        }
        for (int i = k; i < items.size(); i++) {
            Collections.swap(items, k, i);
            permute(items, k + 1, result);
            Collections.swap(items, k, i);
        }
    }
}
