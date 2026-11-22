package ru.aasmc.tracing.collector.persistence.dictionary;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.attribute.AttributeType;
import ru.aasmc.tracing.collector.persistence.PersistenceIT;
import ru.aasmc.tracing.testinfra.JdbcProbe;
import ru.aasmc.tracing.testinfra.TestIds;

@PersistenceIT
class DictionaryRepositoryIT {

    private static final int CONCURRENT_CALLERS = 8;

    private final JdbcProbe db = JdbcProbe.superuser();

    @Autowired
    DictionaryRepository dictionary;

    @Nested
    @DisplayName("Service names")
    class ServiceNames {

        @Test
        @DisplayName("""
                Given new and already stored service names,
                when their ids are asked for twice,
                then each name gets one id, the same both times, and is stored once""")
        void given_newAndStoredNames_when_resolvedTwice_then_sameIdsAndOneRowEach() {
            String stored = TestIds.name("svc");
            Integer storedId = dictionary.serviceIds(List.of(stored)).get(stored);
            String fresh = TestIds.name("svc");

            Map<String, Integer> first = dictionary.serviceIds(List.of(stored, fresh, fresh));
            Map<String, Integer> second = dictionary.serviceIds(List.of(fresh, stored));

            assertThat(first).containsOnlyKeys(stored, fresh).containsEntry(stored, storedId);
            assertThat(second).isEqualTo(first);
            assertThat(rowsOf("tc.service", "name", stored, fresh)).isEqualTo(2);
        }

        @Test
        @DisplayName("""
                Given no names, when ids are asked for, then the result is empty""")
        void given_noNames_when_resolved_then_empty() {
            assertThat(dictionary.serviceIds(List.of())).isEmpty();
        }

        @Test
        @DisplayName("""
                Given 8 callers that insert the same new names at the same moment, in different orders,
                when they run concurrently,
                then all of them get the same ids and each name is stored once""")
        void given_concurrentCallers_when_sameNewNamesInserted_then_oneIdPerName() throws Exception {
            List<String> names =
                    IntStream.range(0, 20).mapToObj(i -> TestIds.name("svc")).toList();

            List<Map<String, Integer>> results =
                    concurrently(caller -> dictionary.serviceIds(caller % 2 == 0 ? names : names.reversed()));

            assertThat(results).allSatisfy(ids -> assertThat(ids).isEqualTo(results.getFirst()));
            assertThat(results.getFirst()).containsOnlyKeys(names);
            assertThat(rowsOf("tc.service", "name", names.toArray(String[]::new)))
                    .isEqualTo(names.size());
        }
    }

    @Nested
    @DisplayName("Span names: at most a configured number of names per service")
    class SpanNames {

        @Test
        @DisplayName("""
                Given a service that already has 2 of at most 3 names,
                when 2 new names are asked for,
                then the first in sorted order gets an id, the other is left out as one too many,
                and the stored names still resolve""")
        void given_serviceNearLimit_when_newNamesResolved_then_namesBeyondLimitLeftOut() {
            int serviceId = newService();
            Map<String, Integer> stored = dictionary.spanNameIds(serviceId, List.of("GET /a", "GET /b"), 3);

            Map<String, Integer> ids = dictionary.spanNameIds(serviceId, List.of("GET /d", "GET /c", "GET /a"), 3);

            assertThat(ids).containsOnlyKeys("GET /a", "GET /c").containsEntry("GET /a", stored.get("GET /a"));
            assertThat(dictionary.spanNameIds(serviceId, List.of("GET /d"), 3)).isEmpty();
            assertThat(dictionary.spanNameIds(serviceId, List.of("GET /b"), 3))
                    .isEqualTo(Map.of("GET /b", stored.get("GET /b")));
        }

        @Test
        @DisplayName("""
                Given one service at its limit,
                when another service asks for the same names,
                then it gets ids of its own: the limit and the names are per service""")
        void given_serviceAtLimit_when_otherServiceResolvesSameNames_then_ownIds() {
            int full = newService();
            int other = newService();
            Map<String, Integer> fullIds = dictionary.spanNameIds(full, List.of("GET /a"), 1);

            Map<String, Integer> otherIds = dictionary.spanNameIds(other, List.of("GET /a"), 1);

            assertThat(otherIds).containsOnlyKeys("GET /a");
            assertThat(otherIds.get("GET /a")).isNotEqualTo(fullIds.get("GET /a"));
        }

        @Test
        @DisplayName("""
                Given 8 callers that insert the same new span names of one service at the same moment,
                when they run concurrently,
                then all of them get the same ids""")
        void given_concurrentCallers_when_sameSpanNamesInserted_then_oneIdPerName() throws Exception {
            int serviceId = newService();
            List<String> names =
                    IntStream.range(0, 20).mapToObj(i -> "GET /" + i).toList();

            List<Map<String, Integer>> results =
                    concurrently(caller -> dictionary.spanNameIds(serviceId, names, 1_000));

            assertThat(results).allSatisfy(ids -> assertThat(ids).isEqualTo(results.getFirst()));
            assertThat(results.getFirst()).containsOnlyKeys(names);
        }
    }

    @Nested
    @DisplayName("Attribute keys")
    class AttributeKeys {

        @Test
        @DisplayName("""
                Given new attribute keys,
                when their ids are asked for,
                then each key is stored once with the type it was first seen with""")
        void given_newKeys_when_resolved_then_storedWithFirstSeenType() {
            AttributeKey user = new AttributeKey(TestIds.name("user.id"));
            AttributeKey amount = new AttributeKey(TestIds.name("order.amount"));

            Map<AttributeKey, Integer> ids =
                    dictionary.attributeKeyIds(Map.of(user, AttributeType.STRING, amount, AttributeType.DOUBLE));

            assertThat(ids).containsOnlyKeys(user, amount);
            assertThat(dictionary.attributeKeyIds(Map.of(user, AttributeType.STRING)))
                    .isEqualTo(Map.of(user, ids.get(user)));
            assertThat(db.rows(
                            "SELECT key, value_type FROM tc.attribute_key WHERE key IN (?, ?) ORDER BY value_type",
                            user.value(),
                            amount.value()))
                    .containsExactly(
                            Map.of("key", amount.value(), "value_type", "DOUBLE"),
                            Map.of("key", user.value(), "value_type", "STRING"));
        }
    }

    @Test
    @DisplayName("""
            Given the ingester's database role,
            when every dictionary is resolved with it,
            then its privileges are enough""")
    void given_ingesterRole_when_dictionariesResolved_then_allowed() {
        DictionaryRepository asIngester = new DictionaryRepository(new NamedParameterJdbcTemplate(
                JdbcProbe.loggedInAs("tc_ingester").dataSource()));
        String service = TestIds.name("svc");
        AttributeKey key = new AttributeKey(TestIds.name("key"));

        int serviceId = asIngester.serviceIds(List.of(service)).get(service);

        assertThat(asIngester.spanNameIds(serviceId, List.of("GET /a"), 10)).containsOnlyKeys("GET /a");
        assertThat(asIngester.attributeKeyIds(Map.of(key, AttributeType.LONG))).containsOnlyKeys(key);
    }

    private int newService() {
        String name = TestIds.name("svc");
        return dictionary.serviceIds(List.of(name)).get(name);
    }

    private int rowsOf(String table, String column, String... values) {
        return db.scalar(
                Integer.class, "SELECT count(*) FROM " + table + " WHERE " + column + " = ANY (?)", (Object) values);
    }

    /** Runs {@code call} on {@value #CONCURRENT_CALLERS} threads that start together, and returns their results. */
    private static <T> List<T> concurrently(CallerTask<T> call) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_CALLERS)) {
            List<Future<T>> futures = IntStream.range(0, CONCURRENT_CALLERS)
                    .mapToObj(caller -> executor.submit(() -> {
                        start.await();
                        return call.run(caller);
                    }))
                    .toList();
            start.countDown();
            List<T> results = new ArrayList<>();
            for (Future<T> future : futures) {
                results.add(future.get());
            }
            return results;
        }
    }

    @FunctionalInterface
    private interface CallerTask<T> {
        T run(int caller);
    }
}
