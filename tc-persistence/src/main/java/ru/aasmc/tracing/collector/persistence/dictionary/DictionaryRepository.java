package ru.aasmc.tracing.collector.persistence.dictionary;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.attribute.AttributeType;

/**
 * The dictionaries that replace strings repeated in every span by small integer ids: service names, span names per
 * service, and attribute keys. An id never changes once assigned, so callers may cache it for good and ask only for
 * the values they have not cached yet.
 *
 * <p>Each method returns the id of every value asked for, inserting the values that are new. Callers on several
 * threads or pods may insert the same new value at the same time; they all get the one id that was stored. Each
 * statement commits on its own: no surrounding transaction is needed or wanted.
 *
 * <p>How the values are resolved, in three steps:
 *
 * <ol>
 *   <li>read the values that are already stored, so that identity values are not used up by inserts that would
 *       conflict;
 *   <li>insert the others with {@code ON CONFLICT DO NOTHING}. Each insert statement takes its values with
 *       {@code ORDER BY}, so concurrent callers insert, and lock, the same rows in the same order and cannot
 *       deadlock;
 *   <li>read once more the values the insert skipped: a concurrent caller may have inserted them after the first
 *       read, and they are visible now. A span name skipped because of the limit stays missing.
 * </ol>
 */
@Repository
public class DictionaryRepository {

    private static final String SELECT_SERVICES = "SELECT id, name FROM tc.service WHERE name = ANY (:names)";
    private static final String INSERT_SERVICES = """
            INSERT INTO tc.service (name)
            SELECT name FROM unnest(CAST(:names AS text[])) AS name ORDER BY name
            ON CONFLICT (name) DO NOTHING
            RETURNING id, name
            """;
    private static final String SELECT_SPAN_NAMES =
            "SELECT id, name FROM tc.span_name WHERE service_id = :serviceId AND name = ANY (:names)";
    private static final String INSERT_SPAN_NAMES = """
            INSERT INTO tc.span_name (service_id, name)
            SELECT :serviceId, name FROM unnest(CAST(:names AS text[])) AS name
            ORDER BY name
            LIMIT greatest(:maxNames - (SELECT count(*) FROM tc.span_name WHERE service_id = :serviceId), 0)
            ON CONFLICT (service_id, name) DO NOTHING
            RETURNING id, name
            """;
    private static final String SELECT_ATTRIBUTE_KEYS =
            "SELECT id, key AS name FROM tc.attribute_key WHERE key = ANY (:names)";
    private static final String INSERT_ATTRIBUTE_KEYS = """
            INSERT INTO tc.attribute_key (key, value_type)
            SELECT key, value_type
            FROM unnest(CAST(:names AS text[]), CAST(:types AS text[])) AS k(key, value_type)
            ORDER BY key
            ON CONFLICT (key) DO NOTHING
            RETURNING id, key AS name
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public DictionaryRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** The id of each service name. */
    public Map<String, Integer> serviceIds(Collection<String> names) {
        return resolve(
                names,
                missing -> idsByName(SELECT_SERVICES, Map.of("names", array(missing))),
                missing -> idsByName(INSERT_SERVICES, Map.of("names", array(missing))));
    }

    /**
     * The id of each span name of one service, as long as the service has at most {@code maxNamesPerService} names.
     * A name that would exceed the limit gets no id and is left out of the result: a service that puts e.g. a URL
     * into its span names would otherwise fill the dictionary without bound. Names that already have an id keep
     * resolving. The limit is checked when names are inserted, so callers inserting at the same moment may exceed it
     * by the few names they insert together.
     */
    public Map<String, Integer> spanNameIds(int serviceId, Collection<String> names, int maxNamesPerService) {
        return resolve(
                names,
                missing -> idsByName(SELECT_SPAN_NAMES, Map.of("serviceId", serviceId, "names", array(missing))),
                missing -> idsByName(
                        INSERT_SPAN_NAMES,
                        Map.of("serviceId", serviceId, "names", array(missing), "maxNames", maxNamesPerService)));
    }

    /** The id of each attribute key; a new key is stored with the type it was first seen with. */
    public Map<AttributeKey, Integer> attributeKeyIds(Map<AttributeKey, AttributeType> typeByKey) {
        Map<String, AttributeType> typeByName = new HashMap<>();
        typeByKey.forEach((key, type) -> typeByName.put(key.value(), type));
        Map<String, Integer> idByName = resolve(
                typeByName.keySet(),
                missing -> idsByName(SELECT_ATTRIBUTE_KEYS, Map.of("names", array(missing))),
                missing -> idsByName(
                        INSERT_ATTRIBUTE_KEYS,
                        Map.of(
                                "names",
                                array(missing),
                                "types",
                                missing.stream()
                                        .map(name -> typeByName.get(name).name())
                                        .toArray(String[]::new))));
        Map<AttributeKey, Integer> idByKey = new HashMap<>();
        idByName.forEach((name, id) -> idByKey.put(new AttributeKey(name), id));
        return idByKey;
    }

    /** The three steps described in the class documentation; {@code select} and {@code insert} return ids by value. */
    private static Map<String, Integer> resolve(
            Collection<String> values,
            Function<List<String>, Map<String, Integer>> select,
            Function<List<String>, Map<String, Integer>> insert) {
        Map<String, Integer> ids = new HashMap<>();
        List<String> requested = values.stream().distinct().toList();
        if (!requested.isEmpty()) {
            ids.putAll(select.apply(requested));
        }
        List<String> notStored = missing(requested, ids);
        if (!notStored.isEmpty()) {
            ids.putAll(insert.apply(notStored));
        }
        List<String> skippedByInsert = missing(requested, ids);
        if (!skippedByInsert.isEmpty()) {
            ids.putAll(select.apply(skippedByInsert));
        }
        return ids;
    }

    private static List<String> missing(Collection<String> values, Map<String, Integer> ids) {
        return values.stream()
                .distinct()
                .filter(value -> !ids.containsKey(value))
                .toList();
    }

    private Map<String, Integer> idsByName(String sql, Map<String, ?> parameters) {
        Map<String, Integer> ids = new HashMap<>();
        jdbc.query(sql, parameters, row -> {
            ids.put(row.getString("name"), row.getInt("id"));
        });
        return ids;
    }

    /** PgJDBC binds a {@code String[]} as a {@code text[]} parameter. */
    private static String[] array(List<String> values) {
        return values.toArray(String[]::new);
    }
}
