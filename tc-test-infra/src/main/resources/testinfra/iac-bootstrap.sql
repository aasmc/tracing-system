-- What infrastructure-as-code does before tc-schema-migrator runs (04 §4 "Role setup", §13.1).
-- Executed once per JVM as the container superuser. Idempotent, so a reused container (local
-- testcontainers.reuse.enable=true) can run it again. Passwords are test-only: role name = password.
DO $$
DECLARE
    r record;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tc_owner') THEN
        CREATE ROLE tc_owner NOLOGIN;
    END IF;
    FOR r IN SELECT * FROM (VALUES ('tc_migrator', true),  ('tc_maintenance', true),
                                   ('tc_ingester', false), ('tc_enricher', false),
                                   ('tc_admin', false),    ('tc_query', false)) AS v(name, owner_member)
    LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r.name) THEN
            EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', r.name, r.name);
            IF r.owner_member THEN
                EXECUTE format('GRANT tc_owner TO %I', r.name);
            END IF;
        END IF;
    END LOOP;
END
$$;

CREATE SCHEMA IF NOT EXISTS tc AUTHORIZATION tc_owner;
