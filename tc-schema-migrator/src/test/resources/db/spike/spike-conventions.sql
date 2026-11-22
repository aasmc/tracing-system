--liquibase formatted sql

--changeset tc:spike-01-ownership
SET LOCAL lock_timeout = '5s';
CREATE TABLE tc.spike_owned (id int NOT NULL);
--rollback DROP TABLE tc.spike_owned;

--changeset tc:spike-02-function splitStatements:false
CREATE FUNCTION tc.spike_fn() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1;
    RETURN 42;
END
$$;

--changeset tc:spike-03-concurrent-index runInTransaction:false
CREATE INDEX CONCURRENTLY spike_owned_id_idx ON tc.spike_owned (id);

--changeset tc:spike-04-parameters
CREATE TABLE tc.spike_param AS SELECT ${fullDays}::int AS full_days, ${digestDays}::int AS digest_days;

--changeset tc:spike-05-grants runOnChange:true
GRANT SELECT ON tc.spike_owned TO tc_query;
