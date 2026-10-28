-- Data-preserving upgrade for installations initialized with the former aiops schema.
BEGIN;
SET LOCAL lock_timeout = '5s';
DO $$
DECLARE
    names text[] := ARRAY['schema_migrations','batches','reports','report_versions',
                          'report_dependencies','analysis_jobs','job_attempts'];
    name text;
    obj record;
BEGIN
    IF to_regclass('aiops.schema_migrations') IS NULL THEN
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM aiops.schema_migrations WHERE version=1) THEN
        RAISE EXCEPTION 'Unrecognized legacy migration ledger; no tables moved';
    END IF;
    FOREACH name IN ARRAY names LOOP
        IF to_regclass(format('aiops.%I',name)) IS NULL THEN
            RAISE EXCEPTION 'Incomplete legacy table set; no tables moved';
        END IF;
        IF to_regclass(format('public.%I','aiops_'||name)) IS NOT NULL THEN
            RAISE EXCEPTION 'Destination already exists; no merge or overwrite performed';
        END IF;
    END LOOP;
    FOREACH name IN ARRAY names LOOP
        -- Prefix indexes/owned sequences before SET SCHEMA to avoid generic-name collisions.
        FOR obj IN SELECT conname FROM pg_constraint
            WHERE conrelid=to_regclass(format('aiops.%I',name)) AND contype IN ('p','u','x')
        LOOP
            EXECUTE format('ALTER TABLE aiops.%I RENAME CONSTRAINT %I TO %I',name,obj.conname,'aiops_'||obj.conname);
        END LOOP;
        FOR obj IN SELECT indexname FROM pg_indexes
            WHERE schemaname='aiops' AND tablename=name AND left(indexname,6)<>'aiops_'
        LOOP
            EXECUTE format('ALTER INDEX aiops.%I RENAME TO %I',obj.indexname,'aiops_'||obj.indexname);
        END LOOP;
        FOR obj IN SELECT s.relname FROM pg_class s JOIN pg_depend d ON d.objid=s.oid
            WHERE s.relkind='S' AND d.refobjid=to_regclass(format('aiops.%I',name)) AND d.deptype IN ('a','i')
        LOOP
            EXECUTE format('ALTER SEQUENCE aiops.%I RENAME TO %I',obj.relname,'aiops_'||obj.relname);
        END LOOP;
        EXECUTE format('ALTER TABLE aiops.%I RENAME TO %I',name,'aiops_'||name);
        EXECUTE format('ALTER TABLE aiops.%I SET SCHEMA public','aiops_'||name);
    END LOOP;
    -- Drop only an empty legacy namespace. Unrelated objects are never removed.
    BEGIN
        DROP SCHEMA aiops;
    EXCEPTION WHEN dependent_objects_still_exist THEN
        NULL;
    END;
END $$;
COMMIT;
