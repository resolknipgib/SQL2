-- 1) Drop tables with names starting with 'TableName'
DROP PROCEDURE IF EXISTS drop_tables_with_prefix CASCADE;

CREATE OR REPLACE PROCEDURE drop_tables_with_prefix() AS $$
DECLARE
    tble_name TEXT;
BEGIN
    FOR tble_name IN (
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name LIKE 'tablename%')
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', tble_name);
    END LOOP;
END;
$$ LANGUAGE plpgsql;


CREATE DATABASE test;
CREATE TABLE IF NOT EXISTS TableNameTasks
(
    Title       VARCHAR PRIMARY KEY,
    ParentTask  VARCHAR,
    MaxXP       BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS Checks
(
    ID      SERIAL  PRIMARY KEY,
    Peer    VARCHAR NOT NULL,
    Task    VARCHAR NOT NULL,
    Date    DATE
);
CREATE TABLE IF NOT EXISTS TableNameP2P
(
    ID              SERIAL  PRIMARY KEY,
    CheckID         BIGINT  NOT NULL,
    CheckingPeer    VARCHAR NOT NULL,
    Time            TIMESTAMP
);
CALL drop_tables_with_prefix();
SELECT * FROM TableNameTasks;

-- 2) List scalar functions with parameters
DROP PROCEDURE IF EXISTS list_scalar_functions CASCADE;
CREATE OR REPLACE PROCEDURE list_scalar_functions(OUT num_functions INTEGER) AS $$
DECLARE
    func RECORD;
BEGIN
    num_functions := 0;

    FOR func IN
        SELECT proname AS function_name,
               pg_get_function_arguments(pg_proc.oid) AS parameters
        FROM pg_proc
        JOIN pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
        WHERE pg_proc.prokind = 'f'
              AND pg_namespace.nspname NOT LIKE 'pg_%'
              AND pg_namespace.nspname != 'information_schema'
              AND pg_get_function_arguments(pg_proc.oid) != ''
    LOOP
        RAISE NOTICE 'Function: %; Parameters: %', func.function_name, func.parameters;
        num_functions := num_functions + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    num_functions INTEGER;
BEGIN
    CALL list_scalar_functions(num_functions);
    RAISE NOTICE 'Number of functions: %', num_functions;
END;
$$ LANGUAGE plpgsql;

-- 3) Drop SQL DML triggers
DROP PROCEDURE IF EXISTS drop_dml_triggers CASCADE;

CREATE OR REPLACE PROCEDURE drop_dml_triggers(OUT num_triggers INTEGER) AS $$
DECLARE
    trigger_name TEXT;
    relname TEXT;
BEGIN
    num_triggers := 0;
    FOR trigger_name, relname IN (
        SELECT
            tgname,
            c.relname
        FROM
            pg_trigger tr
            JOIN pg_class c ON tr.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE
            n.nspname = 'public'
            AND NOT tr.tgisinternal)
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON public.%I CASCADE', trigger_name, relname);
        num_triggers := num_triggers + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION empty_trigger_function() RETURNS TRIGGER AS $$
BEGIN
    -- Здесь ничего не происходит, функция просто возвращает NEW для триггера
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_test
        BEFORE INSERT OR UPDATE ON Checks FOR EACH ROW
       EXECUTE FUNCTION empty_trigger_function();

DO $$
DECLARE
    num_triggers INTEGER;
BEGIN
    CALL drop_dml_triggers(num_triggers);
    RAISE NOTICE 'Number of dropped triggers: %', num_triggers;
END;
$$ LANGUAGE plpgsql;

-- 4) List object types with specific string
DROP PROCEDURE IF EXISTS list_object_types_with_string CASCADE;

CREATE OR REPLACE PROCEDURE list_object_types_with_string(IN search_string TEXT) AS $$
DECLARE
    object_name TEXT;
BEGIN
    FOR object_name IN (
        SELECT
            p.proname
        FROM
            pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            JOIN pg_language l ON p.prolang = l.oid
        WHERE
            n.nspname = 'public'
            AND (l.lanname = 'plpgsql' OR l.lanname = 'sql')
            AND p.proname LIKE '%' || search_string || '%')
    LOOP
        RAISE NOTICE '%', object_name;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


CALL list_object_types_with_string('p');


