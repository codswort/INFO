-- Cоздание базы данных
DROP DATABASE IF EXISTS part4;
CREATE DATABASE part4;

-- Запускается после переключения на базу part4
CREATE TABLE IF NOT EXISTS peers
(
    nickname VARCHAR PRIMARY KEY,
    birthday DATE NOT NULL
);
CREATE TABLE IF NOT EXISTS del1
(
    id   BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    peer VARCHAR
);
CREATE TABLE IF NOT EXISTS del2
(
    id   BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    peer VARCHAR
);
CREATE TABLE IF NOT EXISTS level
(
    id   BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    peer VARCHAR REFERENCES peers (nickname),
    lvl  INTEGER
);
CREATE TABLE IF NOT EXISTS lvl_xp
(
    lvl     INTEGER PRIMARY KEY,
    exp     INTEGER,
    add_exp INTEGER
);

CREATE TABLE IF NOT EXISTS accrual_xp
(
    peer    VARCHAR NOT NULL REFERENCES peers (nickname),
    accrual INTEGER
);

--
CREATE OR REPLACE PROCEDURE fnc_import(path_to_file TEXT, CHAR DEFAULT ',')
AS
$$
BEGIN
    EXECUTE CONCAT('COPY peers FROM ', '''', $1, 'peers.csv'' DELIMITER', QUOTE_LITERAL($2), 'CSV');
    EXECUTE CONCAT('COPY level(peer,lvl) FROM ', '''', $1, 'level.csv'' DELIMITER',
                   QUOTE_LITERAL($2), 'CSV');
    EXECUTE CONCAT('COPY lvl_xp(lvl, exp, add_exp) FROM ', '''', $1, 'lvl_xp.csv'' DELIMITER',
                   QUOTE_LITERAL($2), 'CSV');
    EXECUTE CONCAT('COPY accrual_xp FROM ', '''', $1, 'accrual_xp.csv'' DELIMITER',
                   QUOTE_LITERAL($2), 'CSV');
END;
$$ LANGUAGE PLPGSQL;
-- CALL fnc_import('/users/quayleco/projects/sql/sql2_info_v1.0-1/src/csv/');


CREATE OR REPLACE FUNCTION fnc_trg_accrual_insert()
    RETURNS TRIGGER AS
$trg_accrual_insert$
DECLARE
    new_xp  INTEGER := (SELECT SUM(accrual)
                        FROM accrual_xp
                        GROUP BY peer
                        HAVING accrual_xp.peer = NEW.peer);
    new_lvl INTEGER := (SELECT lvl
                        FROM lvl_xp
                        WHERE exp < new_xp
                        ORDER BY exp DESC
                        LIMIT 1);
BEGIN
    IF (tg_op = 'INSERT') THEN
        UPDATE level SET lvl = new_lvl WHERE peer = NEW.peer;
    END IF;
    RETURN NULL;
END;
$trg_accrual_insert$ LANGUAGE PLPGSQL;

DROP TRIGGER IF EXISTS trg_accrual_insert ON accrual_xp;
DROP TRIGGER IF EXISTS trg_accrual_insert2 ON accrual_xp;
CREATE TRIGGER trg_accrual_insert
    AFTER INSERT
    ON accrual_xp
    FOR EACH ROW
EXECUTE FUNCTION fnc_trg_accrual_insert();
CREATE TRIGGER trg_accrual_insert2
    AFTER INSERT
    ON accrual_xp
    FOR EACH ROW
EXECUTE FUNCTION fnc_trg_accrual_insert();


CREATE OR REPLACE FUNCTION fnc_peer_max()
    RETURNS VARCHAR
AS
$$
BEGIN
    RETURN (SELECT peer FROM (SELECT peer, SUM(accrual) FROM accrual_xp GROUP BY peer ORDER BY 2 DESC LIMIT 1) max_xp);
END;
$$ LANGUAGE PLPGSQL;

-- INSERT INTO accrual_xp VALUES ('edwinevi', 300);
-- SELECT fnc_peer_max();


-- 4.1 хранимая процедура, которая, не уничтожая базу данных, уничтожает все те таблицы текущей базы данных, имена которых начинаются с фразы 'TableName'.
CREATE OR REPLACE PROCEDURE del_all_table(IN name VARCHAR)
AS
$$
DECLARE
    i           INTEGER := 1;
    count_table INTEGER := (SELECT COUNT(*)
                            FROM (SELECT tablename
                                  FROM pg_tables
                                  WHERE schemaname = 'public'
                                    AND tablename LIKE CONCAT(name, '%')) AS cou);
    name_table  VARCHAR := NULL;
BEGIN
    LOOP
        IF i > count_table THEN
            EXIT;
        END IF;
        name_table := (SELECT tablename
                       FROM pg_tables
                       WHERE schemaname = 'public'
                         AND tablename LIKE CONCAT(name, '%')
                       LIMIT 1);
        EXECUTE CONCAT('DROP TABLE ', name_table);
        i = i + 1;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;
-------------------вызов процедуры-----------
CALL del_all_table('del');
----------------------------------------------


-- 4.2 хранимая процедура с выходным параметром, которая выводит список имен и параметров всех скалярных SQL функций пользователя в текущей базе данных.
-- Имена функций без параметров не выводить. Имена и список параметров должны выводиться в одну строку.
-- Выходной параметр возвращает количество найденных функций
---вспомогательная функция №1--------------------------
CREATE OR REPLACE FUNCTION one_fnc(IN parametr VARCHAR)
    RETURNS VARCHAR
AS
$$
BEGIN
    RETURN (SELECT peer FROM (SELECT peer, SUM(accrual) FROM accrual_xp GROUP BY peer ORDER BY 2 DESC LIMIT 1) max_xp);
END;
$$ LANGUAGE PLPGSQL;
-----вспомогательная функция №2--------------------------
CREATE OR REPLACE FUNCTION two_fnc(IN parametr VARCHAR, IN pap INTEGER)
    RETURNS VARCHAR
AS
$$
BEGIN
    RETURN (SELECT peer FROM (SELECT peer, SUM(accrual) FROM accrual_xp GROUP BY peer ORDER BY 2 DESC LIMIT 1) max_xp);
END;
$$ LANGUAGE PLPGSQL;
-----процедура--------------------------
CREATE OR REPLACE PROCEDURE name_function(INOUT res REFCURSOR, OUT amount_of_function INTEGER)
AS
$$
BEGIN
    CREATE VIEW output_table AS
    (
    WITH copy AS (SELECT routine_name, parameter_name, parameters.data_type
                  FROM information_schema.routines
                           JOIN information_schema.parameters ON parameters.specific_name = routines.specific_name
                           JOIN pg_proc ON pg_proc.proname = routines.routine_name
                  WHERE routines.specific_schema = 'public'
                    AND pronargs > 0 and routine_type = 'FUNCTION'
                  ORDER BY 1)
    SELECT routine_name                                          AS function_name,
           STRING_AGG(parameter_name || ': ' || data_type, ', ') AS name_and_type_of_parameter
    FROM copy
    GROUP BY 1);
    OPEN res FOR
        SELECT * FROM output_table;
    amount_of_function = (SELECT COUNT(*) FROM output_table);
    DROP VIEW output_table;
END;
$$ LANGUAGE PLPGSQL;
-------------------вызов процедуры-----------
BEGIN;
CALL name_function('list_function', NULL);
FETCH ALL IN "list_function";
END;
----------------------------------------------


-- 4.3 хранимая процедура с выходным параметром, которая уничтожает все SQL DML триггеры в текущей базе данных.
-- Выходной параметр возвращает количество уничтоженных триггеров.
CREATE OR REPLACE PROCEDURE del_triggers(OUT amount_del_trigger INTEGER)
AS
$$
DECLARE
    i             INTEGER := 1;
    amount        INTEGER := 0;
    count_trigger INTEGER := (SELECT COUNT(*)
                              FROM (SELECT * FROM pg_trigger WHERE tgisinternal = FALSE) AS cow);
    name_trigger  VARCHAR := NULL;
    name_table    VARCHAR := NULL;
BEGIN
    LOOP
        IF i > count_trigger THEN
            EXIT;
        END IF;
        name_trigger := (SELECT tgname
                         FROM (SELECT tgrelid, tgname
                               FROM pg_trigger
                               WHERE tgisinternal = FALSE
                               LIMIT 1) AS name_trigger);
        name_table := (SELECT relname
                       FROM pg_class
                       WHERE oid =
                             (SELECT tgrelid FROM pg_trigger WHERE tgisinternal = FALSE AND tgname = name_trigger));
        EXECUTE CONCAT('DROP TRIGGER ', name_trigger, ' ON ', name_table);
        amount = amount + 1;
        i = i + 1;
    END LOOP;
    SELECT amount INTO amount_del_trigger;
END;
$$ LANGUAGE PLPGSQL;
-------------------вызов процедуры-----------
BEGIN;
CALL del_triggers(NULL);
END;
----------------------------------------------


-- 4.4 хранимая процедура с входным параметром, которая выводит имена и описания типа объектов (только хранимых процедур и скалярных функций),
-- в тексте которых на языке SQL встречается строка, задаваемая параметром процедуры.
CREATE OR REPLACE PROCEDURE list_fnc_and_prc(IN name VARCHAR, INOUT res REFCURSOR)
AS
$$
BEGIN
    OPEN res FOR
        SELECT DISTINCT routine_name name, routine_type type
        FROM information_schema.routines
                 JOIN part4.pg_catalog.pg_proc ON routines.routine_name = pg_proc.proname
        WHERE (routine_type = 'FUNCTION' OR routine_type = 'PROCEDURE')
          AND prosrc LIKE CONCAT('%', name, '%');
END;
$$ LANGUAGE PLPGSQL;
-------------------вызов процедуры-----------
BEGIN;
CALL list_fnc_and_prc('SELECT', 'res');
FETCH ALL IN "res";
END;
----------------------------------------------
