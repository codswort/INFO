-- 2.1 процедура добавления P2P проверки
CREATE OR REPLACE PROCEDURE add_check_p2p(
    checked_peer_ VARCHAR,
    checking_peer_ VARCHAR,
    title_ VARCHAR,
    state_ CHECK_STATUS,
    time_ TIME
)
AS
$$
DECLARE
    old_id      BIGINT  := (SELECT id
                            FROM checks
                            WHERE peer = checked_peer_
                              AND date = CURRENT_DATE
                              AND task = title_
                            ORDER BY id DESC
                            LIMIT 1);
    old_state   VARCHAR := (SELECT p2p.state
                            FROM p2p
                            WHERE p2p.check = old_id
                            ORDER BY time DESC
                            LIMIT 1);
    old_time    TIME    := (SELECT p2p.time
                            FROM p2p
                            WHERE p2p.check = old_id
                            ORDER BY time DESC
                            LIMIT 1);
    old_peer    VARCHAR := (SELECT p2p.checking_peer
                            FROM p2p
                            WHERE p2p.check = old_id
                              AND p2p.checking_peer = checking_peer_
                            ORDER BY time DESC
                            LIMIT 1);
    old_project VARCHAR := (SELECT parent_task
                            FROM tasks
                            WHERE title = title_);
BEGIN
    IF (old_project IS NOT NULL) THEN
        IF (SUBSTRING(old_project FROM '^[^0-6]+') = 'C') THEN
            old_project = (SELECT state
                           FROM verter
                                    JOIN checks c ON verter."check" = c.id
                           WHERE peer = checked_peer_
                             AND task = old_project
                             AND state = 'Success'
                             AND ((date = CURRENT_DATE AND time < time_) OR (date < CURRENT_DATE))
                           LIMIT 1);
        ELSE
            old_project = (SELECT state
                           FROM p2p
                                    JOIN checks c ON p2p."check" = c.id
                           WHERE peer = checked_peer_
                             AND task = old_project
                             AND state = 'Success'
                             AND ((date = CURRENT_DATE AND time < time_) OR (date < CURRENT_DATE))
                           LIMIT 1);
        END IF;
    ELSE
        old_project = 'Success';
    END IF;
    IF (checked_peer_ <> checking_peer_) THEN
        IF (state_ = 'Start' AND (old_time IS NULL OR old_time < time_) AND old_project = 'Success') THEN
            IF (old_state IS NULL OR old_state = 'Success' OR old_state = 'Failure') THEN
                INSERT INTO checks(peer, task, date) VALUES (checked_peer_, title_, CURRENT_DATE);
            END IF;
            IF (old_state = 'Start') THEN
            ELSE
                INSERT INTO p2p ("check", checking_peer, state, time)
                VALUES ((SELECT checks.id
                         FROM checks
                         WHERE checks.peer = checked_peer_
                           AND task = title_
                           AND date = CURRENT_DATE
                         ORDER BY id DESC
                         LIMIT 1),
                        checking_peer_,
                        state_,
                        time_);
            END IF;
        ELSIF (old_state = 'Start' AND old_time < time_ AND old_peer IS NOT NULL) THEN
            INSERT INTO p2p ("check", checking_peer, state, time)
            VALUES (old_id,
                    checking_peer_,
                    state_,
                    time_);
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;



-- 2.2  процедура добавления проверки Verter'ом
CREATE OR REPLACE PROCEDURE add_check_verter(
    checked_peer_ VARCHAR, --ник проверяемого
    title_ VARCHAR, --название задачи
    state_ CHECK_STATUS, -- статус проверки Вертера
    time_ TIME --ВРЕМЯ
)
AS
$$
DECLARE
    old_state  CHECK_STATUS := (SELECT p2p.state
                                FROM p2p
                                         JOIN checks ON checks.id = p2p.check
                                WHERE title_ = checks.task
                                  AND checked_peer_ = checks.peer
                                ORDER BY checks.date DESC, p2p.time DESC
                                LIMIT 1);
    old_check  BIGINT       := (SELECT p2p.check
                                FROM p2p
                                         JOIN checks ON checks.id = p2p.check
                                WHERE title_ = checks.task
                                  AND checked_peer_ = checks.peer
                                ORDER BY checks.date DESC, p2p.time DESC
                                LIMIT 1);
    vert_start VARCHAR      := (SELECT state
                                FROM verter
                                WHERE "check" = old_check
                                ORDER BY verter.time DESC
                                LIMIT 1);
    vert_time  TIME         := (selEct time
                                FROM verter
                                WHERE "check" = old_check
                                ORDER BY verter.time DESC
                                LIMIT 1);
BEGIN
    IF (state_ = 'Start') THEN --если приходит старт
        IF (vert_start = 'Start' OR vert_start = 'Success' OR vert_start = 'Failure') THEN
        ELSE --и если старт не стоит
            IF ((substring(title_ FROM '^[^0-6]+') = 'C') AND old_state = 'Success') THEN
                INSERT INTO verter("check", state, time)
                VALUES (old_check, state_, time_);
            END IF;
        END IF;
    ELSIF (vert_start = 'Start' AND time_ > vert_time) THEN --если приходит (фэйл или саксес) и стоит старт и приходит время позже, чем стоит
        IF ((substring(title_ FROM '^[^0-6]+') = 'C') AND old_state = 'Success') THEN
            INSERT INTO verter("check", state, time)
            VALUES (old_check, state_, time_);
        END IF;
    END IF;
END;
$$ LANGUAGE PLPGSQL;



-- 2.3 триггер: после добавления записи со статутом "начало" в таблицу P2P, изменит соответствующую запись в таблице TransferredPoints
CREATE OR REPLACE FUNCTION fnc_trg_p2p_insert()
    RETURNS TRIGGER AS
$trg_p2p_insert$
DECLARE
    checked_peer_ VARCHAR := (SELECT peer
                              FROM checks
                              WHERE checks.id = new."check"
                                AND date = CURRENT_DATE);
BEGIN

    IF (tg_op = 'INSERT' AND new.state <> 'Start') THEN
        IF ((SELECT id
             FROM transferred_points
             WHERE checking_peer = new.checking_peer
               AND checked_peer = checked_peer_) = NULL) THEN
            INSERT INTO transferred_points(checking_peer, checked_peer, points_amount)
            SELECT (SELECT peer FROM checks WHERE checks.id = new."check"), new.checking_peer, 1;
        ELSE
            UPDATE transferred_points
            SET points_amount = (points_amount + 1)
            WHERE checking_peer = new.checking_peer
              AND checked_peer = checked_peer_;
        END IF;
    END IF;
    RETURN NULL;
END;
$trg_p2p_insert$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_p2p_insert ON p2p;
CREATE TRIGGER trg_p2p_insert
    AFTER INSERT
    ON p2p
    FOR EACH ROW
EXECUTE FUNCTION fnc_trg_p2p_insert();



-- 2.4 триггер: перед добавлением записи в таблицу XP, проверит корректность добавляемой записи
CREATE OR REPLACE FUNCTION fnc_trg_xp_insert()
    RETURNS TRIGGER AS
$fnc_trg_xp_insert$
DECLARE
    xp_max            BIGINT       := (SELECT DISTINCT max_xp
                                       FROM tasks
                                                JOIN checks ON checks.task = tasks.title
                                       WHERE checks.task = (seleCt task FROM checks WHERE id = NEW."check"));
    not_c_task_state      CHECK_STATUS := (SELECT p2p.state
                                       FROM p2p
                                       WHERE "check" = new."check"
                                       ORDER BY time DESC
                                       LIMIT 1);
    c_task_state CHECK_STATUS := (SELECT verter.state
                                       FROM verter
                                       WHERE "check" = new."check"
                                       ORDER BY time DESC
                                       LIMIT 1);
    task_name         VARCHAR      := (SELECT checks.task
                                       FROM checks
                                       WHERE id = new."check");
    repeat_id BIGINT := (SELECT "check" FROM xp WHERE "check" = NEW."check");
BEGIN
    IF (tg_op = 'INSERT' AND repeat_id IS NULL) THEN
        IF (new.xp_amount <= xp_max) THEN
            IF ((substring(task_name FROM '^[^0-6]+') = 'C') AND c_task_state = 'Success') THEN
                RETURN NEW;
            eLsif ((substring(task_name FROM '^[^0-6]+') <> 'C') AND not_c_task_state = 'Success') THEN
                RETURN NEW;
            END IF;
        END IF;
    END IF;
    RETURN NULL;
END;
$fnc_trg_xp_insert$ LANGUAGE PLPGSQL;

DROP TRIGGER IF EXISTS trg_xp_insert ON xp;
CREATE TRIGGER trg_xp_insert
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE FUNCTION fnc_trg_xp_insert();





-- добавление корректных проверок
CALL add_check_p2p('annabelw', 'quayleco', 'CPP1_s21_matrixplus', 'Start', '18:50:23');
CALL add_check_p2p('annabelw', 'quayleco', 'CPP1_s21_matrixplus', 'Failure', '19:50:23');
CALL add_check_p2p('annabelw', 'codswort', 'CPP1_s21_matrixplus', 'Start', '20:50:23');
CALL add_check_p2p('annabelw', 'codswort', 'CPP1_s21_matrixplus', 'Success', '21:50:23');
CALL add_check_p2p('edwinevi', 'nancieco', 'CPP1_s21_matrixplus', 'Start', '18:50:23');
CALL add_check_p2p('edwinevi', 'nancieco', 'CPP1_s21_matrixplus', 'Success', '19:50:23');
CALL add_check_p2p('codswort', 'edwinevi', 'CPP1_s21_matrixplus', 'Start', '20:50:23');
CALL add_check_p2p('codswort', 'edwinevi', 'CPP1_s21_matrixplus', 'Success', '21:50:23');
CALL add_check_p2p('thracebe', 'edwinevi', 'C6_s21_matrix', 'Start', '12:50:23');
CALL add_check_p2p('thracebe', 'edwinevi', 'C6_s21_matrix', 'Success', '13:50:23');
CALL add_check_verter('thracebe', 'C6_s21_matrix','Start', '14:40:43');
CALL add_check_verter('thracebe', 'C6_s21_matrix','Success', '14:42:43');

INSERT INTO xp("check", xp_amount) VALUES (49, 300);
INSERT INTO xp("check", xp_amount) VALUES (50, 300);
INSERT INTO xp("check", xp_amount) VALUES (51, 300);
INSERT INTO xp("check", xp_amount) VALUES (52, 300);


-- проверка на некорректные данные
CALL add_check_p2p('annabelw', 'edwinevi', 'CPP1_s21_matrixplus', 'Failure', '19:50:23');
CALL add_check_p2p('annabelw', 'edwinevi', 'CPP1_s21_matrixplus', 'Start', '19:50:23');
CALL add_check_p2p('thracebe', 'quayleco', 'CPP1_s21_matrixplus', 'Start', '18:50:23');
CALL add_check_p2p('thracebe', 'quayleco', 'CPP1_s21_matrixplus', 'Failure', '19:50:23');
CALL add_check_p2p('nancieco', 'quayleco', 'CPP1_s21_matrixplus', 'Start', '18:50:23');
CALL add_check_p2p('nancieco', 'quayleco', 'CPP1_s21_matrixplus', 'Failure', '19:50:23');
CALL add_check_verter('annabelw', 'C7_SmartCalc','Start', '14:40:43');
CALL add_check_verter('annabelw', 'C7_SmartCalc','Success', '14:42:43');
INSERT INTO xp("check", xp_amount) VALUES (47, 300);
INSERT INTO xp("check", xp_amount) VALUES (49, 3000);
