--1
CREATE OR REPLACE FUNCTION FNC_VIEWING_TABLE()
    RETURNS TABLE
            (
                peer_1        VARCHAR,
                peer_2        VARCHAR,
                points_Amount INTEGER
            )
AS
$$
WITH each_other AS (SELECT one.checking_peer                     AS peer1,
                           one.checked_peer                      AS peer2,
                           one.points_amount - two.points_amount AS points_amount1
                    FROM transferred_points AS one
                             JOIN transferred_points AS two
                                  ON one.checking_peer = two.checked_peer
                                      AND one.checked_peer = two.checking_peer),
     minus AS (SELECT checked_peer,
                      checking_peer,
                      -points_amount
               FROM transferred_points
               WHERE NOT EXISTS(SELECT 1
                                FROM each_other eo
                                WHERE eo.peer1 = transferred_points.checking_peer
                                  AND eo.peer2 = transferred_points.checked_peer))
SELECT checking_peer AS peer_1,
       checked_peer  AS peer_2,
       points_amount AS points_amount
FROM transferred_points
WHERE NOT EXISTS(SELECT 1
                 FROM each_other eo
                 WHERE eo.peer1 = transferred_points.checking_peer
                   AND eo.peer2 = transferred_points.checked_peer)
UNION
SELECT *
FROM each_other
UNION
SELECT *
FROM minus
ORDER BY 1;
$$ LANGUAGE sql;

SELECT *
FROM fnc_viewing_table()
ORDER BY 1, 2;

--2
CREATE OR REPLACE FUNCTION fnc_viewing_xp()
    RETURNS TABLE
            (
                Peer VARCHAR,
                Task VARCHAR,
                XP   INTEGER
            )
AS
$$
    SELECT checks.peer, checks.task, xp.xp_amount FROM checks
    JOIN xp ON xp."check"=checks.id
$$ LANGUAGE SQL;

SELECT *
FROM fnc_viewing_xp()
ORDER BY 2;


--3
CREATE OR REPLACE FUNCTION fnc_only_entry(IN pdate DATE)
    RETURNS SETOF peers AS
$$
WITH entry AS (SELECT *
               FROM peers
                        JOIN time_tracking tt ON peers.nickname = tt.peer
               WHERE (tt.date = pdate AND tt.state = 1)),
     exit AS (SELECT *
              FROM peers
                       JOIN time_tracking tt ON peers.nickname = tt.peer
              WHERE (tt.date = pdate AND tt.state = 2))
        ,
     both_ AS (SELECT *
               FROM entry
               WHERE nickname IN (SELECT nickname FROM exit)
               UNION
               SELECT *
               FROM exit
               WHERE nickname IN (SELECT nickname FROM entry)),
     visit AS (SELECT *
               FROM peers
                        JOIN time_tracking tt ON peers.nickname = tt.peer
               WHERE tt.date = pdate)
SELECT nickname, birthday
FROM visit
EXCEPT
SELECT nickname, birthday
FROM both_;
$$ LANGUAGE sql;

SELECT *
FROM fnc_only_entry('2023-01-01');


--4
CREATE OR REPLACE PROCEDURE prc_p2p_change(INOUT curs REFCURSOR)
AS
$$
BEGIN
    OPEN curs FOR
        WITH checking_peer AS (SELECT transferred_points.checking_peer, SUM(transferred_points.points_amount) AS sum
                               FROM transferred_points
                               GROUP BY transferred_points.checking_peer),
             checked_peer AS (SELECT transferred_points.checked_peer, -SUM(transferred_points.points_amount) AS sub
                              FROM transferred_points
                              GROUP BY transferred_points.checked_peer),
             un AS (SELECT checking_peer AS peer, sum AS points_change
                    FROM checking_peer
                    UNION ALL
                    SELECT *
                    FROM checked_peer)
        SELECT peer, sum(points_change) AS points_change
        FROM un
        GROUP BY peer
        ORDER BY points_change DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_p2p_change('curs');
FETCH ALL IN "curs";
END;


--5
CREATE OR REPLACE PROCEDURE prc_p2p_change_on_first(INOUT curs REFCURSOR)
AS
$$
BEGIN
    OPEN curs FOR
        SELECT fnc_viewing_table.peer_1 AS peer, SUM(Points_Amount) AS points_change
        FROM fnc_viewing_table()
        GROUP BY fnc_viewing_table.peer_1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_p2p_change_on_first('curs');
FETCH ALL IN "curs";
END;


--6
CREATE OR REPLACE PROCEDURE prc_often_checked(INOUT curs REFCURSOR)
AS
$$
BEGIN
    OPEN curs FOR
        WITH counts AS (SELECT checks.date, checks.task, COUNT(checks.task) AS counts
                        FROM checks
                        GROUP BY checks.date, checks.task),
             maxims AS (SELECT counts.date, MAX(counts.counts) AS maxim
                        FROM counts
                        GROUP BY counts.date)
        SELECT counts.date, counts.task
        FROM counts
                 JOIN maxims ON counts.date = maxims.date
        WHERE counts = maxims.maxim;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_often_checked('curs');
FETCH ALL IN "curs";
END;


--7
CREATE OR REPLACE PROCEDURE prc_last_check(INOUT curs REFCURSOR, IN pblock VARCHAR)
AS
$$
BEGIN
    OPEN curs FOR
        WITH block_tasks AS (
            SELECT title
            FROM tasks
            WHERE substring(title FROM '^[^0-9]+') = pblock
        ),
             done AS ( -- ищем пиров завершивших блок
                 SELECT checks.peer, count(DISTINCT checks.task) AS total_checks
                 FROM checks
                          JOIN xp x ON checks.id = x."check"
                          JOIN block_tasks ON checks.task = block_tasks.title
                 GROUP BY checks.peer
                 HAVING count(DISTINCT checks.task) IN (SELECT COUNT(title) FROM block_tasks)),
             final_day AS (SELECT done.peer, MAX(checks.id) AS max_
                           FROM done -- ищем максимальный id
--                                     JOIN checks ON done.peer = checks.peer
                                        JOIN checks ON done.peer = checks.peer
                                        JOIN block_tasks ON checks.task = block_tasks.title
                           GROUP BY done.peer)
        SELECT final_day.peer AS peer, checks.date AS day
        FROM final_day
                 JOIN checks ON checks.id = final_day.max_;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_last_check('curs', 'DO');
FETCH ALL IN "curs";
END;

--8
CREATE OR REPLACE PROCEDURE prc_popular_peer(INOUT curs REFCURSOR)
AS
$$
BEGIN
    OPEN curs FOR
        WITH friend1 AS (SELECT friends.peer1 AS peer, friends.peer2 AS friend FROM friends),
             friend2 AS (SELECT friends.peer2 AS peer, friends.peer1 AS friend FROM friends),
             all_fr AS (SELECT *
                        FROM friend1
                        UNION
                        SELECT *
                        FROM friend2
                        ORDER BY 1),
             recs AS (SELECT all_fr.peer, recommendations.recommended_peer
                      FROM all_fr
                               JOIN recommendations ON all_fr.friend = recommendations.peer
                      WHERE all_fr.peer <> recommendations.recommended_peer),
             count_ AS (SELECT recs.peer, recs.recommended_peer, count(recs.recommended_peer) AS total_count
                        FROM recs
                        GROUP BY recs.peer, recs.recommended_peer),
             max_counts AS (
                 SELECT peer, MAX(total_count) AS max_count
                 FROM count_
                 GROUP BY peer
             )
        SELECT count_.peer, count_.recommended_peer
        FROM count_
                 JOIN max_counts
                      ON count_.peer = max_counts.peer AND count_.total_count = max_counts.max_count
        ORDER BY 1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_popular_peer('curs');
FETCH ALL IN "curs";
END;


--9
CREATE OR REPLACE PROCEDURE prc_blocks(INOUT curs REFCURSOR, IN plock1 VARCHAR, IN pblock2 VARCHAR)
AS
$$
BEGIN
    OPEN curs FOR
        WITH block_tasks_one AS (SELECT title AS title
                                 FROM tasks
                                 WHERE substring(title FROM '^[^0-9]+') = plock1),
             block_tasks_two AS (SELECT title
                                 FROM tasks
                                 WHERE substring(title FROM '^[^0-9]+') = pblock2),
             StartedBlock1 AS (SELECT checks.peer, count(checks.task)
                               FROM checks
                                        JOIN block_tasks_one ON checks.task = block_tasks_one.title
                               GROUP BY checks.peer
                               HAVING COUNT(checks.task) >= 1),
             StartedBlock2 AS (SELECT checks.peer, count(checks.task)
                               FROM checks
                                        JOIN block_tasks_two ON checks.task = block_tasks_two.title
                               GROUP BY checks.peer
                               HAVING COUNT(checks.task) >= 1),
             StartedBothBlocks AS (SELECT peer
                                   FROM StartedBlock1
                                   INTERSECT
                                   SELECT peer
                                   FROM StartedBlock2),
             StartAnyBlock as (SELECT peer
                               FROM StartedBlock1
                               UNION
                               SELECT peer
                               FROM StartedBlock2
             )
        SELECT ROUND((SELECT COUNT(*) FROM StartedBlock1) * 100.0 / (SELECT COUNT(*) FROM peers),2)    AS started_block_1,
               ROUND((SELECT COUNT(*) FROM StartedBlock2) * 100.0 / (SELECT COUNT(*) FROM peers),2)     AS started_block_2,
               ROUND((SELECT COUNT(*) FROM StartedBothBlocks) * 100.0 / (SELECT COUNT(*) FROM peers),2) AS started_both_blocks,
               ROUND(((SELECT COUNT(*) FROM peers) - (SELECT coUnt(*) FROM StartAnyBlock)) * 100.0 /
                (SELECT COUNT(*) FROM peers),2)                                                  AS didnt_start_any_block;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_blocks('curs', 'C', 'CPP');
FETCH ALL IN "curs";
END;


--10
CREATE OR REPLACE PROCEDURE prc_birthday_checks(INOUT curs REFCURSOR)
AS
$$
BEGIN
    OPEN curs FOR
        WITH succ AS (SELECT DISTINCT checks.peer
                      FROM checks
                               JOIN xp x ON checks.id = x."check"
                               JOIN peers p ON p.nickname = checks.peer
                      WHERE EXTRACT(MONTH FROM checks.date) = EXTRACT(MONTH FROM p.birthday)
                        AND EXTRACT(DAY FROM checks.date) = EXTRACT(DAY FROM p.birthday)),
             unsucc AS (SELECT DISTINCT checks.peer
                        FROM checks
                                 JOIN xp x ON checks.id <> x."check"
                                 JOIN peers p ON p.nickname = checks.peer
                        WHERE EXTRACT(MONTH FROM checks.date) = EXTRACT(MONTH FROM p.birthday)
                          AND EXTRACT(DAY FROM checks.date) = EXTRACT(DAY FROM p.birthday))
        SELECT ROUND((SELECT COUNT(*) FROM succ) * 100.0 / (SELECT COUNT(*) FROM peers),2)   AS successful_checks,
               ROUND((SELECT COUNT(*) FROM unsucc) * 100.0 / (SELECT COUNT(*) FROM peers),2) AS unsuccessful_checks;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_birthday_checks('curs');
FETCH ALL IN "curs";
END;

--11
CREATE OR REPLACE PROCEDURE prc_check_tasks(INOUT curs REFCURSOR, IN task1 VARCHAR, IN task2 VARCHAR, IN task3 VARCHAR)
AS
$$
BEGIN
    OPEN curs FOR
        WITH first AS (SELECT checks.peer
                       FROM checks
                                JOIN xp x ON checks.id = x."check"
                       WHERE checks.task = task1),
             second AS (SELECT checks.peer
                        FROM checks
                                 JOIN xp x ON checks.id = x."check"
                        WHERE checks.task = task2),
             not_third AS (SELECT checks.peer
                           FROM checks
                           EXCEPT
                           SELECT checks.peer
                           FROM checks
                                    JOIN xp x ON checks.id = x."check"
                           WHERE checks.task = task3)
        SELECT *
        FROM first
        INTERSECT
        SELECT *
        FROM second
        INTERSECT
        SELECT *
        FROM not_third;
END;
$$ LANGUAGE plpgsql;
BEGIN;
CALL prc_check_tasks('curs', 'C7_SmartCalc', 'CPP1_s21_matrixplus', 'CPP2_s21_containers');
FETCH ALL IN "curs";
END;


--12
CREATE
    OR REPLACE FUNCTION fnc_recur()
    RETURNS TABLE
            (
                task_title VARCHAR,
                Prev_Count INT
            )
AS
$$
WITH RECURSIVE task_tree(task_title) AS (
    SELECT tasks.title, 0 AS num_par
    FROM tasks
    WHERE parent_task IS NULL
    UNION ALL
    SELECT t.title, num_par + 1
    FROM tasks t
             JOIN task_tree tt ON t.parent_task = tt.task_title
)
SELECT task_title, num_par
FROM task_tree;
$$ LANGUAGE sql;

SELECT *
FROM fnc_recur();


--13
CREATE OR REPLACE PROCEDURE lucky_days(INOUT curs REFCURSOR, IN N INT)
AS
$$
BEGIN
    OPEN curs FOR
        SELECT DISTINCT date
        FROM (
            SELECT
                date,
                COUNT(*) OVER (PARTITION BY id_sequence) AS num_consecutive_successes
            FROM (
                SELECT
                    c.date,
                    c.id,
                    COALESCE(t.max_xp * 0.8, 0) AS min_xp_amount,
                    SUM(CASE WHEN xp.xp_amount >= COALESCE(t.max_xp * 0.8, 0) THEN 1 ELSE 0 END) OVER (ORDER BY c.date) AS id_sequence
                FROM checks c
                JOIN p2p ON c.id = p2p.check
                JOIN xp ON c.id = xp.check
                LEFT JOIN tasks t ON c.task = t.title
                WHERE p2p.state = 'Start'
                AND NOT EXISTS (
                    SELECT *
                    FROM checks c2
                    JOIN p2p p2 ON c2.id = p2.check
                    JOIN xp xp2 ON c2.id = xp2.check
                    LEFT JOIN tasks t2 ON c2.task = t2.title
                    WHERE c2.date = c.date AND c2.id < c.id
                    AND (xp2.xp_amount < COALESCE(t2.max_xp * 0.8, 0) OR xp2.xp_amount IS NULL)
                )
            ) AS s1
        ) AS s2
        WHERE num_consecutive_successes >= N
        ORDER BY date;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE prc_lucky_days(INOUT curs REFCURSOR, IN n INT)
AS
$$
BEGIN
    OPEN curs FOR
        WITH except_faile_checks AS (
            SELECT
                c.date,
                c.id,
                COALESCE(t.max_xp * 0.8, 0) AS min_xp_amount,
                SUM(CASE WHEN xp.xp_amount >= COALESCE(t.max_xp * 0.8, 0) THEN 1 ELSE 0 END) OVER (ORDER BY c.date) AS id_sequence
            FROM checks c
            JOIN p2p ON c.id = p2p.check
            JOIN xp ON c.id = xp.check
            LEFT JOIN tasks t ON c.task = t.title
            WHERE p2p.state = 'Start'
            AND NOT EXISTS (
                SELECT *
                FROM checks c2
                JOIN p2p p2 ON c2.id = p2.check
                JOIN xp xp2 ON c2.id = xp2.check
                LEFT JOIN tasks t2 ON c2.task = t2.title
                WHERE c2.date = c.date AND c2.id < c.id
                AND (xp2.xp_amount < COALESCE(t2.max_xp * 0.8, 0) OR xp2.xp_amount IS NULL)
            )
        ), number_consecutive_successful AS (
            SELECT
                date,
                COUNT(*) OVER (PARTITION BY id_sequence) AS num_consecutive_successes
            FROM except_faile_checks
        )
        SELECT DISTINCT date
        FROM number_consecutive_successful
        WHERE num_consecutive_successes >= n
        ORDER BY date;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_lucky_days('curs', 4);
FETCH ALL IN "curs";
END;


--14
CREATE OR REPLACE PROCEDURE prc_max_xp(INOUT curs REFCURSOR)
AS
$$
BEGIN
    OPEN curs FOR
        WITH selection_id AS (SELECT checks.peer, checks.task, MAX(checks.id) AS need_id
                              FROM checks
                                       JOIN xp x ON checks.id = x."check"
                              GROUP BY checks.peer, checks.task
                              ORDER BY 1, 2),
             sum AS (SELECT selection_id.peer, SUM(xp.xp_amount) AS sum_
                     FROM selection_id
                              JOIN xp ON selection_id.need_id = xp."check"
                     GROUP BY selection_id.peer)
        SELECT sum.peer
        FROM sum
        WHERE sum.sum_ = (
            SELECT MAX(sum_)
            FROM sum
        )
        GROUP BY sum.peer;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_max_xp('curs');
FETCH ALL IN "curs";
END;


--15
CREATE OR REPLACE PROCEDURE prc_before_time(INOUT curs REFCURSOR, IN ptime TIME, N INT)
AS
$$
BEGIN
    OPEN curs FOR
        WITH selection AS (SELECT time_tracking.peer, COUNT(time_tracking.time)
                           FROM time_tracking
                           WHERE (time_tracking.state = 1
                               AND time_tracking.time < ptime)
                           GROUP BY time_tracking.peer
                           HAVING COUNT(time_tracking.time) >= N)
        SELECT nickname AS peer
        FROM peers
                 JOIN selection ON selection.peer = peers.nickname;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_before_time('curs', '11:05:23', 2);
FETCH ALL IN "curs";
END;


--16
CREATE OR REPLACE PROCEDURE prc_exit_last_days(INOUT curs REFCURSOR, IN N INT, M INT)
AS
$$
BEGIN
    OPEN curs FOR
        WITH selection AS (SELECT time_tracking.peer, COUNT(time_tracking.state)
                           FROM time_tracking
                           WHERE (time_tracking.state = 2 AND
                                  (time_tracking.date BETWEEN CURRENT_DATE - N + 1 AND CURRENT_DATE))
                           GROUP BY time_tracking.peer
                           HAVING COUNT(time_tracking.time) > M)
        SELECT nickname
        FROM peers
                 JOIN selection ON selection.peer = peers.nickname;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_exit_last_days('curs', 180, 3);
FETCH ALL IN "curs";
END;


--17
CREATE OR REPLACE PROCEDURE prc_early_entries(INOUT curs REFCURSOR)
AS
$$
BEGIN
    OPEN curs FOR
        WITH total_number AS (SELECT EXTRACT(MONTH FROM time_tracking.date) AS Month,
                                     COUNT(time_tracking.time)              AS count_
                              FROM time_tracking
                                       JOIN peers ON time_tracking.peer = peers.nickname
                              WHERE EXTRACT(MONTH FROM time_tracking.date) = EXTRACT(MONTH FROM peers.birthday)
                                AND time_tracking.state = 1
                              GROUP BY EXTRACT(MONTH FROM time_tracking.date)),
             before AS (SELECT EXTRACT(MONTH FROM time_tracking.date) AS Month, COUNT(time_tracking.time) AS count_
                        FROM time_tracking
                                 JOIN peers ON time_tracking.peer = peers.nickname
                        WHERE EXTRACT(MONTH FROM time_tracking.date) = EXTRACT(MONTH FROM peers.birthday)
                          AND time_tracking.time < '12:00:00'
                          AND time_tracking.state = 1
                        GROUP BY EXTRACT(MONTH FROM time_tracking.date))
        SELECT DISTINCT to_char(time_tracking.date, 'Month')      AS month,
                        before.count_ * 100 / total_number.count_ AS Early_Entries
        FROM time_tracking
                 JOIN before ON EXTRACT(MONTH FROM time_tracking.date) = before.Month
                 JOIN total_number ON EXTRACT(MONTH FROM time_tracking.date) = total_number.Month;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_early_entries('curs');
FETCH ALL IN "curs";
END;
