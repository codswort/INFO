-- Cоздание базы данных
DROP DATABASE IF EXISTS school WITH (FORCE);
CREATE DATABASE school;

-- Запускается после переключения на базу school
CREATE TYPE CHECK_STATUS AS ENUM ('Start', 'Success', 'Failure');

CREATE TABLE IF NOT EXISTS peers
(
    nickname VARCHAR PRIMARY KEY,
    birthday DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS time_tracking
(
    id    BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    peer  VARCHAR NOT NULL REFERENCES peers (nickname),
    date  DATE    NOT NULL,
    time  TIME    NOT NULL,
    state SMALLINT CHECK ("state" BETWEEN 1 aNd 2)
);

CREATE TABLE IF NOT EXISTS recommendations
(
    id               BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    peer             VARCHAR REFERENCES peers (nickname),
    recommended_peer VARCHAR REFERENCES peers (nickname),
    CONSTRAINT check_peer_not_equal_recommended_peer CHECK (peer <> recommended_peer)
);

CREATE TABLE IF NOT EXISTS friends
(
    id    BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    peer1 VARCHAR REFERENCES peers (nickname),
    peer2 VARCHAR REFERENCES peers (nickname),
    UNIQUE (peer1, peer2),
    CONSTRAINT check_peer1_not_equal_peer2 CHECK (peer1 <> peer2)
);

CREATE TABLE IF NOT EXISTS transferred_points
(
    id            BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    checking_peer VARCHAR REFERENCES peers (nickname),
    checked_peer  VARCHAR REFERENCES peers (nickname),
    points_amount INTEGER,
    UNIQUE (checking_peer, checked_peer),
    CONSTRAINT check_checking_not_equal_checked CHECK (checking_peer <> checked_peer)
);

CREATE TABLE IF NOT EXISTS tasks
(
    title       VARCHAR PRIMARY KEY,
    parent_task VARCHAR NULL REFERENCES tasks (title),
    max_xp      INTEGER
);

CREATE TABLE IF NOT EXISTS checks
(
    id   BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    peer VARCHAR REFERENCES peers (nickname),
    task VARCHAR REFERENCES tasks (title),
    date DATE
);

CREATE TABLE IF NOT EXISTS p2p
(
    id            BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    "check"       BIGINT NOT NULL REFERENCES checks (id),
    checking_peer VARCHAR REFERENCES peers (nickname),
    state         CHECK_STATUS,
    time          TIME
);

CREATE TABLE IF NOT EXISTS xp
(
    id        BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    "check"   BIGINT REFERENCES checks (id),
    xp_amount INTEGER
);

CREATE TABLE IF NOT EXISTS verter
(
    id      BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    "check" BIGINT NOT NULL REFERENCES checks (id),
    state   CHECK_STATUS,
    time    TIME
);


--  импорт данных из csv
CREATE OR REPLACE PROCEDURE fnc_import(path_to_file TEXT, CHAR DEFAULT ',')
AS
$$
BEGIN
    EXECUTE CONCAT('COPY peers FROM ', '''', $1, 'peers.csv'' DELIMITER', QUOTE_LITERAL($2), 'CSV');
    EXECUTE CONCAT('COPY time_tracking(peer,date,time,state) FROM ', '''', $1, 'time_tracking.csv'' DELIMITER',
                   QUOTE_LITERAL($2), 'CSV');
    EXECUTE CONCAT('COPY recommendations(peer,recommended_peer) FROM ', '''', $1, 'recommendations.csv'' DELIMITER',
                   QUOTE_LITERAL($2), 'CSV');
    EXECUTE CONCAT('COPY friends(peer1, peer2) FROM ', '''', $1, 'friends.csv'' DELIMITER', QUOTE_LITERAL($2), 'CSV');
    EXECUTE CONCAT('COPY transferred_points(checking_peer,checked_peer,points_amount) FROM ', '''', $1,
                   'transferred_points.csv'' DELIMITER', QUOTE_LITERAL($2), 'CSV');
    EXECUTE CONCAT('COPY tasks(title,parent_task,max_xp) FROM ', '''', $1, 'tasks.csv'' DELIMITER', QUOTE_LITERAL($2),
                   'CSV');
    EXECUTE CONCAT('COPY checks(peer,task,date) FROM ', '''', $1, 'checks.csv'' DELIMITER', QUOTE_LITERAL($2), 'CSV');
    EXECUTE CONCAT('COPY p2p("check",checking_peer,state,time) FROM ', '''', $1, 'p2p.csv'' DELIMITER',
                   QUOTE_LITERAL($2), 'CSV');
    EXECUTE CONCAT('COPY xp("check",xp_amount) FROM ', '''', $1, 'xp.csv'' DELIMITER', QUOTE_LITERAL($2), 'CSV');
    EXECUTE CONCAT('COPY verter("check",state,time) FROM ', '''', $1, 'verter.csv'' DELIMITER', QUOTE_LITERAL($2),
                   'CSV');
END;
$$ LANGUAGE PLPGSQL;

--  экспорт данных в csv
CREATE OR REPLACE PROCEDURE fnc_export(path_to_file TEXT)
AS
$$
BEGIN
    EXECUTE CONCAT('COPY peers TO ', '''', $1, 'peers.csv'' CSV');
    EXECUTE CONCAT('COPY time_tracking(peer,date,time,state) TO ', '''', $1, 'time_tracking.csv'' CSV');
    EXECUTE CONCAT('COPY recommendations(peer,recommended_peer) TO ', '''', $1, 'recommendations.csv'' CSV');
    EXECUTE CONCAT('COPY friends(peer1, peer2) TO ', '''', $1, 'friends.csv'' CSV');
    EXECUTE CONCAT('COPY transferred_points(checking_peer,checked_peer,points_amount) TO ', '''', $1,
                   'transferred_points.csv'' CSV');
    EXECUTE CONCAT('COPY tasks(title,parent_task,max_xp) TO ', '''', $1, 'tasks.csv'' CSV');
    execute CONCAT('COPY checks(peer,task,date) TO ', '''', $1, 'checks.csv'' CSV');
    EXECUTE CONCAT('COPY p2p("check",checking_peer,state,time) TO ', '''', $1, 'p2p.csv'' CSV');
    EXECUTE CONCAT('copy xp("check",xp_amount) to ', '''', $1, 'xp.csv'' CSV');
    EXECUTE CONCAT('COPY verter("check",state,time) TO ', '''', $1, 'verter.csv'' CSV');
END;
$$ LANGUAGE PLPGSQL;


-- вызов процедуры импорта
CALL fnc_import('/users/quayleco/projects/sql/sql2_info_v1.0-1/src/csv/');

-- вызов процедуры экспорта
CALL fnc_export('/users/quayleco/projects/sql/sql2_info_v1.0-1/src/csv/');
