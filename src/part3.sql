-- 1) Peer's nickname 1, Peer's nickname 2, number of transferred peer points.
-- The number is negative if peer2 received more points from peer1.
CREATE OR REPLACE VIEW v_points_diff AS (
   WITH cte_pairs AS (SELECT t1.id,
                          t1.checkingpeer AS peer1,
                          t1.checkedpeer  AS peer2,
                          t1.pointsamount AS p1_to_p2,
                          t2.pointsamount AS p2_to_p1
                   FROM transferredpoints t1
                            LEFT JOIN transferredpoints t2
                                 ON t1.checkingpeer = t2.checkedpeer AND t2.checkingpeer = t1.checkedpeer)
SELECT id, peer1, peer2, p1_to_p2 - coalesce(p2_to_p1, 0) AS pointsAmount
FROM cte_pairs
);

CREATE OR REPLACE FUNCTION get_offset_v_points_diff(N INT)
RETURNS TABLE(ID INT, Peer_1 VARCHAR, Peer_2 VARCHAR, Pointsamount BIGINT)
LANGUAGE plpgsql
AS $get_points_diff_for_peers_pair$
BEGIN
    RETURN QUERY
    SELECT v.ID, v.peer1 AS peer_1, v.peer2 AS peer_2, v.pointsamount
    FROM v_points_diff v
    OFFSET N;
END
$get_points_diff_for_peers_pair$;

CREATE OR REPLACE FUNCTION get_points_diff_for_peers_pair()
RETURNS TABLE(Peer1 VARCHAR, Peer2 VARCHAR, PointsAmount BIGINT)
LANGUAGE plpgsql
AS $get_points_diff_for_peers_pair$
BEGIN
    RETURN QUERY
    SELECT v.peer1, v.peer2, v.pointsAmount
    FROM v_points_diff v
    WHERE v.peer1 NOT IN (SELECT peer_2
                          FROM (SELECT *
                                FROM get_offset_v_points_diff(id)
                                WHERE peer_1=v.peer2)
                          AS offset_v_points_diff);
END;
$get_points_diff_for_peers_pair$;

SELECT * FROM get_points_diff_for_peers_pair();


-- 2) successfully passed the check
CREATE OR REPLACE VIEW v_success_checks AS (
    SELECT id
    FROM checks
    WHERE id IN (SELECT checkid FROM verter WHERE state='Success') OR
         (id NOT IN (SELECT checkid FROM verter WHERE state='Start') AND
          id IN (SELECT checkid FROM p2p WHERE state='Success'))
);
select * from v_success_checks;

CREATE OR REPLACE FUNCTION get_received_xp_per_task()
RETURNS TABLE(Peer VARCHAR, Task VARCHAR, XP BIGINT)
LANGUAGE plpgsql
AS $get_received_xp_per_task$
BEGIN
    RETURN QUERY
    SELECT Checks.peer, Checks.task, XP.xp
    FROM Checks JOIN v_success_checks v
        ON Checks.id = v.id
                JOIN XP
        ON Checks.id = XP.checkid;
END;
$get_received_xp_per_task$;

SELECT *
FROM get_received_xp_per_task();

-- 3) the peers who have not left campus for the whole day
-- В заданиях, относящихся к этой таблице, под действием "выходить" подразумеваются все покидания кампуса за день,
-- кроме последнего.
-- В течение одного дня должно быть одинаковое количество записей с состоянием 1 и состоянием 2 для каждого пира.
CREATE OR REPLACE FUNCTION get_peers_who_love_campus(date_ date)
RETURNS TABLE(Peer varchar)
LANGUAGE plpgsql
AS $get_peers_who_love_campus$
BEGIN
    RETURN QUERY
    WITH cte_aggregated_timetracking AS (
        SELECT t.Peer as Peer_, count(*) as out_count
        FROM Timetracking t
        WHERE Date=date_ AND state=2
        GROUP BY Peer_
    )
    SELECT Peer_
    FROM cte_aggregated_timetracking t
    WHERE out_count=1;
END
$get_peers_who_love_campus$;

SELECT * FROM get_peers_who_love_campus('2023-01-01');


-- 4) the change in the number of peer points of each peer using the TransferredPoints table
-- сколько поинтов получил - сколько поинтов пир отдал
CREATE OR REPLACE PROCEDURE get_peers_points_changes(INOUT result refcursor)
LANGUAGE plpgsql
AS $get_peers_points_changes$
BEGIN
    OPEN result FOR
    WITH cte_checkingpeer AS (
    SELECT CheckingPeer AS peer, sum(pointsamount) AS ReceivedPoints
    FROM TransferredPoints
    GROUP BY peer
    ), cte_checkedgpeer AS (
        SELECT CheckedPeer AS peer, sum(pointsamount) AS LostPoints
        FROM TransferredPoints
        GROUP BY peer
    )
    SELECT t1.peer, ReceivedPoints - LostPoints AS PointsChange
    FROM cte_checkingpeer t1 JOIN cte_checkedgpeer t2
      ON t1.peer = t2.peer
    UNION
    SELECT t1.peer, ReceivedPoints AS PointsChange
    FROM cte_checkingpeer t1 LEFT JOIN cte_checkedgpeer t2
      ON t1.peer = t2.peer
    WHERE t2.peer is null
    UNION
    SELECT t2.peer, 0 - LostPoints AS PointsChange
    FROM cte_checkingpeer t1 RIGHT JOIN cte_checkedgpeer t2
      ON t1.peer = t2.peer
    WHERE t1.peer is null
    ORDER BY PointsChange DESC, Peer;
END;
$get_peers_points_changes$;

BEGIN;
CALL get_peers_points_changes('result');
FETCH ALL IN "result";
COMMIT;

-- 5)  the change in the number of peer points of each peer using the table returned by the 1)
CREATE OR REPLACE PROCEDURE get_peers_points_changes2(INOUT result refcursor)
LANGUAGE plpgsql
AS $get_peers_points_changes2$
BEGIN
    OPEN result FOR
    WITH cte_peers_points_moves AS (
        SELECT Peer1 as Peer, PointsAmount
        FROM get_points_diff_for_peers_pair()
        UNION
        SELECT Peer2 as Peer, 0 - PointsAmount
        FROM get_points_diff_for_peers_pair()
    )
    SELECT Peer, SUM(PointsAmount) AS PointsChange
    FROM cte_peers_points_moves
    GROUP BY Peer
    ORDER BY PointsChange DESC, Peer;
END
$get_peers_points_changes2$;

BEGIN;
CALL get_peers_points_changes2('result');
FETCH ALL IN "result";
COMMIT;

-- 6) the most frequently checked task for each day
CREATE OR REPLACE PROCEDURE get_most_checked_task_per_day(IN result refcursor)
LANGUAGE plpgsql
AS $get_most_checked_task_per_day$
BEGIN
    OPEN result FOR
    WITH cte_cheked_tasks AS (
        select date as day, task, count(*) as checks_count
        from checks
        group by day, task
    )
    SELECT to_char(c1.day, 'dd.mm.yyyy'), task
    FROM cte_cheked_tasks c1
    WHERE checks_count = (SELECT max(checks_count) FROM cte_cheked_tasks c2 WHERE c2.day = c1.day)
    ORDER BY day;
END;
$get_most_checked_task_per_day$;


BEGIN;
CALL get_most_checked_task_per_day('result');
FETCH ALL IN "result";
COMMIT;

--7) Find all peers who have completed the whole given block of tasks and the completion date of the last task

-- DROP PROCEDURE IF EXISTS completed_block_tasks CASCADE;
--
-- CREATE OR REPLACE PROCEDURE completed_block_tasks(IN block_name VARCHAR, IN result refcursor) AS $$
-- BEGIN
--     OPEN result FOR
--         WITH block_tasks AS (
--             SELECT *
--             FROM Tasks
--             WHERE ParentTask = block_name
--         ),
--         completed_tasks AS (
--             SELECT Peer, Task, Date
--             FROM Checks
--             WHERE Task IN (SELECT Title FROM block_tasks)
--         ),
--         completed_block AS (
--             SELECT Peer, COUNT(*) AS completed_count
--             FROM completed_tasks
--             GROUP BY Peer
--             HAVING COUNT(*) = (SELECT COUNT(*) FROM block_tasks)
--         )
--         SELECT cb.Peer, MAX(ct.Date) AS completion_date
--         FROM completed_block cb
--         JOIN completed_tasks ct ON cb.Peer = ct.Peer
--         GROUP BY cb.Peer
--         ORDER BY completion_date;
-- END;
-- $$ LANGUAGE plpgsql;

DROP PROCEDURE IF EXISTS completed_block_tasks CASCADE;
CREATE OR REPLACE PROCEDURE completed_block_tasks(IN block varchar, IN ref refcursor) AS $completed_block_tasks$
    BEGIN
        OPEN ref FOR
            WITH tasks_block AS (SELECT *
                                 FROM tasks
                                 WHERE title SIMILAR TO concat(block, '[0-9]%')),
                 last_task AS (SELECT MAX(title) AS title FROM tasks_block),
                 date_of_successful_check AS (SELECT checks.peer, checks.task, checks.date
                                              FROM checks
                                              JOIN p2p ON checks.id = p2p.checkid
                                              WHERE p2p.state = 'Success'
                                              GROUP BY checks.id)
            SELECT date_of_successful_check.peer AS Peer, date_of_successful_check.date
            FROM date_of_successful_check
            JOIN last_task ON date_of_successful_check.task = last_task.title;
    END;
$completed_block_tasks$ LANGUAGE plpgsql;

BEGIN;
CALL completed_block_tasks('CPP', 'result' );
FETCH ALL IN "result" ;
END;

--8) Determine which peer each student should go to for a check.

DROP PROCEDURE IF EXISTS recommended_checkers CASCADE;

CREATE OR REPLACE PROCEDURE recommended_checkers(IN result refcursor) AS $$
BEGIN
    OPEN result FOR
        WITH friends_recommendations AS (
            SELECT f.Peer1, r.RecommendedPeer, COUNT(*) AS recommendation_count
            FROM Friends f
            JOIN Recommendations r ON f.Peer2 = r.Peer
            GROUP BY f.Peer1, r.RecommendedPeer
        ),
        max_recommendations AS (
            SELECT Peer1, RecommendedPeer, RANK() OVER (PARTITION BY Peer1 ORDER BY recommendation_count DESC) AS rank
            FROM friends_recommendations
        )
        SELECT Peer1 AS Peer, RecommendedPeer
        FROM max_recommendations
        WHERE rank = 1;
END;
$$ LANGUAGE plpgsql;


BEGIN;
CALL recommended_checkers('result' );
FETCH ALL IN "result" ;
END;

--9) Determine the percentage of peers who: Started only block 1; Started only block 2; Started both; Have not started any of them.
CREATE OR REPLACE PROCEDURE block_start_percentages(IN block1 VARCHAR, IN block2 VARCHAR, IN result refcursor) AS $block_start_percentages$
    DECLARE count_peers bigint := (SELECT COUNT(peers.nickname) FROM peers);
    BEGIN
        OPEN result FOR
            WITH start_block1 AS (SELECT DISTINCT peer
                                  FROM checks
                                  WHERE checks.task SIMILAR TO concat(block1, '[0-9]%')),
                 start_block2 AS (SELECT DISTINCT peer
                                  FROM checks
                                  WHERE checks.task SIMILAR TO concat(block2, '[0-9]%')),
                 start_only_block1 AS (SELECT peer FROM start_block1
                                      EXCEPT
                                      SELECT peer FROM start_block2),
                 start_only_block2 AS (SELECT peer FROM start_block2
                                      EXCEPT
                                      SELECT peer FROM start_block1),
                 start_both_block AS (SELECT peer FROM start_block1
                                      INTERSECT
                                      SELECT peer FROM start_block2),
                 no_start AS (SELECT COUNT(nickname) AS peer_count
                                 FROM peers
                                 LEFT JOIN checks ON peers.nickname = checks.peer
                                 WHERE peer IS NULL)
            SELECT (((SELECT COUNT(*) FROM start_only_block1) * 100) / count_peers) AS start_only_block1,
                   (((SELECT COUNT(*) FROM start_only_block2) * 100) / count_peers) AS start_only_block2,
                   (((SELECT COUNT(*) FROM start_both_block) * 100) / count_peers) AS start_both_block,
                   (((SELECT peer_count FROM no_start) * 100) / count_peers) AS no_start;
    END;
$block_start_percentages$ LANGUAGE plpgsql;

BEGIN;
CALL block_start_percentages('C', 'DO','result' );
FETCH ALL IN "result" ;
END;

-- 10)  The percentage of peers who have passed/failed a check on their birthday
CREATE VIEW v_birthday_checks AS (
    SELECT *
    FROM checks JOIN peers
        ON checks.peer = peers.nickname
    WHERE to_char(checks.date, 'MM-DD') = to_char(peers.birthday, 'MM-DD')
);
SELECT * FROM v_birthday_checks;

CREATE OR REPLACE PROCEDURE get_birthday_checks_result(IN result refcursor)
LANGUAGE plpgsql
AS $get_birthday_checks_result$
DECLARE birthday_checks_count numeric;
DECLARE success_checks_count numeric;
DECLARE failure_checks_count numeric;
BEGIN
    SELECT count(DISTINCT peer) INTO birthday_checks_count FROM v_birthday_checks;
    SELECT count(DISTINCT peer) INTO success_checks_count FROM v_birthday_checks WHERE id IN (SELECT id FROM v_success_checks);
    SELECT count(DISTINCT peer) INTO failure_checks_count FROM v_birthday_checks WHERE id NOT IN (SELECT id FROM v_success_checks);
    OPEN result FOR
    SELECT round(100*success_checks_count/birthday_checks_count) AS SuccessfulChecks,
           round(100*failure_checks_count/birthday_checks_count) AS UnsuccessfulChecks;
END
$get_birthday_checks_result$;

BEGIN;
CALL get_birthday_checks_result('result');
FETCH ALL IN "result";
ROLLBACK;




-- 11) Determine all peers who did the given tasks 1 and 2, but did not do task 3
CREATE OR REPLACE FUNCTION is_success_task_by_peer(_peer varchar, _task varchar)
RETURNS boolean
LANGUAGE plpgsql
AS $is_success_task_by_peer$
BEGIN
    IF (
         WITH cte_success_task_by_peer AS (
            SELECT id
            FROM checks
            WHERE peer=_peer AND
                  task=_task AND
                    (id IN (SELECT checkid FROM verter WHERE state='Success') OR
                    (id NOT IN (SELECT checkid FROM verter WHERE state='Start') AND
                    id IN (SELECT checkid FROM p2p WHERE state='Success')))
         )
         SELECT count(*) FROM cte_success_task_by_peer) > 0
    THEN RETURN true;
    ELSE RETURN false;
    END IF;
END;
$is_success_task_by_peer$;


CREATE OR REPLACE PROCEDURE peers_who_did_tasks12(IN result refcursor,
                                                task1 varchar, task2 varchar, task3 varchar)
LANGUAGE plpgsql
AS $peers_who_did_tasks$
BEGIN
    OPEN result FOR
    SELECT DISTINCT peer
    FROM checks
    WHERE (SELECT * FROM is_success_task_by_peer(peer, task1)) = true AND
          (SELECT * FROM is_success_task_by_peer(peer, task2)) = true AND
          (SELECT * FROM is_success_task_by_peer(peer, task3)) = false;
END;
$peers_who_did_tasks$;

BEGIN;
CALL peers_who_did_tasks12('result', 'DO1_Linux', 'C4_s21_math', 'SQL3_RetailAnalitycs_v1.0');
FETCH ALL IN "result";
COMMIT;

-- 12) Using recursive common table expression, output the number of preceding tasks for each task
CREATE OR REPLACE PROCEDURE number_of_preceding_tasks(IN result refcursor)
LANGUAGE plpgsql
AS $number_of_preceding_tasks$
BEGIN
    OPEN result FOR
    WITH RECURSIVE cte_recursive AS (
        SELECT title as task, 0 as PrevCount
        FROM tasks
        WHERE parenttask is null
        UNION
        SELECT t.title as task, PrevCount+1
        FROM tasks t INNER JOIN cte_recursive
          ON t.parenttask = cte_recursive.task
    )
    SELECT *
    FROM cte_recursive;
END;
$number_of_preceding_tasks$;

BEGIN;
CALL number_of_preceding_tasks('result');
FETCH ALL IN "result";
COMMIT;


-- 13) Find "lucky" days for checks. A day is considered "lucky" if it has at least N consecutive successful checks
CREATE OR REPLACE VIEW v_check_result_with_time AS (
    WITH cte_ch_result_with_time AS (
        WITH cte_p2p_verter AS (
            SELECT checks.id as id, task, date, p.state as p_state, p.time, v.state as v_state
            FROM checks LEFT JOIN p2p p
              ON checks.id = p.checkid
                        LEFT JOIN verter v
              ON checks.id = v.checkid AND (v.state IN ('Success', 'Failure'))
            WHERE p.state is not null
            ORDER BY p.time
        )
        SELECT c1.id, c1.time as start_time, c2.p_state, c1.v_state as v_result, v.state as v_start, maxxp*0.8 as success_xp, xp
        FROM cte_p2p_verter c1 JOIN cte_p2p_verter c2
          ON c1.id = c2.id AND c1.p_state='Start' AND c2.p_state!='Start'
                               LEFT JOIN verter v
          ON c1.id = v.checkid AND v.state='Start'
                               JOIN tasks
          ON tasks.title=c1.task
                               JOIN xp
          ON xp.checkid=c1.id
    )
    SELECT id,
           start_time,
           start_time::date as day,
           (CASE WHEN v_result is null THEN p_state ELSE v_result END) as result,
           (CASE WHEN v_start is not null AND v_result is null THEN true ELSE false END) as v_started_not_finished
    FROM cte_ch_result_with_time
    WHERE xp >= success_xp
    ORDER BY start_time
);
-- SELECT * FROM v_check_result_with_time;

CREATE SEQUENCE IF NOT EXISTS incr_sequence
INCREMENT 1
START 1;

CREATE OR REPLACE PROCEDURE lucky_days_for_checks(IN pr_result refcursor, N integer)
LANGUAGE plpgsql
AS $lucky_days_for_checks$
BEGIN
    OPEN pr_result FOR
    WITH cte_consecutive_successful_checks_count AS (
        WITH cte_finished_partitioned_check  AS (
            WITH cte_finished_check AS (
                SELECT *, lag(result) OVER (PARTITION BY day ORDER BY start_time) AS lag_
                FROM v_check_result_with_time
                WHERE v_started_not_finished=false
            )
            SELECT start_time, day, result, lag_,
                   CASE WHEN result='Failure' THEN nextval('incr_sequence') ELSE currval('incr_sequence') END AS flag
            FROM cte_finished_check
        )
        SELECT *, count(*) OVER (PARTITION BY day, result, flag ORDER BY start_time) AS count_
        FROM cte_finished_partitioned_check
    )
    SELECT DISTINCT day as successful_day
    FROM cte_consecutive_successful_checks_count
    WHERE count_ >= N;
END;
$lucky_days_for_checks$;

BEGIN;
CALL lucky_days_for_checks('pr_result', 2);
FETCH ALL IN "pr_result";
COMMIT;



-- 14) Find the peer with the highest amount of XP
CREATE OR REPLACE PROCEDURE get_peer_with_max_xp(INOUT result refcursor)
LANGUAGE plpgsql
AS $get_peer_with_max_xp$
BEGIN
    OPEN result FOR
    WITH cte_peers_xp_per_check AS (
        SELECT nickname, xp
        FROM XP JOIN Checks
          ON xp.checkid = Checks.id
                JOIN Peers
          ON Checks.peer = Peers.nickname
    )
    SELECT nickname as Peer, sum(XP) AS XP
    FROM cte_peers_xp_per_check
    GROUP BY nickname
    ORDER BY XP DESC
    LIMIT 1;
END
$get_peer_with_max_xp$;

BEGIN;
CALL get_peer_with_max_xp('cursor');
FETCH ALL IN "cursor";
COMMIT;


-- 15) Determine the peers that came before the given time at least N times during the whole time
-- если пир зашел, вышел и еще раз зашел, считается, что он 2 раза пришел? в запросе - да
CREATE OR REPLACE PROCEDURE enter_campus_on_time_peers(IN result refcursor, time_ time, N integer)
LANGUAGE plpgsql
AS $enter_campus_on_time_peers$
BEGIN
    OPEN result FOR
    WITH cte_peer_coming_time AS (
        SELECT peer, count(*) AS coming_count
        FROM timetracking
        WHERE time<time_ AND state=1
        GROUP BY peer
    )
    SELECT peer
    FROM cte_peer_coming_time
    WHERE coming_count>=N;
END;
$enter_campus_on_time_peers$;

SELECT now()::time;

BEGIN;
CALL enter_campus_on_time_peers('result', '12:00:01', 2);
FETCH ALL IN "result";
COMMIT;


-- 16) Determine the peers who left the campus more than M times during the last N days
CREATE OR REPLACE PROCEDURE left_campus_peers(IN result refcursor, N_days integer, M_times integer)
LANGUAGE plpgsql
AS $enter_campus_on_time_peers$
BEGIN
    OPEN result FOR
    WITH cte_peer_exits AS (
        SELECT peer, count(*) AS exits_count
        FROM timetracking
        WHERE date >= now()::date - N_days AND state=2
        GROUP BY peer
    )
    SELECT peer
    FROM cte_peer_exits
    WHERE exits_count > M_times;
END;
$enter_campus_on_time_peers$;


BEGIN;
CALL left_campus_peers('result', 100, 1);
FETCH ALL IN "result";
COMMIT;

-- COPY XP                FROM '/tmp/school21_SQLInfo/xp.csv'                DELIMITER ',' CSV HEADER;
-- COPY Tasks             FROM '/tmp/school21_SQLInfo/tasks.csv'             DELIMITER ',' CSV HEADER;
--
-- TRUNCATE TABLE checks CASCADE;
-- COPY checks FROM '/tmp/school21_SQLInfo/checks.csv' DELIMITER ',' CSV HEADER;
-- select * from checks;
--
-- TRUNCATE TABLE p2p CASCADE;
-- COPY p2p FROM '/tmp/school21_SQLInfo/p2p.csv' DELIMITER ',' CSV HEADER;
-- select * from p2p;
--
-- TRUNCATE TABLE verter CASCADE;
-- COPY verter FROM '/tmp/school21_SQLInfo/verter.csv' DELIMITER ',' CSV HEADER;
-- select * from verter;
--
-- TRUNCATE TABLE xp CASCADE;
-- COPY xp FROM '/tmp/school21_SQLInfo/xp.csv' DELIMITER ',' CSV HEADER;
-- select * from xp;
--
-- TRUNCATE TABLE timetracking CASCADE;
-- COPY timetracking FROM '/tmp/school21_SQLInfo/timetracking.csv' DELIMITER ',' CSV HEADER;
-- select * from timetracking;

--17) Determine for each month the percentage of early entries

DROP PROCEDURE IF EXISTS early_entries_percentage CASCADE;

CREATE OR REPLACE PROCEDURE early_entries_percentage(IN result refcursor) AS $$
BEGIN
    OPEN result FOR
        WITH month_birthdays AS (
            SELECT
                EXTRACT(MONTH FROM Birthday) AS Month,
                Nickname
            FROM Peers
        ),
        month_entries AS (
            SELECT
                EXTRACT(MONTH FROM tt.Date) AS Month,
                COUNT(*) AS TotalEntries,
                COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM tt.Time) < 12) AS EarlyEntries
            FROM TimeTracking tt
            JOIN month_birthdays mb ON tt.Peer = mb.Nickname
            GROUP BY EXTRACT(MONTH FROM tt.Date)
        )
        SELECT
            TO_CHAR(TO_DATE(CAST(Month AS TEXT), 'MM'), 'Month') AS Month,
            ROUND((EarlyEntries * 100.0) / TotalEntries, 2) AS EarlyEntries
        FROM month_entries
        ORDER BY Month;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL early_entries_percentage('result' );
FETCH ALL IN "result" ;
END;
