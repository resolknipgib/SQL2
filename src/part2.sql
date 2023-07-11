-- 1. A PROCEDURE FOR ADDING P2P CHECK
CREATE OR REPLACE PROCEDURE pc_adding_P2P_check(checked_peer   VARCHAR,
                                                checking_peer  VARCHAR,
                                                task_title     VARCHAR,
                                                check_status   CheckStatus,
                                                time_          TIMESTAMP)
LANGUAGE plpgsql
AS $adding_P2P_check$
BEGIN
    IF checked_peer = checking_peer THEN
       RAISE EXCEPTION 'Checking and checked peer have the same nickname %.', checked_peer;
    END IF;
    IF check_status = 'Start' THEN
       IF time_::DATE != CURRENT_DATE THEN
          RAISE EXCEPTION 'P2P and check always happen on the same day, % != %.', time_::Date, CURRENT_DATE;
       END IF;
       INSERT INTO Checks(ID, Peer, Task, Date)
              VALUES ((SELECT max(id) + 1 FROM Checks), checked_peer, task_title, CURRENT_DATE);
       INSERT INTO P2P(ID, CheckID, CheckingPeer, State, Time)
              VALUES ((SELECT max(id) + 1 FROM P2P), (SELECT max(id) FROM Checks), checking_peer, check_status, time_);
    ELSE
         IF time_::date != (SELECT date FROM checks WHERE id IN (SELECT CheckID FROM p2p
                                                                 WHERE CheckingPeer = checking_peer
                                                                       AND State = 'Start')) THEN
            RAISE EXCEPTION 'P2P and check always happen on the same day.';
         END IF;
         INSERT INTO P2P(ID, CheckID, CheckingPeer, State, Time)
                VALUES ((SELECT max(id) + 1 FROM P2P),
                        (SELECT id FROM checks
                          WHERE Task = task_title
                                AND Peer = checked_peer
                                AND (checks.id IN (SELECT CheckID FROM p2p
                                                    WHERE CheckingPeer = checking_peer
                                                          AND State = 'Start'))
                                AND (checks.id NOT IN (SELECT CheckID FROM p2p
                                                        WHERE CheckingPeer = checking_peer
                                                              AND State IN ('Success','Failure')))
                        ),
                       checking_peer, check_status, time_);
    END IF;
END $adding_P2P_check$;

-- TEST QUERIES/CALLS
-- OK
CALL pc_adding_P2P_check('eclipse',
                         'storm',
                         'DO5_SimpleDocker',
                         'Start',
                         CURRENT_TIMESTAMP::TIMESTAMP);
-- OK
CALL pc_adding_P2P_check('orbit',
                         'ltalia',
                         'CPP4_3DViewer_v2.0',
                         'Success',
                         '2023-01-09 10:30:00.000000');
-- OK
CALL pc_adding_P2P_check('midnight',
                         'titan',
                         'A8_Algorithmic_trading',
                         'Start',
                         CURRENT_TIMESTAMP::TIMESTAMP);
-- OK
CALL pc_adding_P2P_check('midnight',
                         'titan',
                         'A8_Algorithmic_trading',
                         'Failure',
                         CURRENT_TIMESTAMP::TIMESTAMP + (20 ||' minutes')::INTERVAL);
-- ERROR: Checking and checked peer have the same nickname zephyr
CALL pc_adding_P2P_check('zephyr',
                         'zephyr',
                         'CPP3_SmartCalc_v2.0',
                         'Start',
                         CURRENT_TIMESTAMP::TIMESTAMP);
-- ERROR: duplicate key value violates unique constraint "uq_check"
CALL pc_adding_P2P_check('eclipse',
                         'storm',
                         'DO5_SimpleDocker',
                         'Start',
                         CURRENT_TIMESTAMP::TIMESTAMP);
-- ERROR: insert or update on table "checks" violates foreign key constraint "fk_peers_nickname"
CALL pc_adding_P2P_check('aboba',
                         'pukich',
                         'DO5_SimpleDocker',
                         'Start',
                         CURRENT_TIMESTAMP::TIMESTAMP);
-- ERROR: insert or update on table "checks" violates foreign key constraint "fk_tasks_title"
CALL pc_adding_P2P_check('nebula',
                         'lunar',
                         'Minishell',
                         'Start',
                         CURRENT_TIMESTAMP::TIMESTAMP);
-- ERROR: P2P and check always happen on the same day, 2023-01-12 != 2023-02-04
CALL pc_adding_P2P_check('nebula',
                         'lunar',
                         'CPP1_s21_matrix+',
                         'Start',
                         '2023-01-12 11:00:00.000000');
-- ERROR: null value in column "CheckID" of relation "p2p" violates not-null constraint
CALL pc_adding_P2P_check('eclipse',
                         'storm',
                         'C4_s21_math',
                         'Failure',
                         '2023-02-04 11:00:00.000000');
-- ERROR: P2P and check always happen on the same day
CALL pc_adding_P2P_check('eclipse',
                         'storm',
                         'DO5_SimpleDocker',
                         'Failure',
                         '2023-01-12 11:00:00.000000');
-- ERROR: null value in column "CheckID" of relation "p2p" violates not-null constraint
CALL pc_adding_P2P_check('midnight',
                         'titan',
                         'A8_Algorithmic_trading',
                         'Success',
                         CURRENT_TIMESTAMP::TIMESTAMP + (20 ||' minutes')::INTERVAL);

-- 2. A PROCEDURE FOR ADDING VERTER CHECK PROCEDURE
CREATE OR REPLACE PROCEDURE pc_adding_Verter_check(checked_nickname VARCHAR,
                                                   task_name        VARCHAR,
                                                   check_status     CheckStatus,
                                                   time_            TIMESTAMP)
LANGUAGE plpgsql
AS $pc_adding_Verter_check$
BEGIN
    IF (SELECT CheckID
          FROM P2P
               JOIN Checks ON P2P.CheckID = Checks.id
         WHERE Task = task_name AND Peer = checked_nickname AND P2P.state = 'Success') IS NULL THEN
       RAISE EXCEPTION 'The evaluation of the project by the Verter cannot be put down without a record of the successful p2p verification.';
    END IF;
    IF (check_status = 'Success' OR check_status = 'Failure') AND (SELECT CheckID
                                                                     FROM P2P
                                                                          JOIN Checks ON P2P.CheckID = Checks.id
                                                                    WHERE Task = task_name AND Peer = checked_nickname AND P2P.state = 'Start'
                                                                        ) IS NULL THEN
       RAISE EXCEPTION 'The evaluation of the project by the Verter cannot be put down without a record of the beginning of the check.';
    END IF;
    INSERT INTO Verter(ID, CheckID, State, Time)
           VALUES ((SELECT max(id) + 1 FROM Verter),
                   (SELECT CheckID FROM P2P JOIN Checks ON P2P.CheckID = Checks.id
                                   WHERE Task = task_name AND Peer = checked_nickname AND P2P.state = 'Success'
                     ORDER BY P2P.Time DESC, P2P.ID DESC
                     LIMIT 1),
                   check_status, time_);
END $pc_adding_Verter_check$;

-- TEST QUERIES/CALLS
-- OK
CALL pc_adding_Verter_check('storm', 'C2_SimpleBashUtils', 'Success', '2023-10-01 21:30:00');
-- OK
CALL pc_adding_Verter_check('fabet', 'CPP9_MonitoringSystem', 'Start', '2023-01-10 23:00:00');
-- OK
CALL pc_adding_Verter_check('fabet', 'CPP9_MonitoringSystem', 'Success', '2023-01-10 23:30:00');
-- ERROR: the evaluation of the project by the Verter cannot be put down without a record of the beginning of the check
CALL pc_adding_Verter_check('midnight', 'A8_Algorithmic_trading', 'Success', '2023-03-22 23:30:00');
-- ERROR: the evaluation of the project by the Verter cannot be put down without a record of the successful p2p verification
CALL pc_adding_Verter_check('xenon', 'SQL1', 'Start', '2023-01-09 20:30:00');



-- 3. A TRIGGER TO CHANGE THE CORRESPONDING RECORD IN THE TRANSFERREDPOINTS TABLE
CREATE OR REPLACE FUNCTION fnc_transferred_points_adding()
          RETURNS TRIGGER
LANGUAGE plpgsql
AS $transferred_points_adding$
    DECLARE record_count     NUMERIC;
    DECLARE checked_nickname VARCHAR;
    BEGIN
        SELECT Peer INTO checked_nickname FROM Checks JOIN P2P ON Checks.id = P2P.CheckID WHERE Checks.id=NEW.CheckID;
        SELECT count(PointsAmount) INTO record_count FROM TransferredPoints
         WHERE CheckingPeer = NEW.CheckingPeer
               AND CheckedPeer = checked_nickname;
        IF record_count = 0 THEN
            INSERT INTO TransferredPoints(CheckingPeer, CheckedPeer, PointsAmount)
            VALUES (NEW.CheckingPeer, checked_nickname, 1);
        ELSE
            UPDATE TransferredPoints SET PointsAmount=PointsAmount+1
             WHERE CheckingPeer=NEW.CheckingPeer AND
                                            CheckedPeer=checked_nickname;
        END IF;
        RETURN NEW;
    END
$transferred_points_adding$;

DROP TRIGGER IF EXISTS tr_transferred_points_adding ON P2P;
CREATE TRIGGER tr_transferred_points_adding
         AFTER INSERT ON P2P FOR EACH ROW
          WHEN (NEW.State='Start')
       EXECUTE FUNCTION fnc_transferred_points_adding();

-- TEST QUERIES/CALLS
-- OK
CALL pc_adding_P2P_check('shadowlord','kytropu','A5_s21_memory','Start',CURRENT_TIMESTAMP::TIMESTAMP);
SELECT * FROM transferredpoints;
-- добавилась запись в transferredpoints: 15,kytropu,shadowlord,1

-- OK
CALL pc_adding_P2P_check('shadowlord','kytropu','DO1_Linux','Start',CURRENT_TIMESTAMP::TIMESTAMP);
SELECT * FROM transferredpoints;
-- значение pointsamount увеличилось на 1: 15,kytropu,shadowlord,2

-- OK
CALL pc_adding_P2P_check('vex','heboni','DO1_Linux','Start',CURRENT_TIMESTAMP::TIMESTAMP);
SELECT * FROM transferredpoints;
-- добавилась запись в transferredpoints: 16,heboni,vex,1


-- 4. A TRIGGER TO CHECK IF ADDING A RECORD TO THE XP TABLE IS CORRECT
CREATE OR REPLACE FUNCTION fnc_xp_insert_check()
          RETURNS TRIGGER
LANGUAGE plpgsql
AS $xp_insert_check$
    BEGIN
        IF (SELECT count(*) FROM XP WHERE checkid = NEW.checkid) > 0 THEN
            RAISE EXCEPTION 'There is already the record for this check';
        END IF;
        IF (SELECT maxxp FROM tasks JOIN checks ON tasks.title = checks.Task
                        WHERE checks.id=NEW.CheckID) < NEW.xp THEN
            RAISE EXCEPTION 'XP does exceed the maximum available for the task being checked';
        END IF;
        IF 'Success' NOT IN (SELECT State FROM p2p WHERE CheckID=NEW.CheckID)
                            OR ('Start' IN (SELECT State FROM verter WHERE CheckID=NEW.CheckID)
                            AND 'Success' NOT IN (SELECT State FROM verter WHERE CheckID=NEW.CheckID)) THEN
           RAISE EXCEPTION 'The record refers to the unsuccessful check';
        END IF;
        RETURN NEW;
    END
$xp_insert_check$;

DROP TRIGGER IF EXISTS tr_xp_insert_check ON XP;
CREATE TRIGGER tr_xp_insert_check
        BEFORE INSERT OR UPDATE ON XP FOR EACH ROW
       EXECUTE FUNCTION fnc_xp_insert_check();

-- TEST QUERIES/CALLS
-- ERROR: XP does exceed the maximum available for the task being checked
INSERT INTO XP (CheckID, XP) VALUES (2, 400);

-- ERROR: There is already the record for this check
INSERT INTO XP (CheckID, XP) VALUES (2, 300);

-- OK
DELETE FROM XP WHERE checkid = 2;
INSERT INTO XP (CheckID, XP) VALUES (2, 300);
SELECT * FROM XP;

-- ERROR: the record refers to the unsuccessful check
insert into XP(CheckID, xp) values (19, 300);
