-- SCRIPT THAT CREATES THE DATABASE AND ALL THE TABLES
CREATE DATABASE Info21;

DROP TABLE IF EXISTS Peers CASCADE;
CREATE TABLE IF NOT EXISTS Peers
(
    Nickname    VARCHAR PRIMARY KEY,
    Birthday    DATE    NOT NULL,

    CONSTRAINT  uq_nickname UNIQUE (Nickname)
);

DROP TABLE IF EXISTS Tasks CASCADE;
CREATE TABLE IF NOT EXISTS Tasks
(
    Title       VARCHAR PRIMARY KEY,
    ParentTask  VARCHAR,
    MaxXP       BIGINT NOT NULL,

    CONSTRAINT  ch_positive_xp CHECK (MaxXP >= 0),
    CONSTRAINT  fk_tasks_title FOREIGN KEY (ParentTask) REFERENCES Tasks(Title)
);

DROP TYPE IF EXISTS CheckStatus CASCADE;
CREATE TYPE CheckStatus AS ENUM ('Start', 'Success', 'Failure');

DROP TABLE IF EXISTS Checks CASCADE;
CREATE TABLE IF NOT EXISTS Checks
(
    ID      SERIAL  PRIMARY KEY,
    Peer    VARCHAR NOT NULL,
    Task    VARCHAR NOT NULL,
    Date    DATE,

    CONSTRAINT      fk_peers_nickname FOREIGN KEY (Peer) REFERENCES Peers(Nickname),
    CONSTRAINT      fk_tasks_title    FOREIGN KEY (Task) REFERENCES Tasks(Title),
    CONSTRAINT      uq_check          UNIQUE      (Peer, Task, Date)

);

DROP TABLE IF EXISTS P2P CASCADE;
CREATE TABLE IF NOT EXISTS P2P
(
    ID              SERIAL  PRIMARY KEY,
    CheckID         BIGINT  NOT NULL,
    CheckingPeer    VARCHAR NOT NULL,
    State           CheckStatus,
    Time            TIMESTAMP,

    CONSTRAINT      fk_check_id       FOREIGN KEY (CheckID)      REFERENCES Checks(ID),
    CONSTRAINT      fk_peers_nickname FOREIGN KEY (CheckingPeer) REFERENCES Peers(Nickname),
    CONSTRAINT      uq_p2p            UNIQUE      (CheckID, CheckingPeer, State, Time)
);

DROP TABLE IF EXISTS Verter CASCADE;
CREATE TABLE IF NOT EXISTS Verter
(
    ID          SERIAL PRIMARY KEY,
    CheckID     BIGINT NOT NULL,
    State       CheckStatus,
    Time        TIMESTAMP,

    CONSTRAINT  fk_checks_id FOREIGN KEY (CheckID) REFERENCES Checks(ID)
);

DROP TABLE IF EXISTS TransferredPoints CASCADE;
CREATE TABLE IF NOT EXISTS TransferredPoints
(
    ID              SERIAL  PRIMARY KEY,
    CheckingPeer    VARCHAR NOT NULL,
    CheckedPeer     VARCHAR NOT NULL,
    PointsAmount    BIGINT  NOT NULL DEFAULT 1,

    CONSTRAINT      fk_peers_nickname1  FOREIGN KEY (CheckingPeer) REFERENCES Peers(Nickname),
    CONSTRAINT      fk_peers_nickname2  FOREIGN KEY (CheckedPeer)  REFERENCES Peers(Nickname),
    CONSTRAINT      ch_nicknames        CHECK       (CheckingPeer != CheckedPeer),
    CONSTRAINT      uq_peers_pair       UNIQUE      (CheckingPeer, CheckedPeer)
);

DROP TABLE IF EXISTS Friends CASCADE;
CREATE TABLE IF NOT EXISTS Friends
(
    ID      SERIAL  PRIMARY KEY,
    Peer1   VARCHAR NOT NULL,
    Peer2   VARCHAR NOT NULL,

    CONSTRAINT  fk_peers_nickname1  FOREIGN KEY (Peer1) REFERENCES Peers(Nickname),
    CONSTRAINT  fk_peers_nickname2  FOREIGN KEY (Peer2) REFERENCES Peers(Nickname),
    CONSTRAINT  ch_nicknames        CHECK       (Peer1 != Peer2),
    CONSTRAINT  uq_peers_friends    UNIQUE      (Peer1, Peer2)
);

DROP TABLE IF EXISTS Recommendations CASCADE;
CREATE TABLE IF NOT EXISTS Recommendations
(
    ID              SERIAL  PRIMARY KEY,
    Peer            VARCHAR NOT NULL,
    RecommendedPeer VARCHAR NOT NULL,

    CONSTRAINT  fk_peers_nickname1 FOREIGN KEY (Peer)            REFERENCES Peers(Nickname),
    CONSTRAINT  fk_peers_nickname2 FOREIGN KEY (RecommendedPeer) REFERENCES Peers(Nickname),
    CONSTRAINT  ch_nicknames       CHECK       (Peer != RecommendedPeer)
);

DROP TABLE IF EXISTS XP CASCADE;
CREATE TABLE IF NOT EXISTS XP
(
    ID          SERIAL PRIMARY KEY,
    CheckID     BIGINT NOT NULL,
    XP          BIGINT NOT NULL,

    CONSTRAINT  fk_checks_id FOREIGN KEY (CheckID) REFERENCES Checks(id)
);

DROP TABLE IF EXISTS TimeTracking CASCADE;
CREATE TABLE IF NOT EXISTS TimeTracking
(
    ID      SERIAL  PRIMARY KEY,
    Peer    VARCHAR NOT NULL,
    Date    DATE    NOT NULL,
    Time    Time    NOT NULL,
    State   BIGINT  NOT NULL,

    CONSTRAINT fk_peers_nickname FOREIGN KEY (Peer) REFERENCES Peers(Nickname),
    CONSTRAINT ch_state          CHECK (State IN (1, 2))
);

-- PROCEDURES THAT ALLOW TO IMPORT AND EXPORT DATA FOR EACH TABLE FROM/TO A CSV-FILE
CREATE OR REPLACE PROCEDURE pc_import_peers_from_csv()
LANGUAGE plpgsql
AS $import_peers_from_csv$
BEGIN
    COPY Peers FROM '/Users/fabet/Desktop/SQL2_Info21_v1.0-0/materials/import/peers.csv' DELIMITER ',' CSV HEADER;
END;
$import_peers_from_csv$;

CALL pc_import_peers_from_csv();

CREATE OR REPLACE PROCEDURE pc_export_to_csv_from_peers()
LANGUAGE plpgsql
AS $export_to_csv_from_peers$
BEGIN
    COPY Peers TO '/Users/fabet/Desktop/SQL2_Info21_v1.0-0/materials/export.csv' DELIMITER ',' CSV HEADER;
END;
$export_to_csv_from_peers$;

CALL pc_export_to_csv_from_peers();

CREATE OR REPLACE PROCEDURE pc_import_to_table_from_csv()
LANGUAGE plpgsql
AS $import_to_table_from_csv$
BEGIN
    COPY :table_name FROM :csv_path DELIMITER :delim CSV HEADER;
END;
$import_to_table_from_csv$;

CALL pc_import_to_table_from_csv();

CREATE OR REPLACE PROCEDURE pc_export_to_csv_from_table()
LANGUAGE plpgsql
AS $export_to_csv_from_table$
BEGIN
    COPY :table_name TO :new_csv_path DELIMITER :delim CSV HEADER;
END;
$export_to_csv_from_table$;

CALL pc_export_to_csv_from_table();

-- SUPPORT PROCEDURES
CREATE OR REPLACE PROCEDURE pc_import_to_table_from_csv()
LANGUAGE plpgsql
AS $import_to_table_from_csv$
BEGIN
    COPY Peers             FROM '/Users/fabet/Desktop/SQL2_Info21_v1.0-0/materials/import/peers.csv'             DELIMITER ',' CSV HEADER;
    COPY Tasks             FROM '/Users/fabet/Desktop/SQL2_Info21_v1.0-0/materials/import/tasks.csv'             DELIMITER ',' CSV HEADER;
    COPY Checks            FROM '/Users/fabet/Desktop/SQL2_Info21_v1.0-0/materials/import/checks.csv'            DELIMITER ',' CSV HEADER;
    COPY P2P               FROM '/Users/fabet/Desktop/SQL2_Info21_v1.0-0/materials/import/p2p.csv'               DELIMITER ',' CSV HEADER;
    COPY Verter            FROM '/Users/fabet/Desktop/SQL2_Info21_v1.0-0/materials/import/verter.csv'            DELIMITER ',' CSV HEADER;
    COPY TransferredPoints FROM '/Users/fabet/Desktop/SQL2_Info21_v1.0-0/materials/import/transferredpoints.csv' DELIMITER ',' CSV HEADER;
    COPY Friends           FROM '/Users/fabet/Desktop/SQL2_Info21_v1.0-0/materials/import/friends.csv'           DELIMITER ',' CSV HEADER;
    COPY Recommendations   FROM '/Users/fabet/Desktop/SQL2_Info21_v1.0-0/materials/import/recommendations.csv'   DELIMITER ',' CSV HEADER;
    COPY XP                FROM '/Users/fabet/Desktop/SQL2_Info21_v1.0-0/materials/import/xp.csv'                DELIMITER ',' CSV HEADER;
    COPY TimeTracking      FROM '/Users/fabet/Desktop/SQL2_Info21_v1.0-0/materials/import/timetracking.csv'      DELIMITER ',' CSV HEADER;
END;
$import_to_table_from_csv$;

CALL pc_import_to_table_from_csv();

CREATE OR REPLACE PROCEDURE pc_truncate_all_tables()
LANGUAGE plpgsql
AS $truncate_all_tables$
BEGIN
    TRUNCATE Peers CASCADE;
    TRUNCATE Tasks CASCADE;
    TRUNCATE Checks CASCADE;
    TRUNCATE P2P CASCADE;
    TRUNCATE Verter CASCADE;
    TRUNCATE TransferredPoints CASCADE;
    TRUNCATE Friends CASCADE;
    TRUNCATE Recommendations CASCADE;
    TRUNCATE XP CASCADE;
    TRUNCATE TimeTracking CASCADE;
END;
$truncate_all_tables$;

CALL pc_truncate_all_tables();

