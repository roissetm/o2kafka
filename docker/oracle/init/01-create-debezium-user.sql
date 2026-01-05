-- ============================================
-- Create Debezium CDC User with LogMiner Privileges
-- ============================================
-- This script creates the debezium user and grants
-- all necessary privileges for CDC with LogMiner

ALTER SESSION SET CONTAINER = XEPDB1;

-- Create Debezium user
CREATE USER debezium IDENTIFIED BY dbz_password
    DEFAULT TABLESPACE users
    QUOTA UNLIMITED ON users;

-- Basic session privileges
GRANT CREATE SESSION TO debezium;
GRANT SET CONTAINER TO debezium;

-- LogMiner required privileges
GRANT SELECT ON V_$DATABASE TO debezium;
GRANT SELECT ON V_$LOG TO debezium;
GRANT SELECT ON V_$LOG_HISTORY TO debezium;
GRANT SELECT ON V_$LOGMNR_LOGS TO debezium;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO debezium;
GRANT SELECT ON V_$LOGFILE TO debezium;
GRANT SELECT ON V_$ARCHIVED_LOG TO debezium;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO debezium;
GRANT SELECT ON V_$TRANSACTION TO debezium;

-- Catalog access
GRANT SELECT_CATALOG_ROLE TO debezium;
GRANT EXECUTE_CATALOG_ROLE TO debezium;

-- Table access for CDC
GRANT SELECT ANY TABLE TO debezium;
GRANT FLASHBACK ANY TABLE TO debezium;
GRANT SELECT ANY TRANSACTION TO debezium;

-- LogMiner execution
GRANT LOGMINING TO debezium;
GRANT EXECUTE ON DBMS_LOGMNR TO debezium;
GRANT EXECUTE ON DBMS_LOGMNR_D TO debezium;

-- For schema history storage (optional, if storing in Oracle)
GRANT CREATE TABLE TO debezium;
GRANT CREATE SEQUENCE TO debezium;
GRANT ALTER ANY TABLE TO debezium;

COMMIT;
