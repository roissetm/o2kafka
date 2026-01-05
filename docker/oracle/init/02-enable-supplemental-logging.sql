-- ============================================
-- Enable Supplemental Logging for CDC
-- ============================================
-- Supplemental logging is required for Debezium to capture
-- all column values in change events

-- Connect to CDB root to enable database-level supplemental logging
ALTER SESSION SET CONTAINER = CDB$ROOT;

-- Enable minimal supplemental logging at database level
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- Switch to PDB
ALTER SESSION SET CONTAINER = XEPDB1;

-- Enable supplemental logging for all columns
-- This ensures all column values are captured in redo logs
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- Verify supplemental logging is enabled
SELECT SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_ALL
FROM V$DATABASE;

COMMIT;
