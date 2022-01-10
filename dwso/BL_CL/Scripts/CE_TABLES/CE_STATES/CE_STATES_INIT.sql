-- inserting row which contains only default values
INSERT INTO BL_3NF.CE_STATES 
(
    STATE_ID,
    STATE_SRCID,
    SOURCE_SYSTEM,
    SOURCE_TABLE,
    STATE_NAME,
    STATE_COUNTRY_ID,
    INSERT_DT,
    UPDATE_DT
)
VALUES (-1, -99, 'N/A', 'N/A', 'N/A', -1, TO_DATE('1900-01-01', 'YYYY-MM-DD'), 
TO_DATE('1900-01-01', 'YYYY-MM-DD'));

COMMIT;