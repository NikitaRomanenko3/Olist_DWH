-- inserting row which contains only default values
INSERT INTO BL_3NF.CE_CITIES 
(
    CITY_ID,
    CITY_SRCID,
    SOURCE_SYSTEM,
    SOURCE_TABLE,
    CITY_NAME,
    CITY_STATE_ID,
    INSERT_DT,
    UPDATE_DT
)
VALUES (-1, -99, 'N/A', 'N/A', 'N/A', -1, TO_DATE('1900-01-01', 'YYYY-MM-DD'), 
TO_DATE('1900-01-01', 'YYYY-MM-DD'));

COMMIT;