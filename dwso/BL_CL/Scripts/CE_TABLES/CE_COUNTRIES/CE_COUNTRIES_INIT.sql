-- inserting row which contains only default values
INSERT INTO BL_3NF.CE_COUNTRIES 
(
    COUNTRY_ID,
    COUNTRY_SRCID,
    SOURCE_SYSTEM,
    SOURCE_TABLE,
    COUNTRY_NAME,
    INSERT_DT,
    UPDATE_DT
)
VALUES (-1, -99, 'N/A', 'N/A', 'N/A', TO_DATE('1900-01-01', 'YYYY-MM-DD'), TO_DATE('1900-01-01', 'YYYY-MM-DD'));

COMMIT;