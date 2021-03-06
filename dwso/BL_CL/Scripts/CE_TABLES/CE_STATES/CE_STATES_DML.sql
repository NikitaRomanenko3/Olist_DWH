INSERT INTO BL_3NF.CE_STATES 
(
    STATE_SRCID,
    SOURCE_SYSTEM,
    SOURCE_TABLE,
    STATE_NAME,
    STATE_COUNTRY_ID
)

WITH TEMP_Q AS 
(
    SELECT 
        MAX(INSERT_DT) AS MAX_DATE
    FROM BL_3NF.CE_STATES 
)
SELECT DISTINCT
    STATE_ID,
    'SA_SOURCE_SYSTEM_RETAIL',
    'SA_OLIST_GEO',
    STATE_NAME,
    COUNTRY_ID
FROM SA_SOURCE_SYSTEM_RETAIL.SA_OLIST_GEO
WHERE INSERT_DATE > (SELECT MAX_DATE FROM TEMP_Q)
    AND STATE_ID NOT IN (
                            SELECT
                                STATE_SRCID
                            FROM BL_3NF.CE_STATES
                        )
;

COMMIT;