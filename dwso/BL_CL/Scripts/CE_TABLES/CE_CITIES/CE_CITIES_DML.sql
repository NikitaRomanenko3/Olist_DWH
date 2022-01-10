INSERT INTO BL_3NF.CE_CITIES 
(
    CITY_SRCID,
    SOURCE_SYSTEM,
    SOURCE_TABLE,
    CITY_NAME,
    CITY_STATE_ID
)

WITH TEMP_Q AS 
(
    SELECT 
        MAX(INSERT_DT) AS MAX_DATE
    FROM BL_3NF.CE_CITIES 
)
SELECT DISTINCT
    CITY_ID,
    'SA_SOURCE_SYSTEM_RETAIL',
    'SA_OLIST_GEO',
    CITY_NAME,
    STATE_ID
FROM SA_SOURCE_SYSTEM_RETAIL.SA_OLIST_GEO
WHERE INSERT_DATE > (SELECT MAX_DATE FROM TEMP_Q)
    AND CITY_ID NOT IN (
                            SELECT 
                                CITY_SRCID
                            FROM BL_3NF.CE_CITIES
                        )
;

COMMIT;