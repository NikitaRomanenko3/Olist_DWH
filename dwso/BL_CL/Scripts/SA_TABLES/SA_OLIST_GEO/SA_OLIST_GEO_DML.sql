INSERT INTO SA_SOURCE_SYSTEM_RETAIL.SA_OLIST_GEO 
(
    CITY_ID,
    CITY_NAME,
    STATE_ID,
    STATE_NAME,
    COUNTRY_ID,
    COUNTRY_NAME
)
SELECT
    TO_NUMBER(CITY_ID),
    CITY_NAME,
    TO_NUMBER(STATE_ID),
    STATE_NAME,
    TO_NUMBER(COUNTRY_ID),
    COUNTRY_NAME  
FROM SA_SOURCE_SYSTEM_RETAIL.EXT_OLIST_GEO;

COMMIT;