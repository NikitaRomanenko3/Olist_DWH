--ALTER SESSION SET NLS_DATE_LANGUAGE = ENGLISH;
--ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD';

CREATE OR REPLACE PACKAGE BODY PKG_ETL_DIM AS 

--------------------------------------------------------------------------------
PROCEDURE LD_DIM_DATES
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN

    DELETE FROM BL_DM.DIM_DATES;
    
    LOGGER(
        IN_LOG_ID => VAR_LOG_ID,
        IN_OPERATION_TYPE => 'DELETE',
        IN_TARGET_TABLE => 'DIM_DATES',
        IN_OPERATION_STATUS => 'FINISH'
        );
    
    EXECUTE IMMEDIATE q'{INSERT INTO BL_DM.DIM_DATES
SELECT 
    TO_DATE('01/01/1970', 'dd/mm/yyyy') + ROWNUM - 1 as EVENT_DT,
    TO_NUMBER(TO_CHAR(TO_DATE('01/01/1970', 'dd/mm/yyyy') + ROWNUM - 1, 'D')) AS DAY_OF_WEEK_NUMBER,
    TO_CHAR(TO_DATE('01/01/1970', 'dd/mm/yyyy') + ROWNUM - 1, 'DAY') AS DAY_OF_WEEK_NAME,
    EXTRACT(MONTH FROM TO_DATE('01/01/1970', 'dd/mm/yyyy') + ROWNUM - 1) AS MONTH_NUMBER,
    TO_CHAR(TO_DATE('01/01/1970', 'dd/mm/yyyy') + ROWNUM - 1, 'MONTH') AS MONTH_NAME,
     TO_NUMBER(TO_CHAR(TO_DATE('01/01/1970', 'dd/mm/yyyy') + ROWNUM - 1, 'Q')) AS QUARTER_NUMBER,
    EXTRACT(YEAR FROM TO_DATE('01/01/1970', 'dd/mm/yyyy') + ROWNUM - 1) AS YEAR_NUMBER
FROM dual
CONNECT BY LEVEL <= TO_DATE('01/01/2030', 'dd/mm/yyyy') - TO_DATE('01/01/1970', 'dd/mm/yyyy') + 1}';
    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'CREATE TABLE',
            IN_TARGET_TABLE => 'DIM_DATES',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'DIM_DATES',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;
--------------------------------------------------------------------------------
PROCEDURE LD_DIM_GEOLOCATIONS
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'DIM_GEOLOCATIONS',
            IN_OPERATION_STATUS => 'BEGIN'
            );

    MERGE INTO DIM_GEOLOCATIONS D_GEO
        USING (
                SELECT
                    'BL_3NF' AS SOURCE_SYSTEM,
                    'CE_CITIES' AS SOURCE_TABLE,
                    CE_CT.CITY_ID AS GEOLOCATION_CITY_ID,
                    CE_CT.CITY_NAME AS GEOLOCATION_CITY_NAME,
                    CE_ST.STATE_ID AS GEOLOCATION_STATE_ID,
                    CE_ST.STATE_NAME AS GEOLOCATION_STATE_NAME,
                    CE_CTR.COUNTRY_ID AS GEOLOCATION_COUNTRY_ID,
                    CE_CTR.COUNTRY_NAME AS GEOLOCATION_COUNTRY_NAME
                FROM CE_CITIES CE_CT
                LEFT JOIN CE_STATES CE_ST ON CE_ST.STATE_ID = CE_CT.CITY_STATE_ID
                LEFT JOIN CE_COUNTRIES CE_CTR ON CE_CTR.COUNTRY_ID = CE_ST.STATE_COUNTRY_ID
                ) CE_GEO
        ON (D_GEO.GEOLOCATION_CITY_ID = CE_GEO.GEOLOCATION_CITY_ID AND 
            D_GEO.SOURCE_SYSTEM = CE_GEO.SOURCE_SYSTEM AND 
            D_GEO.SOURCE_TABLE = CE_GEO.SOURCE_TABLE)
    WHEN MATCHED THEN UPDATE SET
                        D_GEO.GEOLOCATION_CITY_NAME = CE_GEO.GEOLOCATION_CITY_NAME,    
                        D_GEO.GEOLOCATION_STATE_ID = CE_GEO.GEOLOCATION_STATE_ID,
                        D_GEO.GEOLOCATION_STATE_NAME = CE_GEO.GEOLOCATION_STATE_NAME,
                        D_GEO.GEOLOCATION_COUNTRY_ID = CE_GEO.GEOLOCATION_COUNTRY_ID,
                        D_GEO.GEOLOCATION_COUNTRY_NAME = CE_GEO.GEOLOCATION_COUNTRY_NAME,
                        D_GEO.UPDATE_DT = CURRENT_DATE
                            WHERE 
                        D_GEO.GEOLOCATION_CITY_NAME <> CE_GEO.GEOLOCATION_CITY_NAME OR    
                        D_GEO.GEOLOCATION_STATE_ID <> CE_GEO.GEOLOCATION_STATE_ID OR
                        D_GEO.GEOLOCATION_STATE_NAME <> CE_GEO.GEOLOCATION_STATE_NAME OR
                        D_GEO.GEOLOCATION_COUNTRY_ID <> CE_GEO.GEOLOCATION_COUNTRY_ID OR
                        D_GEO.GEOLOCATION_COUNTRY_NAME <> CE_GEO.GEOLOCATION_COUNTRY_NAME 

    WHEN NOT MATCHED THEN INSERT (
                                    D_GEO.GEOLOCATION_SURR_ID,
                                    D_GEO.SOURCE_SYSTEM,
                                    D_GEO.SOURCE_TABLE,
                                    D_GEO.GEOLOCATION_CITY_ID,
                                    D_GEO.GEOLOCATION_CITY_NAME,
                                    D_GEO.GEOLOCATION_STATE_ID,
                                    D_GEO.GEOLOCATION_STATE_NAME,
                                    D_GEO.GEOLOCATION_COUNTRY_ID,
                                    D_GEO.GEOLOCATION_COUNTRY_NAME,
                                    D_GEO.INSERT_DT,
                                    D_GEO.UPDATE_DT
                                    )
        VALUES (
                    SEQ_DIM_GEOLOCATIONS_SURR.NEXTVAL,
                    CE_GEO.SOURCE_SYSTEM,
                    CE_GEO.SOURCE_TABLE,
                    CE_GEO.GEOLOCATION_CITY_ID,
                    CE_GEO.GEOLOCATION_CITY_NAME,
                    CE_GEO.GEOLOCATION_STATE_ID,
                    CE_GEO.GEOLOCATION_STATE_NAME,
                    CE_GEO.GEOLOCATION_COUNTRY_ID,
                    CE_GEO.GEOLOCATION_COUNTRY_NAME,
                    CURRENT_DATE,
                    CURRENT_DATE
                );
    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'DIM_GEOLOCATIONS',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'DIM_GEOLOCATIONS',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;
--------------------------------------------------------------------------------
PROCEDURE LD_DIM_PAYMENT_TYPES
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'DIM_PAYMENT_TYPES',
            IN_OPERATION_STATUS => 'BEGIN'
            );

    MERGE INTO DIM_PAYMENT_TYPES DIM_PT
        USING (
                SELECT
                    PAYMENT_TYPE_ID,
                    'BL_3NF' AS SOURCE_SYSTEM,
                    'CE_PAYMENT_TYPES' AS SOURCE_TABLE,
                    PAYMENT_TYPE_NAME
                FROM CE_PAYMENT_TYPES
                ) CE_PT
        ON (DIM_PT.PAYMENT_TYPE_ID = CE_PT.PAYMENT_TYPE_ID AND
            DIM_PT.SOURCE_SYSTEM = CE_PT.SOURCE_SYSTEM AND
            DIM_PT.SOURCE_TABLE = CE_PT.SOURCE_TABLE)
    WHEN MATCHED THEN UPDATE SET
                        DIM_PT.PAYMENT_TYPE_NAME = CE_PT.PAYMENT_TYPE_NAME,
                        DIM_PT.UPDATE_DT = CURRENT_DATE
                            WHERE
                        DIM_PT.PAYMENT_TYPE_NAME <> CE_PT.PAYMENT_TYPE_NAME
                        
    WHEN NOT MATCHED THEN INSERT (
                                    DIM_PT.PAYMENT_TYPE_SURR_ID,
                                    DIM_PT.PAYMENT_TYPE_ID,
                                    DIM_PT.SOURCE_SYSTEM,
                                    DIM_PT.SOURCE_TABLE,
                                    DIM_PT.PAYMENT_TYPE_NAME,
                                    DIM_PT.INSERT_DT,
                                    DIM_PT.UPDATE_DT
                                    )
        VALUES (
                    SEQ_DIM_PAYMENT_TYPES_SURR.NEXTVAL,
                    CE_PT.PAYMENT_TYPE_ID,
                    CE_PT.SOURCE_SYSTEM,
                    CE_PT.SOURCE_TABLE,
                    CE_PT.PAYMENT_TYPE_NAME,
                    CURRENT_DATE,
                    CURRENT_DATE
                );
    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'DIM_PAYMENT_TYPES',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'DIM_PAYMENT_TYPES',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;

--------------------------------------------------------------------------------
PROCEDURE LD_DIM_SUPPLIERS
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'DIM_SUPPLIERS',
            IN_OPERATION_STATUS => 'BEGIN'
            );

    MERGE INTO DIM_SUPPLIERS DIM_SUPP
        USING (
            SELECT
            CE_SUPP.SUPPLIER_ID,
            'BL_3NF' AS SOURCE_SYSTEM,
            'CE_SUPPLIERS' AS SOURCE_TABLE,
            CE_SUPP.SUPPLIER_NAME,
            CE_SUPP.SUPPLIER_CITY_ID,
            NVL(CE_C.CITY_NAME, 'N/A') AS SUPPLIER_CITY_NAME,
            NVL(CE_S.STATE_ID, -1) AS SUPPLIER_STATE_ID,
            NVL(CE_S.STATE_NAME, 'N/A') AS SUPPLIER_STATE_NAME
            
        FROM CE_SUPPLIERS CE_SUPP
        LEFT JOIN CE_CITIES CE_C ON CE_C.CITY_ID = CE_SUPP.SUPPLIER_CITY_ID AND 
                CE_C.SOURCE_SYSTEM = CE_SUPP.SOURCE_SYSTEM
        LEFT JOIN CE_STATES CE_S ON CE_S.STATE_ID = CE_C.CITY_STATE_ID AND 
                  CE_S.SOURCE_SYSTEM = CE_C.SOURCE_SYSTEM
                ) CE_SUPP
        ON (DIM_SUPP.SUPPLIER_ID = CE_SUPP.SUPPLIER_ID AND 
            DIM_SUPP.SOURCE_SYSTEM = CE_SUPP.SOURCE_SYSTEM AND 
            DIM_SUPP.SOURCE_TABLE = CE_SUPP.SOURCE_TABLE)
    WHEN MATCHED THEN UPDATE SET
                        DIM_SUPP.SUPPLIER_NAME = CE_SUPP.SUPPLIER_NAME,
                        DIM_SUPP.SUPPLIER_CITY_ID = CE_SUPP.SUPPLIER_CITY_ID,
                        DIM_SUPP.SUPPLIER_CITY_NAME = CE_SUPP.SUPPLIER_CITY_NAME,
                        DIM_SUPP.SUPPLIER_STATE_ID = CE_SUPP.SUPPLIER_STATE_ID,
                        DIM_SUPP.SUPPLIER_STATE_NAME = CE_SUPP.SUPPLIER_STATE_NAME,
                        DIM_SUPP.UPDATE_DT = CURRENT_DATE
                            WHERE
                        DIM_SUPP.SUPPLIER_NAME <> CE_SUPP.SUPPLIER_NAME OR 
                        DIM_SUPP.SUPPLIER_CITY_ID <> CE_SUPP.SUPPLIER_CITY_ID OR
                        DIM_SUPP.SUPPLIER_CITY_NAME <> CE_SUPP.SUPPLIER_CITY_NAME OR
                        DIM_SUPP.SUPPLIER_STATE_ID <> CE_SUPP.SUPPLIER_STATE_ID OR
                        DIM_SUPP.SUPPLIER_STATE_NAME <> CE_SUPP.SUPPLIER_STATE_NAME
                        
    WHEN NOT MATCHED THEN INSERT (
                                    DIM_SUPP.SUPPLIER_SURR_ID,
                                    DIM_SUPP.SUPPLIER_ID,
                                    DIM_SUPP.SOURCE_SYSTEM,
                                    DIM_SUPP.SOURCE_TABLE,
                                    DIM_SUPP.SUPPLIER_NAME,
                                    DIM_SUPP.SUPPLIER_CITY_ID,
                                    DIM_SUPP.SUPPLIER_CITY_NAME,
                                    DIM_SUPP.SUPPLIER_STATE_ID,
                                    DIM_SUPP.SUPPLIER_STATE_NAME,
                                    DIM_SUPP.INSERT_DT,
                                    DIM_SUPP.UPDATE_DT
                                    )
        VALUES (
                    SEQ_DIM_SUPPLIERS_SURR.NEXTVAL,
                    CE_SUPP.SUPPLIER_ID,
                    CE_SUPP.SOURCE_SYSTEM,
                    CE_SUPP.SOURCE_TABLE,
                    CE_SUPP.SUPPLIER_NAME,
                    CE_SUPP.SUPPLIER_CITY_ID,
                    CE_SUPP.SUPPLIER_CITY_NAME,
                    CE_SUPP.SUPPLIER_STATE_ID,
                    CE_SUPP.SUPPLIER_STATE_NAME,
                    CURRENT_DATE,
                    CURRENT_DATE
                );
    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'DIM_SUPPLIERS',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'DIM_SUPPLIERS',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;

--------------------------------------------------------------------------------
PROCEDURE LD_DIM_CUSTOMERS
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'DIM_CUSTOMERS',
            IN_OPERATION_STATUS => 'BEGIN'
            );

    MERGE INTO DIM_CUSTOMERS DIM_CUST
        USING (
                SELECT
                    CE_CUST.CUSTOMER_ID,
                    'BL_3NF' AS SOURCE_SYSTEM,
                    'CE_CUSTOMERS' AS SOURCE_TABLE,
                    CE_CUST.CUSTOMER_FIRST_NAME,
                    CE_CUST.CUSTOMER_LAST_NAME,
                    CE_CUST.CUSTOMER_COMPANY_NAME,
                    CE_CUST.CUSTOMER_CITY_ID,
                    NVL(CE_C.CITY_NAME, 'N/A') AS CUSTOMER_CITY_NAME,
                    NVL(CE_S.STATE_ID, -1) AS CUSTOMER_STATE_ID,
                    NVL(CE_S.STATE_NAME, 'N/A') AS CUSTOMER_STATE_NAME,
                    IS_COMPANY
                FROM CE_CUSTOMERS CE_CUST
                LEFT JOIN CE_CITIES CE_C ON CE_C.CITY_ID = CE_CUST.CUSTOMER_CITY_ID AND 
                          CE_C.SOURCE_SYSTEM = CE_CUST.SOURCE_SYSTEM
                LEFT JOIN CE_STATES CE_S ON CE_S.STATE_ID = CE_C.CITY_STATE_ID AND 
                          CE_C.SOURCE_SYSTEM = CE_S.SOURCE_SYSTEM
                ) CE_CUST
        ON (DIM_CUST.CUSTOMER_ID = CE_CUST.CUSTOMER_ID AND 
            DIM_CUST.SOURCE_SYSTEM = CE_CUST.SOURCE_SYSTEM AND 
            DIM_CUST.SOURCE_TABLE = CE_CUST.SOURCE_TABLE)
    WHEN MATCHED THEN UPDATE SET
                        DIM_CUST.CUSTOMER_FIRST_NAME = CE_CUST.CUSTOMER_FIRST_NAME,
                        DIM_CUST.CUSTOMER_LAST_NAME = CE_CUST.CUSTOMER_LAST_NAME,
                        DIM_CUST.CUSTOMER_COMPANY_NAME = CE_CUST.CUSTOMER_COMPANY_NAME,
                        DIM_CUST.CUSTOMER_CITY_ID = CE_CUST.CUSTOMER_CITY_ID,
                        DIM_CUST.CUSTOMER_CITY_NAME = CE_CUST.CUSTOMER_CITY_NAME,
                        DIM_CUST.CUSTOMER_STATE_ID = CE_CUST.CUSTOMER_STATE_ID,
                        DIM_CUST.CUSTOMER_STATE_NAME = CE_CUST.CUSTOMER_STATE_NAME,
                        DIM_CUST.IS_COMPANY = CE_CUST.IS_COMPANY,
                        DIM_CUST.UPDATE_DT  = CURRENT_DATE
                            WHERE
                        DIM_CUST.CUSTOMER_FIRST_NAME <> CE_CUST.CUSTOMER_FIRST_NAME OR
                        DIM_CUST.CUSTOMER_LAST_NAME <> CE_CUST.CUSTOMER_LAST_NAME OR
                        DIM_CUST.CUSTOMER_COMPANY_NAME <> CE_CUST.CUSTOMER_COMPANY_NAME OR
                        DIM_CUST.CUSTOMER_CITY_ID <> CE_CUST.CUSTOMER_CITY_ID OR
                        DIM_CUST.CUSTOMER_CITY_NAME <> CE_CUST.CUSTOMER_CITY_NAME OR
                        DIM_CUST.CUSTOMER_STATE_ID <> CE_CUST.CUSTOMER_STATE_ID OR
                        DIM_CUST.CUSTOMER_STATE_NAME <> CE_CUST.CUSTOMER_STATE_NAME OR
                        DIM_CUST.IS_COMPANY <> CE_CUST.IS_COMPANY
                        
    WHEN NOT MATCHED THEN INSERT (
                                    DIM_CUST.CUSTOMER_SURR_ID,
                                    DIM_CUST.CUSTOMER_ID,
                                    DIM_CUST.SOURCE_SYSTEM,
                                    DIM_CUST.SOURCE_TABLE,
                                    DIM_CUST.CUSTOMER_FIRST_NAME,
                                    DIM_CUST.CUSTOMER_LAST_NAME,
                                    DIM_CUST.CUSTOMER_COMPANY_NAME,
                                    DIM_CUST.CUSTOMER_CITY_ID,
                                    DIM_CUST.CUSTOMER_CITY_NAME,
                                    DIM_CUST.CUSTOMER_STATE_ID,
                                    DIM_CUST.CUSTOMER_STATE_NAME,
                                    DIM_CUST.IS_COMPANY,
                                    DIM_CUST.INSERT_DT,
                                    DIM_CUST.UPDATE_DT
                                    )
        VALUES (
                    SEQ_DIM_CUSTOMERS_SURR.NEXTVAL,
                    CE_CUST.CUSTOMER_ID,
                    CE_CUST.SOURCE_SYSTEM,
                    CE_CUST.SOURCE_TABLE,
                    CE_CUST.CUSTOMER_FIRST_NAME,
                    CE_CUST.CUSTOMER_LAST_NAME,
                    CE_CUST.CUSTOMER_COMPANY_NAME,
                    CE_CUST.CUSTOMER_CITY_ID,
                    CE_CUST.CUSTOMER_CITY_NAME,
                    CE_CUST.CUSTOMER_STATE_ID,
                    CE_CUST.CUSTOMER_STATE_NAME,
                    CE_CUST.IS_COMPANY,
                    CURRENT_DATE,
                    CURRENT_DATE
                );
    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'DIM_CUSTOMERS',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'DIM_CUSTOMERS',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;
--------------------------------------------------------------------------------
PROCEDURE LD_DIM_LOGISTIC_PARTNERS
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'DIM_LOGISTIC_PARTNERS',
            IN_OPERATION_STATUS => 'BEGIN'
            );

    MERGE INTO DIM_LOGISTIC_PARTNERS DIM_LP
        USING (
                SELECT 
                    LP.LOGISTIC_PARTNER_ID AS LOGISTIC_PARTNERS_ID,
                    'BL_3NF' AS SOURCE_SYSTEM,
                    'CE_LOGISTIC_PARTNERS' AS SOURCE_TABLE,
                    LP.LOGISTIC_PARTNER_NAME AS LOGISTIC_PARTNERS_NAME,
                    NVL(CITY.CITY_ID, -1) AS LOGISTIC_PARTNERS_CITY_ID,
                    NVL(CITY.CITY_NAME, 'N/A') AS LOGISTIC_PARTNERS_CITY_NAME,
                    NVL(STATE_.STATE_ID, -1) AS LOGISTIC_PARTNERS_STATE_ID,
                    NVL(STATE_.STATE_NAME, 'N/A') AS LOGISTIC_PARTNERS_STATE_NAME
                    
                FROM CE_LOGISTIC_PARTNERS LP
                LEFT JOIN CE_CITIES CITY ON CITY.CITY_ID = LP.LOGISTIC_PARTNER_CITY_ID
                LEFT JOIN CE_STATES STATE_ ON STATE_.STATE_ID = CITY.CITY_STATE_ID
                ) CE_LP
        ON (DIM_LP.LOGISTIC_PARTNERS_ID = CE_LP.LOGISTIC_PARTNERS_ID AND 
            DIM_LP.SOURCE_SYSTEM = CE_LP.SOURCE_SYSTEM AND
            DIM_LP.SOURCE_TABLE = CE_LP.SOURCE_TABLE)
    WHEN MATCHED THEN UPDATE SET
                    DIM_LP.LOGISTIC_PARTNERS_NAME = CE_LP.LOGISTIC_PARTNERS_NAME,
                    DIM_LP.LOGISTIC_PARTNERS_CITY_ID = CE_LP.LOGISTIC_PARTNERS_CITY_ID,
                    DIM_LP.LOGISTIC_PARTNERS_CITY_NAME = CE_LP.LOGISTIC_PARTNERS_CITY_NAME,
                    DIM_LP.LOGISTIC_PARTNERS_STATE_ID = CE_LP.LOGISTIC_PARTNERS_STATE_ID,
                    DIM_LP.LOGISTIC_PARTNERS_STATE_NAME = CE_LP.LOGISTIC_PARTNERS_STATE_NAME,
                    DIM_LP.UPDATE_DT = CURRENT_DATE
                            WHERE
                    DIM_LP.LOGISTIC_PARTNERS_NAME <> CE_LP.LOGISTIC_PARTNERS_NAME OR
                    DIM_LP.LOGISTIC_PARTNERS_CITY_ID <> CE_LP.LOGISTIC_PARTNERS_CITY_ID OR
                    DIM_LP.LOGISTIC_PARTNERS_CITY_NAME <> CE_LP.LOGISTIC_PARTNERS_CITY_NAME OR
                    DIM_LP.LOGISTIC_PARTNERS_STATE_ID <> CE_LP.LOGISTIC_PARTNERS_STATE_ID OR
                    DIM_LP.LOGISTIC_PARTNERS_STATE_NAME <> CE_LP.LOGISTIC_PARTNERS_STATE_NAME 
                        
    WHEN NOT MATCHED THEN INSERT ( 
                                    DIM_LP.LOGISTIC_PARTNERS_SURR_ID,
                                    DIM_LP.LOGISTIC_PARTNERS_ID,
                                    DIM_LP.SOURCE_SYSTEM,
                                    DIM_LP.SOURCE_TABLE,
                                    DIM_LP.LOGISTIC_PARTNERS_NAME,
                                    DIM_LP.LOGISTIC_PARTNERS_CITY_ID,
                                    DIM_LP.LOGISTIC_PARTNERS_CITY_NAME,
                                    DIM_LP.LOGISTIC_PARTNERS_STATE_ID,
                                    DIM_LP.LOGISTIC_PARTNERS_STATE_NAME,
                                    DIM_LP.INSERT_DT,
                                    DIM_LP.UPDATE_DT
                                    )
        VALUES (
                    SEQ_DIM_LOGISTIC_PARTNERS.NEXTVAL,
                    CE_LP.LOGISTIC_PARTNERS_ID,
                    CE_LP.SOURCE_SYSTEM,
                    CE_LP.SOURCE_TABLE,
                    CE_LP.LOGISTIC_PARTNERS_NAME,
                    CE_LP.LOGISTIC_PARTNERS_CITY_ID,
                    CE_LP.LOGISTIC_PARTNERS_CITY_NAME,
                    CE_LP.LOGISTIC_PARTNERS_STATE_ID,
                    CE_LP.LOGISTIC_PARTNERS_STATE_NAME,
                    CURRENT_DATE,
                    CURRENT_DATE
                );
    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'DIM_LOGISTIC_PARTNERS',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'DIM_LOGISTIC_PARTNERS',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;
--------------------------------------------------------------------------------
PROCEDURE LD_DIM_PRODUCTS_SCD
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;

BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'DIM_PRODUCTS_SCD',
            IN_OPERATION_STATUS => 'BEGIN'
            );

---- adding new rows, also closing previous versions of existing rows

    MERGE INTO DIM_PRODUCTS_SCD CE_P
        USING (
                SELECT
                    'BL_3NF' AS SOURCE_SYSTEM,
                    'CE_PRODUCTS_SCD' AS SOURCE_TABLE,
                    P.PRODUCT_ID,
                    P.PRODUCT_NAME,
                    P.PRODUCT_CATEGORY_ID,
                    NVL(PC.PRODUCT_CATEGORY_NAME, -1) AS PRODUCT_CATEGORY_NAME,
                    P.PRODUCT_WEIGHT_G AS PRODUCT_WEIGHT,
                    P.PRODUCT_LENGTH_CM AS PRODUCT_LENGTH,
                    P.PRODUCT_HEIGHT_CM AS PRODUCT_HEIGHT,
                    P.PRODUCT_WIDTH_CM AS PRODUCT_WIDTH,
                    P.START_DT,
                    P.END_DT,
                    P.IS_ACTIVE
                FROM CE_PRODUCTS_SCD P
                LEFT JOIN CE_PRODUCTS_CATEGORIES_SCD PC ON P.PRODUCT_CATEGORY_ID =
                          PC.PRODUCT_CATEGORY_ID AND PC.IS_ACTIVE = 'Y'
                WHERE P.PRODUCT_ID <> -1 AND P.IS_ACTIVE = 'Y'
                ) SA_P
        ON (CE_P.PRODUCT_ID = SA_P.PRODUCT_ID AND
            CE_P.SOURCE_SYSTEM = SA_P.SOURCE_SYSTEM AND 
            CE_P.SOURCE_TABLE = SA_P.SOURCE_TABLE)
    WHEN MATCHED THEN UPDATE SET
                        CE_P.END_DT = CURRENT_DATE,
                        CE_P.IS_ACTIVE = 'N'
                            WHERE
                        CE_P.END_DT = TO_DATE('9999-12-31', 'YYYY-MM-DD') AND (
                        CE_P.PRODUCT_NAME <> SA_P.PRODUCT_NAME OR
                        CE_P.PRODUCT_CATEGORY_ID <> SA_P.PRODUCT_CATEGORY_ID OR
                        CE_P.PRODUCT_CATEGORY_NAME <> SA_P.PRODUCT_CATEGORY_NAME OR
                        CE_P.PRODUCT_WEIGHT <> SA_P.PRODUCT_WEIGHT OR
                        CE_P.PRODUCT_LENGTH <> SA_P.PRODUCT_LENGTH OR
                        CE_P.PRODUCT_HEIGHT <> SA_P.PRODUCT_HEIGHT OR
                        CE_P.PRODUCT_WIDTH <> SA_P.PRODUCT_WIDTH
                        )

    WHEN NOT MATCHED THEN INSERT (
                                    CE_P.PRODUCT_SURR_ID,
                                    CE_P.SOURCE_SYSTEM,
                                    CE_P.SOURCE_TABLE,
                                    CE_P.PRODUCT_ID,
                                    CE_P.PRODUCT_NAME,
                                    CE_P.PRODUCT_CATEGORY_ID,
                                    CE_P.PRODUCT_CATEGORY_NAME,
                                    CE_P.PRODUCT_WEIGHT,
                                    CE_P.PRODUCT_LENGTH,
                                    CE_P.PRODUCT_HEIGHT,
                                    CE_P.PRODUCT_WIDTH,
                                    CE_P.START_DT,
                                    CE_P.END_DT,
                                    CE_P.IS_ACTIVE,
                                    CE_P.INSERT_DT
                                    )
        VALUES (
                    SEQ_DIM_PRODUCTS_SCD_SURR.NEXTVAL,
                    SA_P.SOURCE_SYSTEM,
                    SA_P.SOURCE_TABLE,
                    SA_P.PRODUCT_ID,
                    SA_P.PRODUCT_NAME,
                    SA_P.PRODUCT_CATEGORY_ID,
                    SA_P.PRODUCT_CATEGORY_NAME,
                    SA_P.PRODUCT_WEIGHT,
                    SA_P.PRODUCT_LENGTH,
                    SA_P.PRODUCT_HEIGHT,
                    SA_P.PRODUCT_WIDTH,
                    SA_P.START_DT,
                    SA_P.END_DT,
                    SA_P.IS_ACTIVE,
                    CURRENT_DATE
                );

    LOGGER(
        IN_LOG_ID => VAR_LOG_ID,
        IN_OPERATION_TYPE => 'MERGE',
        IN_TARGET_TABLE => 'DIM_PRODUCTS_SCD',
        IN_OPERATION_STATUS => '1 MERGE FINISH',
        IN_ROW_HANDLED => SQL%ROWCOUNT
        );

    ---- adding new rows, which have got previous versions into the table
    MERGE INTO DIM_PRODUCTS_SCD CE_P
        USING (
                SELECT
                    'BL_3NF' AS SOURCE_SYSTEM,
                    'CE_PRODUCTS_SCD' AS SOURCE_TABLE,
                    P.PRODUCT_ID,
                    P.PRODUCT_NAME,
                    P.PRODUCT_CATEGORY_ID,
                    NVL(PC.PRODUCT_CATEGORY_NAME, -1) AS PRODUCT_CATEGORY_NAME,
                    P.PRODUCT_WEIGHT_G AS PRODUCT_WEIGHT,
                    P.PRODUCT_LENGTH_CM AS PRODUCT_LENGTH,
                    P.PRODUCT_HEIGHT_CM AS PRODUCT_HEIGHT,
                    P.PRODUCT_WIDTH_CM AS PRODUCT_WIDTH,
                    P.START_DT,
                    P.END_DT,
                    P.IS_ACTIVE
                FROM CE_PRODUCTS_SCD P
                LEFT JOIN CE_PRODUCTS_CATEGORIES_SCD PC ON P.PRODUCT_CATEGORY_ID =
                          PC.PRODUCT_CATEGORY_ID AND PC.IS_ACTIVE = 'Y'
                WHERE P.PRODUCT_ID <> -1 AND P.IS_ACTIVE = 'Y'
                ) SA_P
        ON (CE_P.PRODUCT_ID = SA_P.PRODUCT_ID AND
            CE_P.SOURCE_SYSTEM = SA_P.SOURCE_SYSTEM AND 
            CE_P.SOURCE_TABLE = SA_P.SOURCE_TABLE AND
            CE_P.IS_ACTIVE = 'Y')

    WHEN NOT MATCHED THEN INSERT (
                                    CE_P.PRODUCT_SURR_ID,
                                    CE_P.SOURCE_SYSTEM,
                                    CE_P.SOURCE_TABLE,
                                    CE_P.PRODUCT_ID,
                                    CE_P.PRODUCT_NAME,
                                    CE_P.PRODUCT_CATEGORY_ID,
                                    CE_P.PRODUCT_CATEGORY_NAME,
                                    CE_P.PRODUCT_WEIGHT,
                                    CE_P.PRODUCT_LENGTH,
                                    CE_P.PRODUCT_HEIGHT,
                                    CE_P.PRODUCT_WIDTH,
                                    CE_P.START_DT,
                                    CE_P.END_DT,
                                    CE_P.IS_ACTIVE,
                                    CE_P.INSERT_DT
                                    )
        VALUES (
                    SEQ_DIM_PRODUCTS_SCD_SURR.NEXTVAL,
                    SA_P.SOURCE_SYSTEM,
                    SA_P.SOURCE_TABLE,
                    SA_P.PRODUCT_ID,
                    SA_P.PRODUCT_NAME,
                    SA_P.PRODUCT_CATEGORY_ID,
                    SA_P.PRODUCT_CATEGORY_NAME,
                    SA_P.PRODUCT_WEIGHT,
                    SA_P.PRODUCT_LENGTH,
                    SA_P.PRODUCT_HEIGHT,
                    SA_P.PRODUCT_WIDTH,
                    SA_P.START_DT,
                    SA_P.END_DT,
                    SA_P.IS_ACTIVE,
                    CURRENT_DATE 
                ); 

    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'DIM_PRODUCTS_SCD',
            IN_OPERATION_STATUS => '2 MERGE FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'DIM_PRODUCTS_SCD',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;
--------------------------------------------------------------------------------
PROCEDURE LD_FCT_SALES_DD_INIT
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'INSERT',
            IN_TARGET_TABLE => 'FCT_SALES_DD',
            IN_OPERATION_STATUS => 'BEGIN'
            );
    
    INSERT INTO FCT_SALES_DD (
                                ORDER_ID,
                                SOURCE_SYSTEM,
                                SOURCE_TABLE,
                                EVENT_DT,
                                PRODUCT_SURR_ID,
                                CUSTOMER_SURR_ID,
                                SUPPLIER_SURR_ID,
                                PAYMENT_TYPE_SURR_ID,
                                LOGISTIC_PARTNERS_SURR_ID,
                                GEOLOCATION_SURR_ID,
                                FCT_PRODUCT_AMOUNT_SOLD,
                                FCT_PRODUCT_COST,
                                INSERT_DT,
                                UPDATE_DT
                             )
    SELECT
        SALE.SALE_ID AS ORDER_ID,
        'BL_3NF' AS SOURCE_SYSTEM,
        'CE_SALES' AS SOURCE_TABLE,
        SALE.PURCHASE_DATE AS EVENT_DT,
        NVL(PROD.PRODUCT_SURR_ID, -1),
        NVL(CUST.CUSTOMER_SURR_ID, -1),
        NVL(SUPP.SUPPLIER_SURR_ID, -1),
        NVL(PT.PAYMENT_TYPE_SURR_ID, -1),
        NVL(LP.LOGISTIC_PARTNERS_SURR_ID, -1),
        NVL(GEO.GEOLOCATION_SURR_ID, -1),
        SALE.AMOUNT_VALUE AS FCT_PRODUCT_AMOUNT_SOLD,
        SALE.COST_VALUE AS FCT_PRODUCT_COST,
        CURRENT_DATE,
        CURRENT_DATE
        
    FROM CE_SALES SALE
    LEFT JOIN DIM_PRODUCTS_SCD PROD ON SALE.PRODUCT_ID = PROD.PRODUCT_ID AND 
        PROD.IS_ACTIVE = 'Y'
    LEFT JOIN DIM_CUSTOMERS CUST ON CUST.CUSTOMER_ID = SALE.CUSTOMER_ID
    LEFT JOIN DIM_SUPPLIERS SUPP ON SUPP.SUPPLIER_ID = SALE.SUPPLIER_ID
    LEFT JOIN DIM_LOGISTIC_PARTNERS LP ON LP.LOGISTIC_PARTNERS_ID = SALE.LOGISTIC_PARTNER_ID
    LEFT JOIN DIM_PAYMENT_TYPES PT ON PT.PAYMENT_TYPE_ID = SALE.PAYMENT_TYPE_ID
    LEFT JOIN DIM_GEOLOCATIONS GEO ON CUST.CUSTOMER_CITY_ID = GEO.GEOLOCATION_CITY_ID;

    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'INSERT',
            IN_TARGET_TABLE => 'FCT_SALES_DD',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'INSERT',
                IN_TARGET_TABLE => 'FCT_SALES_DD',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;
-------------------------------------------------------------------------------
PROCEDURE LD_FCT_SALES_DD_REG (CURR_DATE DATE)
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
    CURR_PARTITION VARCHAR2(50);
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'PARTITION EXCHANGE',
            IN_TARGET_TABLE => 'FCT_SALES_DD',
            IN_OPERATION_STATUS => 'BEGIN'
            );
    
    FOR I IN 0..1
    LOOP
    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE WRK_FOR_EXCHANGE_FCT_SALES_DD';
    
    INSERT INTO WRK_FOR_EXCHANGE_FCT_SALES_DD (
                                ORDER_ID,
                                SOURCE_SYSTEM,
                                SOURCE_TABLE,
                                EVENT_DT,
                                PRODUCT_SURR_ID,
                                CUSTOMER_SURR_ID,
                                SUPPLIER_SURR_ID,
                                PAYMENT_TYPE_SURR_ID,
                                LOGISTIC_PARTNERS_SURR_ID,
                                GEOLOCATION_SURR_ID,
                                FCT_PRODUCT_AMOUNT_SOLD,
                                FCT_PRODUCT_COST,
                                INSERT_DT,
                                UPDATE_DT
                             )
    SELECT
        SALE.SALE_ID AS ORDER_ID,
        'BL_3NF' AS SOURCE_SYSTEM,
        'CE_SALES' AS SOURCE_TABLE,
        SALE.PURCHASE_DATE AS EVENT_DT,
        NVL(PROD.PRODUCT_SURR_ID, -1),
        NVL(CUST.CUSTOMER_SURR_ID, -1),
        NVL(SUPP.SUPPLIER_SURR_ID, -1),
        NVL(PT.PAYMENT_TYPE_SURR_ID, -1),
        NVL(LP.LOGISTIC_PARTNERS_SURR_ID, -1),
        NVL(GEO.GEOLOCATION_SURR_ID, -1),
        SALE.AMOUNT_VALUE AS FCT_PRODUCT_AMOUNT_SOLD,
        SALE.COST_VALUE AS FCT_PRODUCT_COST,
        CURRENT_DATE,
        CURRENT_DATE
        
    FROM CE_SALES SALE
    LEFT JOIN DIM_PRODUCTS_SCD PROD ON SALE.PRODUCT_ID = PROD.PRODUCT_ID AND 
        PROD.IS_ACTIVE = 'Y'
    LEFT JOIN DIM_CUSTOMERS CUST ON CUST.CUSTOMER_ID = SALE.CUSTOMER_ID
    LEFT JOIN DIM_SUPPLIERS SUPP ON SUPP.SUPPLIER_ID = SALE.SUPPLIER_ID
    LEFT JOIN DIM_LOGISTIC_PARTNERS LP ON LP.LOGISTIC_PARTNERS_ID = SALE.LOGISTIC_PARTNER_ID
    LEFT JOIN DIM_PAYMENT_TYPES PT ON PT.PAYMENT_TYPE_ID = SALE.PAYMENT_TYPE_ID
    LEFT JOIN DIM_GEOLOCATIONS GEO ON CUST.CUSTOMER_CITY_ID = GEO.GEOLOCATION_CITY_ID
    WHERE TRUNC(SALE.PURCHASE_DATE, 'MM') = TRUNC(ADD_MONTHS(CURR_DATE, -I), 'MM');
    
    COMMIT;

    CURR_PARTITION := 'SALES_' ||  TO_CHAR(TRUNC(ADD_MONTHS(CURR_DATE, -I), 'MM'), 'MM_YY');
    
    EXECUTE IMMEDIATE 'ALTER TABLE BL_DM.FCT_SALES_DD 
                    EXCHANGE PARTITION '||CURR_PARTITION||' WITH TABLE WRK_FOR_EXCHANGE_FCT_SALES_DD 
                    WITHOUT VALIDATION
                    UPDATE GLOBAL INDEXES';

    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'PARTITION EXCHANGE',
            IN_TARGET_TABLE => 'FCT_SALES_DD',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    
    END LOOP;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'PARTITION EXCHANGE',
                IN_TARGET_TABLE => 'FCT_SALES_DD',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;
-------------------------------------------------------------------------------

END PKG_ETL_DIM;