CREATE OR REPLACE PACKAGE BODY PKG_ETL_CE AS 

--------------------------------------------------------------------------------
PROCEDURE LD_CE_COUNTRIES
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_COUNTRIES',
            IN_OPERATION_STATUS => 'BEGIN'
            );

    MERGE INTO CE_COUNTRIES CE_C
        USING (
                SELECT DISTINCT
                    COUNTRY_ID,
                    'SA_SOURCE_SYSTEM_RETAIL' AS SOURCE_SYSTEM,
                    'SA_OLIST_GEO' AS SOURCE_TABLE,
                    TRIM(COUNTRY_NAME) AS COUNTRY_NAME
                FROM SA_OLIST_GEO
             ) SA_C
        ON (CE_C.COUNTRY_SRCID = SA_C.COUNTRY_ID)
    WHEN MATCHED THEN UPDATE SET
                        CE_C.COUNTRY_NAME = SA_C.COUNTRY_NAME,
                        CE_C.UPDATE_DT = CURRENT_DATE
                            WHERE 
                        CE_C.COUNTRY_NAME <> SA_C.COUNTRY_NAME

    WHEN NOT MATCHED THEN INSERT (
                                    CE_C.COUNTRY_ID,
                                    CE_C.COUNTRY_SRCID,
                                    CE_C.SOURCE_SYSTEM,
                                    CE_C.SOURCE_TABLE,
                                    CE_C.COUNTRY_NAME,
                                    CE_C.INSERT_DT,
                                    CE_C.UPDATE_DT
                                    )
        VALUES (
                SEQ_COUNTRIES_SURR.NEXTVAL,
                SA_C.COUNTRY_ID,
                SA_C.SOURCE_SYSTEM,
                SA_C.SOURCE_TABLE,
                SA_C.COUNTRY_NAME,
                CURRENT_DATE,
                CURRENT_DATE  
                );
    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_COUNTRIES',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'CE_COUNTRIES',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;

--------------------------------------------------------------------------------
PROCEDURE LD_CE_STATES
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_STATES',
            IN_OPERATION_STATUS => 'BEGIN'
            );

    MERGE INTO CE_STATES CE_S
        USING (SELECT DISTINCT
                    STATE_ID,
                    'SA_SOURCE_SYSTEM_RETAIL' AS SOURCE_SYSTEM,
                    'SA_OLIST_GEO' AS SOURCE_TABLE,
                    TRIM(STATE_NAME) AS STATE_NAME,
                    NVL(COUNTRY_ID, -1) AS COUNTRY_ID
                FROM SA_OLIST_GEO
                ) SA_S
        ON (SA_S.STATE_ID = CE_S.STATE_SRCID)
    WHEN MATCHED THEN UPDATE SET
                       CE_S.STATE_NAME = SA_S.STATE_NAME,
                       CE_S.STATE_COUNTRY_ID = SA_S.COUNTRY_ID,
                       CE_S.UPDATE_DT = CURRENT_DATE
                            WHERE 
                       CE_S.STATE_NAME <> SA_S.STATE_NAME OR
                       CE_S.STATE_COUNTRY_ID <> SA_S.COUNTRY_ID
                        
    WHEN NOT MATCHED THEN INSERT (
                                    CE_S.STATE_ID,
                                    CE_S.STATE_SRCID,
                                    CE_S.SOURCE_SYSTEM,
                                    CE_S.SOURCE_TABLE,
                                    CE_S.STATE_NAME,
                                    CE_S.STATE_COUNTRY_ID,
                                    CE_S.INSERT_DT,
                                    CE_S.UPDATE_DT
                                    )
        VALUES (
                    SEQ_STATES_SURR.NEXTVAL,
                    SA_S.STATE_ID,
                    SA_S.SOURCE_SYSTEM,
                    SA_S.SOURCE_TABLE,
                    SA_S.STATE_NAME,
                    SA_S.COUNTRY_ID,
                    CURRENT_DATE,
                    CURRENT_DATE
                );
    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_STATES',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'CE_STATES',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;

--------------------------------------------------------------------------------
PROCEDURE LD_CE_CITIES
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_CITIES',
            IN_OPERATION_STATUS => 'BEGIN'
            );

    MERGE INTO CE_CITIES CE_C
        USING (
                SELECT DISTINCT
                    CITY_ID AS CITY_SRCID,
                    'SA_SOURCE_SYSTEM_RETAIL' AS SOURCE_SYSTEM,
                    'SA_OLIST_GEO' AS SOURCE_TABLE,
                    TRIM(CITY_NAME) AS CITY_NAME,
                    NVL(STATE_ID, -1) AS CITY_STATE_ID
                FROM SA_SOURCE_SYSTEM_RETAIL.SA_OLIST_GEO
                ) SA_C
        ON (SA_C.CITY_SRCID = CE_C.CITY_SRCID)
    WHEN MATCHED THEN UPDATE SET
                       CE_C.CITY_NAME = SA_C.CITY_NAME,
                       CE_C.CITY_STATE_ID = SA_C.CITY_STATE_ID,
                       CE_C.UPDATE_DT = CURRENT_DATE
                            WHERE 
                       CE_C.CITY_NAME <> SA_C.CITY_NAME  OR 
                       CE_C.CITY_STATE_ID <> SA_C.CITY_STATE_ID
                       
    WHEN NOT MATCHED THEN INSERT (
                                    CE_C.CITY_ID,
                                    CE_C.CITY_SRCID,
                                    CE_C.SOURCE_SYSTEM,
                                    CE_C.SOURCE_TABLE,
                                    CE_C.CITY_NAME,
                                    CE_C.CITY_STATE_ID,
                                    CE_C.INSERT_DT,
                                    CE_C.UPDATE_DT
                                    )
        VALUES (
                    SEQ_CITIES_SURR.NEXTVAL,
                    SA_C.CITY_SRCID,
                    SA_C.SOURCE_SYSTEM,
                    SA_C.SOURCE_TABLE,
                    SA_C.CITY_NAME,
                    SA_C.CITY_STATE_ID,
                    CURRENT_DATE,
                    CURRENT_DATE
                );
    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_CITIES',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'CE_CITIES',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;

--------------------------------------------------------------------------------
PROCEDURE LD_CE_LOGISTIC_PARTNERS
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_LOGISTIC_PARTNERS',
            IN_OPERATION_STATUS => 'BEGIN'
            );

    MERGE INTO CE_LOGISTIC_PARTNERS CE_LP
        USING (
                SELECT
                    LOGISTIC_PARTNER_ID AS LOGISTIC_PARTNER_SRCID,
                    'SA_SOURCE_SYSTEM_RETAIL' AS SOURCE_SYSTEM,
                    'SA_LOGISTIC_PARTNERS' AS SOURCE_TABLE,
                    TRIM(LOGISTIC_PARTNER_NAME) AS LOGISTIC_PARTNER_NAME,
                    NVL(CITY_ID, -1) AS LOGISTIC_PARTNER_CITY_ID
                FROM SA_LOGISTIC_PARTNERS
                ) SA_LP
        ON (CE_LP.LOGISTIC_PARTNER_SRCID = SA_LP.LOGISTIC_PARTNER_SRCID)
    WHEN MATCHED THEN UPDATE SET
                       CE_LP.LOGISTIC_PARTNER_NAME = SA_LP.LOGISTIC_PARTNER_NAME,
                       CE_LP.LOGISTIC_PARTNER_CITY_ID = SA_LP.LOGISTIC_PARTNER_CITY_ID,
                       CE_LP.UPDATE_DT = CURRENT_DATE
                            WHERE 
                       CE_LP.LOGISTIC_PARTNER_NAME <> SA_LP.LOGISTIC_PARTNER_NAME OR
                       CE_LP.LOGISTIC_PARTNER_CITY_ID <> SA_LP.LOGISTIC_PARTNER_CITY_ID
                       
    WHEN NOT MATCHED THEN INSERT (
                                    CE_LP.LOGISTIC_PARTNER_ID,
                                    CE_LP.LOGISTIC_PARTNER_SRCID,
                                    CE_LP.SOURCE_SYSTEM,
                                    CE_LP.SOURCE_TABLE,
                                    CE_LP.LOGISTIC_PARTNER_NAME,
                                    CE_LP.LOGISTIC_PARTNER_CITY_ID,
                                    CE_LP.INSERT_DT,
                                    CE_LP.UPDATE_DT
                                    )
        VALUES (
                    SEQ_LOGISTIC_PARTNERS_SURR.NEXTVAL,
                    SA_LP.LOGISTIC_PARTNER_SRCID,
                    SA_LP.SOURCE_SYSTEM,
                    SA_LP.SOURCE_TABLE,
                    SA_LP.LOGISTIC_PARTNER_NAME,
                    SA_LP.LOGISTIC_PARTNER_CITY_ID,
                    CURRENT_DATE,
                    CURRENT_DATE
                );
    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_LOGISTIC_PARTNERS',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'CE_LOGISTIC_PARTNERS',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;
--------------------------------------------------------------------------------
PROCEDURE LD_CE_SUPPLIERS
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_SUPPLIERS',
            IN_OPERATION_STATUS => 'BEGIN'
            );

    MERGE INTO CE_SUPPLIERS CE_S
        USING (
                SELECT 
                    SUPPLIER_ID AS SUPPLIER_SRCID,
                    'SA_SOURCE_SYSTEM_RETAIL' AS SOURCE_SYSTEM,
                    'SA_OLIST_GEO' AS SOURCE_TABLE,
                    NVL(TRIM(SUPPLIER_NAME), 'N/A') AS SUPPLIER_NAME,
                    NVL(TRIM(SECTOR), 'N/A') AS SUPPLIER_SECTOR,
                    NVL((SELECT CITY_ID FROM CE_CITIES NF WHERE LOWER(NF.CITY_NAME) = 
                         LOWER(TRIM(SA.SUPPLIER_CITY)) and ROWNUM <= 1), -1) AS SUPPLIER_CITY_ID
                FROM SA_OLIST_SUPPLIERS_DATASET SA
                ) SA_S
        ON (CE_S.SUPPLIER_SRCID = SA_S.SUPPLIER_SRCID)
    WHEN MATCHED THEN UPDATE SET
                       CE_S.SUPPLIER_NAME = SA_S.SUPPLIER_NAME,
                       CE_S.SUPPLIER_SECTOR = SA_S.SUPPLIER_SECTOR,
                       CE_S.SUPPLIER_CITY_ID = SA_S.SUPPLIER_CITY_ID,
                       CE_S.UPDATE_DT = CURRENT_DATE
                            WHERE 
                       CE_S.SUPPLIER_NAME <> SA_S.SUPPLIER_NAME OR 
                       CE_S.SUPPLIER_SECTOR <> SA_S.SUPPLIER_SECTOR OR
                       CE_S.SUPPLIER_CITY_ID <> SA_S.SUPPLIER_CITY_ID 
                           
    WHEN NOT MATCHED THEN INSERT (
                                    CE_S.SUPPLIER_ID,
                                    CE_S.SUPPLIER_SRCID,
                                    CE_S.SOURCE_SYSTEM,
                                    CE_S.SOURCE_TABLE,
                                    CE_S.SUPPLIER_NAME,
                                    CE_S.SUPPLIER_SECTOR,
                                    CE_S.SUPPLIER_CITY_ID,
                                    CE_S.INSERT_DT,
                                    CE_S.UPDATE_DT
                                    )
        VALUES (
                    SEQ_SUPPLIERS_SURR.NEXTVAL,
                    SA_S.SUPPLIER_SRCID,
                    SA_S.SOURCE_SYSTEM,
                    SA_S.SOURCE_TABLE,
                    SA_S.SUPPLIER_NAME,
                    SA_S.SUPPLIER_SECTOR,
                    SA_S.SUPPLIER_CITY_ID,
                    CURRENT_DATE,
                    CURRENT_DATE
                );
    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_SUPPLIERS',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'CE_SUPPLIERS',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;

--------------------------------------------------------------------------------
PROCEDURE LD_CE_PAYMENT_TYPES
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_PAYMENT_TYPES',
            IN_OPERATION_STATUS => 'BEGIN'
            );

    MERGE INTO CE_PAYMENT_TYPES CE_P
        USING (
                SELECT
                    PAYMENT_TYPE_ID as PAYMENT_TYPE_SRCID,
                    'SA_SOURCE_SYSTEM_RETAIL' as SOURCE_SYSTEM,
                    'SA_OLIST_GEO' as SOURCE_TABLE,
                    TRIM(PAYMENT_TYPE_NAME) AS PAYMENT_TYPE_NAME
                FROM SA_PAYMENTS_TYPES) SA_P
        ON (SA_P.PAYMENT_TYPE_SRCID = CE_P.PAYMENT_TYPE_SRCID)
    WHEN MATCHED THEN UPDATE SET
                       CE_P.PAYMENT_TYPE_NAME = SA_P.PAYMENT_TYPE_NAME,
                       CE_P.UPDATE_DT = CURRENT_DATE
                            WHERE 
                        CE_P.PAYMENT_TYPE_NAME <> SA_P.PAYMENT_TYPE_NAME
                           
    WHEN NOT MATCHED THEN INSERT (
                                    CE_P.PAYMENT_TYPE_ID,
                                    CE_P.PAYMENT_TYPE_SRCID,
                                    CE_P.SOURCE_SYSTEM,
                                    CE_P.SOURCE_TABLE,
                                    CE_P.PAYMENT_TYPE_NAME,
                                    CE_P.INSERT_DT,
                                    CE_P.UPDATE_DT
                                    )
        VALUES (
                    SEQ_PAYMENT_TYPES_SURR.NEXTVAL,
                    SA_P.PAYMENT_TYPE_SRCID,
                    SA_P.SOURCE_SYSTEM,
                    SA_P.SOURCE_TABLE,
                    SA_P.PAYMENT_TYPE_NAME,
                    CURRENT_DATE,
                    CURRENT_DATE
                );
    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_PAYMENT_TYPES',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'CE_PAYMENT_TYPES',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;

--------------------------------------------------------------------------------
PROCEDURE LD_CE_CUSTOMERS
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_CUSTOMERS',
            IN_OPERATION_STATUS => 'BEGIN'
            );

    MERGE INTO CE_CUSTOMERS CE_C
        USING (
                    
    SELECT 
        TRIM(CUSTOMER_ID) AS CUSTOMER_SRCID,
        'SA_SOURCE_SYSTEM_RETAIL' AS SOURCE_SYSTEM,
        'SA_CUSTOMERS_RETAIL' AS SOURCE_TABLE,
        TRIM(FIRST_NAME) AS CUSTOMER_FIRST_NAME,
        TRIM(LAST_NAME) AS CUSTOMER_LAST_NAME,
        'N/A' AS CUSTOMER_COMPANY_NAME,
        NVL((SELECT CITY_ID FROM CE_CITIES NF WHERE LOWER(NF.CITY_NAME) = 
             LOWER(TRIM(SA.CUSTOMER_CITY)) and ROWNUM <= 1), -1) AS CUSTOMER_CITY_ID,
        'N' AS IS_COMPANY
    FROM SA_CUSTOMERS_RETAIL SA
    
    UNION ALL
    
    SELECT 
        TO_CHAR(CUSTOMER_ID) AS CUSTOMER_SRCID,
        'SA_SOURCE_SYSTEM_BUSINESS' AS SOURCE_SYSTEM,
        'SA_CUSTOMERS_BUSINESS' AS SOURCE_TABLE,
        'N/A' AS CUSTOMER_FIRST_NAME,
        'N/A' AS CUSTOMER_LAST_NAME,
        TRIM(COMPANY_NAME) AS CUSTOMER_COMPANY_NAME,
        NVL((SELECT CITY_ID FROM CE_CITIES NF WHERE LOWER(NF.CITY_NAME) = 
             LOWER(TRIM(SA.COMPANY_CITY)) and ROWNUM <= 1), -1) AS CUSTOMER_CITY_ID,
        'Y' AS IS_COMPANY     
    FROM SA_CUSTOMERS_BUSINESS SA
            ) SA_C
        ON (SA_C.CUSTOMER_SRCID = CE_C.CUSTOMER_SRCID AND 
            SA_C.SOURCE_SYSTEM = CE_C.SOURCE_SYSTEM AND 
            SA_C.SOURCE_TABLE = CE_C.SOURCE_TABLE)
    WHEN MATCHED THEN UPDATE SET
                        CE_C.CUSTOMER_FIRST_NAME = SA_C.CUSTOMER_FIRST_NAME,
                        CE_C.CUSTOMER_LAST_NAME = SA_C.CUSTOMER_LAST_NAME,
                        CE_C.CUSTOMER_COMPANY_NAME = SA_C.CUSTOMER_COMPANY_NAME,
                        CE_C.CUSTOMER_CITY_ID = SA_C.CUSTOMER_CITY_ID,
                        CE_C.UPDATE_DT = CURRENT_DATE
                            WHERE
                        CE_C.CUSTOMER_FIRST_NAME <> SA_C.CUSTOMER_FIRST_NAME OR
                        CE_C.CUSTOMER_LAST_NAME <> SA_C.CUSTOMER_LAST_NAME OR
                        CE_C.CUSTOMER_COMPANY_NAME <> SA_C.CUSTOMER_COMPANY_NAME OR
                        CE_C.CUSTOMER_CITY_ID <> SA_C.CUSTOMER_CITY_ID
                        
    WHEN NOT MATCHED THEN INSERT (
                                    CE_C.CUSTOMER_ID,
                                    CE_C.CUSTOMER_SRCID,
                                    CE_C.SOURCE_SYSTEM,
                                    CE_C.SOURCE_TABLE,
                                    CE_C.CUSTOMER_FIRST_NAME,
                                    CE_C.CUSTOMER_LAST_NAME,
                                    CE_C.CUSTOMER_COMPANY_NAME,
                                    CE_C.CUSTOMER_CITY_ID,
                                    CE_C.IS_COMPANY,
                                    CE_C.INSERT_DT,
                                    CE_C.UPDATE_DT
                                    )
        VALUES (
                    SEQ_CUSTOMERS_SURR.NEXTVAL,
                    SA_C.CUSTOMER_SRCID,
                    SA_C.SOURCE_SYSTEM,
                    SA_C.SOURCE_TABLE,
                    SA_C.CUSTOMER_FIRST_NAME,
                    SA_C.CUSTOMER_LAST_NAME,
                    SA_C.CUSTOMER_COMPANY_NAME,
                    SA_C.CUSTOMER_CITY_ID,
                    SA_C.IS_COMPANY,
                    CURRENT_DATE,
                    CURRENT_DATE
                );
    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_CUSTOMERS',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'CE_CUSTOMERS',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;

--------------------------------------------------------------------------------
PROCEDURE LD_CE_PRODUCTS_CATEGORIES_SCD
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_PRODUCTS_CATEGORIES_SCD',
            IN_OPERATION_STATUS => 'BEGIN'
            );

---- adding new rows, also closing previous versions of existing rows

    MERGE INTO CE_PRODUCTS_CATEGORIES_SCD CE_PC
        USING (
                SELECT DISTINCT
                    PRODUCT_CATEGORY_ID AS PRODUCT_CATEGORY_SRCID,
                    'SA_SOURCE_SYSTEM_RETAIL' AS SOURCE_SYSTEM,
                    'SA_PRODUCTS' AS SOURCE_TABLE,
                    TRIM(PRODUCT_CATEGORY_NAME_ENGLISH) AS PRODUCT_CATEGORY_NAME,
                    PRODUCT_CATEGORY_MARGIN_RATE
                FROM INCR_CE_PRODUCTS_CATEGORIES
                ) SA_PC
        ON (CE_PC.PRODUCT_CATEGORY_SRCID = SA_PC.PRODUCT_CATEGORY_SRCID)
    WHEN MATCHED THEN UPDATE SET
                        CE_PC.END_DT = CURRENT_DATE,
                        CE_PC.IS_ACTIVE = 'N'
                            WHERE
                        CE_PC.END_DT = TO_DATE('9999-12-31', 'YYYY-MM-DD') AND (
                        CE_PC.PRODUCT_CATEGORY_NAME <> SA_PC.PRODUCT_CATEGORY_NAME OR
                        CE_PC.PRODUCT_CATEGORY_MARGIN_RATE <> SA_PC.PRODUCT_CATEGORY_MARGIN_RATE)
                        
    WHEN NOT MATCHED THEN INSERT (
                                    CE_PC.PRODUCT_CATEGORY_ID,
                                    CE_PC.PRODUCT_CATEGORY_SRCID,
                                    CE_PC.SOURCE_SYSTEM,
                                    CE_PC.SOURCE_TABLE,
                                    CE_PC.PRODUCT_CATEGORY_NAME,
                                    CE_PC.PRODUCT_CATEGORY_MARGIN_RATE,
                                    CE_PC.START_DT,
                                    CE_PC.END_DT,
                                    CE_PC.IS_ACTIVE,
                                    CE_PC.INSERT_DT
                                    )
        VALUES (
                    SEQ_PRODUCTS_CATEGORIES_SURR.NEXTVAL,
                    SA_PC.PRODUCT_CATEGORY_SRCID,
                    SA_PC.SOURCE_SYSTEM,
                    SA_PC.SOURCE_TABLE,
                    SA_PC.PRODUCT_CATEGORY_NAME,
                    SA_PC.PRODUCT_CATEGORY_MARGIN_RATE,
                    CURRENT_DATE,
                    TO_DATE('9999-12-31', 'YYYY-MM-DD'),
                    'Y',
                    CURRENT_DATE
                );

    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_PRODUCTS_CATEGORIES_SCD',
            IN_OPERATION_STATUS => '1 MERGE FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
                
    ---- adding new rows, which have got previous versions into the table
    
    MERGE INTO CE_PRODUCTS_CATEGORIES_SCD CE_PC
        USING (
                SELECT DISTINCT
                    INCR.PRODUCT_CATEGORY_ID AS PRODUCT_CATEGORY_SRCID,
                    'SA_SOURCE_SYSTEM_RETAIL' AS SOURCE_SYSTEM,
                    'SA_PRODUCTS' AS SOURCE_TABLE,
                    TRIM(INCR.PRODUCT_CATEGORY_NAME_ENGLISH) AS PRODUCT_CATEGORY_NAME,
                    INCR.PRODUCT_CATEGORY_MARGIN_RATE,
                    INIT.PRODUCT_CATEGORY_ID AS SURR_ID
                FROM INCR_CE_PRODUCTS_CATEGORIES INCR
                LEFT JOIN CE_PRODUCTS_CATEGORIES_SCD INIT ON INIT.PRODUCT_CATEGORY_SRCID
                          = INCR.PRODUCT_CATEGORY_ID
                ) SA_PC
        ON (CE_PC.PRODUCT_CATEGORY_SRCID = SA_PC.PRODUCT_CATEGORY_SRCID AND 
            CE_PC.IS_ACTIVE = 'Y')
                        
    WHEN NOT MATCHED THEN INSERT (
                                    CE_PC.PRODUCT_CATEGORY_ID,
                                    CE_PC.PRODUCT_CATEGORY_SRCID,
                                    CE_PC.SOURCE_SYSTEM,
                                    CE_PC.SOURCE_TABLE,
                                    CE_PC.PRODUCT_CATEGORY_NAME,
                                    CE_PC.PRODUCT_CATEGORY_MARGIN_RATE,
                                    CE_PC.START_DT,
                                    CE_PC.END_DT,
                                    CE_PC.IS_ACTIVE,
                                    CE_PC.INSERT_DT
                                    )
        VALUES (
                    SA_PC.SURR_ID,
                    SA_PC.PRODUCT_CATEGORY_SRCID,
                    SA_PC.SOURCE_SYSTEM,
                    SA_PC.SOURCE_TABLE,
                    SA_PC.PRODUCT_CATEGORY_NAME,
                    SA_PC.PRODUCT_CATEGORY_MARGIN_RATE,
                    CURRENT_DATE,
                    TO_DATE('9999-12-31', 'YYYY-MM-DD'),
                    'Y',
                    CURRENT_DATE
                ); 
                
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_PRODUCTS_CATEGORIES_SCD',
            IN_OPERATION_STATUS => '2 MERGE FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
                
     -- update loaded date into required incr view          
    UPDATE PRM_MTA_INCREMENTAL_LOAD SET
        PREVIOUS_LOADED_DATE = (SELECT MAX(INSERT_DT) FROM CE_PRODUCTS_CATEGORIES_SCD)
    WHERE TARGET_TABLE_NAME = 'CE_PRODUCTS_CATEGORIES_SCD';
    
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'CE_PRODUCTS_CATEGORIES_SCD',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;

-------------------------------------------------------------------------------
PROCEDURE LD_CE_PRODUCTS_SCD
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_PRODUCTS_SCD',
            IN_OPERATION_STATUS => 'BEGIN'
            );

---- adding new rows, also closing previous versions of existing rows

    MERGE INTO CE_PRODUCTS_SCD CE_P
        USING (
                SELECT 
                    PRODUCT_ID AS PRODUCT_SRCID,
                    'SA_SOURCE_SYSTEM_RETAIL' AS SOURCE_SYSTEM,
                    'SA_PRODUCTS' AS SOURCE_TABLE,
                    TRIM(PRODUCT_NAME) AS PRODUCT_NAME,
                    NVL(PRODUCT_CATEGORY_ID, -1) AS PRODUCT_CATEGORY_ID,
                    NVL(PRODUCT_WEIGHT_G, -1) AS PRODUCT_WEIGHT_G, 
                    NVL(PRODUCT_LENGTH_CM, -1) AS PRODUCT_LENGTH_CM, 
                    NVL(PRODUCT_HEIGHT_CM, -1) AS PRODUCT_HEIGHT_CM, 
                    NVL(PRODUCT_WIDTH_CM, -1) AS PRODUCT_WIDTH_CM
                FROM INCR_CE_PRODUCTS
                ) SA_P
        ON (CE_P.PRODUCT_SRCID = SA_P.PRODUCT_SRCID)
    WHEN MATCHED THEN UPDATE SET
                        CE_P.END_DT = CURRENT_DATE,
                        CE_P.IS_ACTIVE = 'N'
                            WHERE
                        CE_P.END_DT = TO_DATE('9999-12-31', 'YYYY-MM-DD') AND (
                        CE_P.PRODUCT_NAME <> SA_P.PRODUCT_NAME OR
                        CE_P.PRODUCT_CATEGORY_ID <> SA_P.PRODUCT_CATEGORY_ID OR
                        CE_P.PRODUCT_WEIGHT_G <> SA_P.PRODUCT_WEIGHT_G OR
                        CE_P.PRODUCT_LENGTH_CM <> SA_P.PRODUCT_LENGTH_CM OR
                        CE_P.PRODUCT_HEIGHT_CM <> SA_P.PRODUCT_HEIGHT_CM OR
                        CE_P.PRODUCT_WIDTH_CM <> SA_P.PRODUCT_WIDTH_CM
                        )
                        
    WHEN NOT MATCHED THEN INSERT (
                                    CE_P.PRODUCT_ID,
                                    CE_P.PRODUCT_SRCID,
                                    CE_P.SOURCE_SYSTEM,
                                    CE_P.SOURCE_TABLE,
                                    CE_P.PRODUCT_NAME,
                                    CE_P.PRODUCT_CATEGORY_ID,
                                    CE_P.PRODUCT_WEIGHT_G,
                                    CE_P.PRODUCT_LENGTH_CM,
                                    CE_P.PRODUCT_HEIGHT_CM,
                                    CE_P.PRODUCT_WIDTH_CM,
                                    CE_P.START_DT,
                                    CE_P.END_DT,
                                    CE_P.IS_ACTIVE,
                                    CE_P.INSERT_DT
                                    )
        VALUES (
                    SEQ_PRODUCTS_SURR.NEXTVAL,
                    SA_P.PRODUCT_SRCID,
                    SA_P.SOURCE_SYSTEM,
                    SA_P.SOURCE_TABLE,
                    SA_P.PRODUCT_NAME,
                    SA_P.PRODUCT_CATEGORY_ID,
                    SA_P.PRODUCT_WEIGHT_G,
                    SA_P.PRODUCT_LENGTH_CM,
                    SA_P.PRODUCT_HEIGHT_CM,
                    SA_P.PRODUCT_WIDTH_CM,
                    CURRENT_DATE,
                    TO_DATE('9999-12-31', 'YYYY-MM-DD'),
                    'Y',
                    CURRENT_DATE
                );
                
    LOGGER(
        IN_LOG_ID => VAR_LOG_ID,
        IN_OPERATION_TYPE => 'MERGE',
        IN_TARGET_TABLE => 'CE_PRODUCTS_SCD',
        IN_OPERATION_STATUS => '1 MERGE FINISH',
        IN_ROW_HANDLED => SQL%ROWCOUNT
        );
                
    ---- adding new rows, which have got previous versions into the table
    MERGE INTO CE_PRODUCTS_SCD CE_P
        USING (
                SELECT DISTINCT
                    INCR.PRODUCT_ID AS PRODUCT_SRCID,
                    'SA_SOURCE_SYSTEM_RETAIL' AS SOURCE_SYSTEM,
                    'SA_PRODUCTS' AS SOURCE_TABLE,
                    TRIM(INCR.PRODUCT_NAME) AS PRODUCT_NAME,
                    NVL(INCR.PRODUCT_CATEGORY_ID, -1) AS PRODUCT_CATEGORY_ID,
                    NVL(INCR.PRODUCT_WEIGHT_G, -1) AS PRODUCT_WEIGHT_G, 
                    NVL(INCR.PRODUCT_LENGTH_CM, -1) AS PRODUCT_LENGTH_CM, 
                    NVL(INCR.PRODUCT_HEIGHT_CM, -1) AS PRODUCT_HEIGHT_CM, 
                    NVL(INCR.PRODUCT_WIDTH_CM, -1) AS PRODUCT_WIDTH_CM,
                    INIT.PRODUCT_ID AS SURR_ID
                FROM INCR_CE_PRODUCTS INCR
                LEFT JOIN CE_PRODUCTS_SCD INIT ON INIT.PRODUCT_SRCID
                          = INCR.PRODUCT_ID
                ) SA_P
        ON (CE_P.PRODUCT_SRCID = SA_P.PRODUCT_SRCID AND 
            CE_P.IS_ACTIVE = 'Y')
                        
    WHEN NOT MATCHED THEN INSERT (
                                    CE_P.PRODUCT_ID,
                                    CE_P.PRODUCT_SRCID,
                                    CE_P.SOURCE_SYSTEM,
                                    CE_P.SOURCE_TABLE,
                                    CE_P.PRODUCT_NAME,
                                    CE_P.PRODUCT_CATEGORY_ID,
                                    CE_P.PRODUCT_WEIGHT_G,
                                    CE_P.PRODUCT_LENGTH_CM,
                                    CE_P.PRODUCT_HEIGHT_CM,
                                    CE_P.PRODUCT_WIDTH_CM,
                                    CE_P.START_DT,
                                    CE_P.END_DT,
                                    CE_P.IS_ACTIVE,
                                    CE_P.INSERT_DT
                                    )
        VALUES (
                    SA_P.SURR_ID,
                    SA_P.PRODUCT_SRCID,
                    SA_P.SOURCE_SYSTEM,
                    SA_P.SOURCE_TABLE,
                    SA_P.PRODUCT_NAME,
                    SA_P.PRODUCT_CATEGORY_ID,
                    SA_P.PRODUCT_WEIGHT_G,
                    SA_P.PRODUCT_LENGTH_CM,
                    SA_P.PRODUCT_HEIGHT_CM,
                    SA_P.PRODUCT_WIDTH_CM,
                    CURRENT_DATE,
                    TO_DATE('9999-12-31', 'YYYY-MM-DD'),
                    'Y',
                    CURRENT_DATE
                ); 
                
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_PRODUCTS_SCD',
            IN_OPERATION_STATUS => '2 MERGE FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
                
     -- update loaded date into required incr view          
    UPDATE PRM_MTA_INCREMENTAL_LOAD SET
        PREVIOUS_LOADED_DATE = (SELECT MAX(INSERT_DT) FROM CE_PRODUCTS_SCD)
    WHERE TARGET_TABLE_NAME = 'CE_PRODUCTS_SCD';
    
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'CE_PRODUCTS_SCD',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;
--------------------------------------------------------------------------------
PROCEDURE LD_CE_SALES_RETAIL
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_SALES_RETAIL',
            IN_OPERATION_STATUS => 'BEGIN'
            );

    MERGE INTO CE_SALES CE_S
        USING (
                SELECT
                    TRIM(ORDER_ID) AS SALE_SRCID,
                    SOURCE_SYSTEM,
                    SOURCE_TABLE,
                    NVL(PRODUCT_ID, -1) AS PRODUCT_ID,
                    NVL(CUSTOMER_ID, -1) AS CUSTOMER_ID,
                    NVL(SUPPLIER_ID, -1) AS SUPPLIER_ID,
                    NVL(LOGISTIC_PARTNER_ID, -1) AS LOGISTIC_PARTNER_ID,
                    NVL(PAYMENT_TYPE_ID, -1) AS PAYMENT_TYPE_ID,
                    NVL(PAYMENT_VALUE, -1) AS AMOUNT_VALUE,
                    NVL(COST_VALUE, -1) AS COST_VALUE,
                    NVL(PURCHASE_DATE, TO_DATE('1900-01-01', 'YYYY-MM-DD')) AS PURCHASE_DATE
                FROM INCR_CE_SALES_RETAIL
                ) SA_S
        ON (SA_S.SALE_SRCID = CE_S.SALE_SRCID)
    WHEN MATCHED THEN UPDATE SET
                        CE_S.PRODUCT_ID = SA_S.PRODUCT_ID,
                        CE_S.CUSTOMER_ID = SA_S.CUSTOMER_ID,
                        CE_S.SUPPLIER_ID = SA_S.SUPPLIER_ID,
                        CE_S.LOGISTIC_PARTNER_ID = SA_S.LOGISTIC_PARTNER_ID,
                        CE_S.PAYMENT_TYPE_ID = SA_S.PAYMENT_TYPE_ID,
                        CE_S.AMOUNT_VALUE = SA_S.AMOUNT_VALUE,
                        CE_S.COST_VALUE = SA_S.COST_VALUE,
                        CE_S.PURCHASE_DATE = SA_S.PURCHASE_DATE,
                        CE_S.UPDATE_DT = CURRENT_DATE
                            WHERE 
                        CE_S.PRODUCT_ID <> SA_S.PRODUCT_ID OR
                        CE_S.CUSTOMER_ID <> SA_S.CUSTOMER_ID OR
                        CE_S.SUPPLIER_ID <> SA_S.SUPPLIER_ID OR
                        CE_S.LOGISTIC_PARTNER_ID <> SA_S.LOGISTIC_PARTNER_ID OR
                        CE_S.PAYMENT_TYPE_ID <> SA_S.PAYMENT_TYPE_ID OR
                        CE_S.AMOUNT_VALUE <> SA_S.AMOUNT_VALUE OR
                        CE_S.COST_VALUE <> SA_S.COST_VALUE OR
                        CE_S.PURCHASE_DATE <> SA_S.PURCHASE_DATE
                           
    WHEN NOT MATCHED THEN INSERT (
                                    CE_S.SALE_ID,
                                    CE_S.SALE_SRCID,
                                    CE_S.SOURCE_SYSTEM,
                                    CE_S.SOURCE_TABLE,
                                    CE_S.PRODUCT_ID,
                                    CE_S.CUSTOMER_ID,
                                    CE_S.SUPPLIER_ID,
                                    CE_S.LOGISTIC_PARTNER_ID,
                                    CE_S.PAYMENT_TYPE_ID,
                                    CE_S.AMOUNT_VALUE,
                                    CE_S.COST_VALUE,
                                    CE_S.PURCHASE_DATE,
                                    CE_S.INSERT_DT,
                                    CE_S.UPDATE_DT      
                                  )
        VALUES (
                    SEQ_SALES_SURR.NEXTVAL,
                    SA_S.SALE_SRCID,
                    SA_S.SOURCE_SYSTEM,
                    SA_S.SOURCE_TABLE,
                    SA_S.PRODUCT_ID,
                    SA_S.CUSTOMER_ID,
                    SA_S.SUPPLIER_ID,
                    SA_S.LOGISTIC_PARTNER_ID,
                    SA_S.PAYMENT_TYPE_ID,
                    SA_S.AMOUNT_VALUE,
                    SA_S.COST_VALUE,
                    SA_S.PURCHASE_DATE,
                    CURRENT_DATE,
                    CURRENT_DATE 
                );
    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_SALES_RETAIL',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    
     -- update loaded date into required incr view
    UPDATE PRM_MTA_INCREMENTAL_LOAD SET
        PREVIOUS_LOADED_DATE = (SELECT MAX(INSERT_DT) FROM CE_SALES
                                WHERE SOURCE_SYSTEM = 'SA_SOURCE_SYSTEM_RETAIL' AND 
                                SOURCE_TABLE = 'SA_SALES')
    WHERE TARGET_TABLE_NAME = 'CE_SALES' AND SA_TABLE_NAME = 'SA_SALES_RETAIL';        
    
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'CE_SALES_RETAIL',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;
--------------------------------------------------------------------------------
PROCEDURE LD_CE_SALES_BUSINESS
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_SALES_BUSINESS',
            IN_OPERATION_STATUS => 'BEGIN'
            );

    MERGE INTO CE_SALES CE_S
        USING (
                SELECT
                    TRIM(ORDER_ID) AS SALE_SRCID,
                    SOURCE_SYSTEM,
                    SOURCE_TABLE,
                    NVL(PRODUCT_ID, -1) AS PRODUCT_ID,
                    NVL(CUSTOMER_ID, -1) AS CUSTOMER_ID,
                    NVL(SUPPLIER_ID, -1) AS SUPPLIER_ID,
                    NVL(LOGISTIC_PARTNER_ID, -1) AS LOGISTIC_PARTNER_ID,
                    NVL(PAYMENT_TYPE_ID, -1) AS PAYMENT_TYPE_ID,
                    NVL(PAYMENT_VALUE, -1) AS AMOUNT_VALUE,
                    NVL(COST_VALUE, -1) AS COST_VALUE,
                    NVL(PURCHASE_DATE, TO_DATE('1900-01-01', 'YYYY-MM-DD')) AS PURCHASE_DATE
                FROM INCR_CE_SALES_BUSINESS
                ) SA_S
        ON (SA_S.SALE_SRCID = CE_S.SALE_SRCID)
    WHEN MATCHED THEN UPDATE SET
                        CE_S.PRODUCT_ID = SA_S.PRODUCT_ID,
                        CE_S.CUSTOMER_ID = SA_S.CUSTOMER_ID,
                        CE_S.SUPPLIER_ID = SA_S.SUPPLIER_ID,
                        CE_S.LOGISTIC_PARTNER_ID = SA_S.LOGISTIC_PARTNER_ID,
                        CE_S.PAYMENT_TYPE_ID = SA_S.PAYMENT_TYPE_ID,
                        CE_S.AMOUNT_VALUE = SA_S.AMOUNT_VALUE,
                        CE_S.COST_VALUE = SA_S.COST_VALUE,
                        CE_S.PURCHASE_DATE = SA_S.PURCHASE_DATE,
                        CE_S.UPDATE_DT = CURRENT_DATE
                            WHERE 
                        CE_S.PRODUCT_ID <> SA_S.PRODUCT_ID OR
                        CE_S.CUSTOMER_ID <> SA_S.CUSTOMER_ID OR
                        CE_S.SUPPLIER_ID <> SA_S.SUPPLIER_ID OR
                        CE_S.LOGISTIC_PARTNER_ID <> SA_S.LOGISTIC_PARTNER_ID OR
                        CE_S.PAYMENT_TYPE_ID <> SA_S.PAYMENT_TYPE_ID OR
                        CE_S.AMOUNT_VALUE <> SA_S.AMOUNT_VALUE OR
                        CE_S.COST_VALUE <> SA_S.COST_VALUE OR
                        CE_S.PURCHASE_DATE <> SA_S.PURCHASE_DATE
                           
    WHEN NOT MATCHED THEN INSERT (
                                    CE_S.SALE_ID,
                                    CE_S.SALE_SRCID,
                                    CE_S.SOURCE_SYSTEM,
                                    CE_S.SOURCE_TABLE,
                                    CE_S.PRODUCT_ID,
                                    CE_S.CUSTOMER_ID,
                                    CE_S.SUPPLIER_ID,
                                    CE_S.LOGISTIC_PARTNER_ID,
                                    CE_S.PAYMENT_TYPE_ID,
                                    CE_S.AMOUNT_VALUE,
                                    CE_S.COST_VALUE,
                                    CE_S.PURCHASE_DATE,
                                    CE_S.INSERT_DT,
                                    CE_S.UPDATE_DT      
                                  )
        VALUES (
                    SEQ_SALES_SURR.NEXTVAL,
                    SA_S.SALE_SRCID,
                    SA_S.SOURCE_SYSTEM,
                    SA_S.SOURCE_TABLE,
                    SA_S.PRODUCT_ID,
                    SA_S.CUSTOMER_ID,
                    SA_S.SUPPLIER_ID,
                    SA_S.LOGISTIC_PARTNER_ID,
                    SA_S.PAYMENT_TYPE_ID,
                    SA_S.AMOUNT_VALUE,
                    SA_S.COST_VALUE,
                    SA_S.PURCHASE_DATE,
                    CURRENT_DATE,
                    CURRENT_DATE 
                );
    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_SALES_BUSINESS',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    
     -- update loaded date into required incr view
    UPDATE PRM_MTA_INCREMENTAL_LOAD SET
        PREVIOUS_LOADED_DATE = (SELECT MAX(INSERT_DT) FROM CE_SALES
                                WHERE SOURCE_SYSTEM = 'SA_SOURCE_SYSTEM_BUSINESS' AND 
                                SOURCE_TABLE = 'SA_SALES_BUSINESS')
    WHERE TARGET_TABLE_NAME = 'CE_SALES' AND SA_TABLE_NAME = 'SA_SALES_BUSINESS';        
    
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'CE_SALES_BUSINESS',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;

--------------------------------------------------------------------------------
PROCEDURE LD_CE_SALES_MAIN
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE.NEXTVAL;
    
BEGIN
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_SALES_MAIN',
            IN_OPERATION_STATUS => 'BEGIN'
            );

    MERGE INTO CE_SALES CE_S
        USING (
                SELECT
                    TRIM(ORDER_ID) AS SALE_SRCID,
                    SOURCE_SYSTEM,
                    SOURCE_TABLE,
                    NVL(PRODUCT_ID, -1) AS PRODUCT_ID,
                    NVL(CUSTOMER_ID, -1) AS CUSTOMER_ID,
                    NVL(SUPPLIER_ID, -1) AS SUPPLIER_ID,
                    NVL(LOGISTIC_PARTNER_ID, -1) AS LOGISTIC_PARTNER_ID,
                    NVL(PAYMENT_TYPE_ID, -1) AS PAYMENT_TYPE_ID,
                    NVL(PAYMENT_VALUE, -1) AS AMOUNT_VALUE,
                    NVL(COST_VALUE, -1) AS COST_VALUE,
                    NVL(PURCHASE_DATE, TO_DATE('1900-01-01', 'YYYY-MM-DD')) AS PURCHASE_DATE
                FROM INCR_CE_SALES_BUSINESS
                
                UNION ALL 
                
                SELECT
                    TRIM(ORDER_ID) AS SALE_SRCID,
                    SOURCE_SYSTEM,
                    SOURCE_TABLE,
                    NVL(PRODUCT_ID, -1) AS PRODUCT_ID,
                    NVL(CUSTOMER_ID, -1) AS CUSTOMER_ID,
                    NVL(SUPPLIER_ID, -1) AS SUPPLIER_ID,
                    NVL(LOGISTIC_PARTNER_ID, -1) AS LOGISTIC_PARTNER_ID,
                    NVL(PAYMENT_TYPE_ID, -1) AS PAYMENT_TYPE_ID,
                    NVL(PAYMENT_VALUE, -1) AS AMOUNT_VALUE,
                    NVL(COST_VALUE, -1) AS COST_VALUE,
                    NVL(PURCHASE_DATE, TO_DATE('1900-01-01', 'YYYY-MM-DD')) AS PURCHASE_DATE
                FROM INCR_CE_SALES_RETAIL
                ) SA_S
        ON (SA_S.SALE_SRCID = CE_S.SALE_SRCID AND 
            SA_S.SOURCE_SYSTEM = CE_S.SOURCE_SYSTEM AND
            SA_S.SOURCE_TABLE = CE_S.SOURCE_TABLE)
    WHEN MATCHED THEN UPDATE SET
                        CE_S.PRODUCT_ID = SA_S.PRODUCT_ID,
                        CE_S.CUSTOMER_ID = SA_S.CUSTOMER_ID,
                        CE_S.SUPPLIER_ID = SA_S.SUPPLIER_ID,
                        CE_S.LOGISTIC_PARTNER_ID = SA_S.LOGISTIC_PARTNER_ID,
                        CE_S.PAYMENT_TYPE_ID = SA_S.PAYMENT_TYPE_ID,
                        CE_S.AMOUNT_VALUE = SA_S.AMOUNT_VALUE,
                        CE_S.COST_VALUE = SA_S.COST_VALUE,
                        CE_S.PURCHASE_DATE = SA_S.PURCHASE_DATE,
                        CE_S.UPDATE_DT = CURRENT_DATE
                            WHERE 
                        CE_S.PRODUCT_ID <> SA_S.PRODUCT_ID OR
                        CE_S.CUSTOMER_ID <> SA_S.CUSTOMER_ID OR
                        CE_S.SUPPLIER_ID <> SA_S.SUPPLIER_ID OR
                        CE_S.LOGISTIC_PARTNER_ID <> SA_S.LOGISTIC_PARTNER_ID OR
                        CE_S.PAYMENT_TYPE_ID <> SA_S.PAYMENT_TYPE_ID OR
                        CE_S.AMOUNT_VALUE <> SA_S.AMOUNT_VALUE OR
                        CE_S.COST_VALUE <> SA_S.COST_VALUE OR
                        CE_S.PURCHASE_DATE <> SA_S.PURCHASE_DATE
                           
    WHEN NOT MATCHED THEN INSERT (
                                    CE_S.SALE_ID,
                                    CE_S.SALE_SRCID,
                                    CE_S.SOURCE_SYSTEM,
                                    CE_S.SOURCE_TABLE,
                                    CE_S.PRODUCT_ID,
                                    CE_S.CUSTOMER_ID,
                                    CE_S.SUPPLIER_ID,
                                    CE_S.LOGISTIC_PARTNER_ID,
                                    CE_S.PAYMENT_TYPE_ID,
                                    CE_S.AMOUNT_VALUE,
                                    CE_S.COST_VALUE,
                                    CE_S.PURCHASE_DATE,
                                    CE_S.INSERT_DT,
                                    CE_S.UPDATE_DT      
                                  )
        VALUES (
                    SEQ_SALES_SURR.NEXTVAL,
                    SA_S.SALE_SRCID,
                    SA_S.SOURCE_SYSTEM,
                    SA_S.SOURCE_TABLE,
                    SA_S.PRODUCT_ID,
                    SA_S.CUSTOMER_ID,
                    SA_S.SUPPLIER_ID,
                    SA_S.LOGISTIC_PARTNER_ID,
                    SA_S.PAYMENT_TYPE_ID,
                    SA_S.AMOUNT_VALUE,
                    SA_S.COST_VALUE,
                    SA_S.PURCHASE_DATE,
                    CURRENT_DATE,
                    CURRENT_DATE 
                );
    
    LOGGER(
            IN_LOG_ID => VAR_LOG_ID,
            IN_OPERATION_TYPE => 'MERGE',
            IN_TARGET_TABLE => 'CE_SALES_MAIN',
            IN_OPERATION_STATUS => 'FINISH',
            IN_ROW_HANDLED => SQL%ROWCOUNT
            );
    
     -- update loaded date into required incr view
    UPDATE PRM_MTA_INCREMENTAL_LOAD SET
        PREVIOUS_LOADED_DATE = (SELECT MAX(INSERT_DT) FROM CE_SALES
                                WHERE SOURCE_SYSTEM = 'SA_SOURCE_SYSTEM_BUSINESS' AND 
                                SOURCE_TABLE = 'SA_SALES_BUSINESS')
    WHERE TARGET_TABLE_NAME = 'CE_SALES' AND SA_TABLE_NAME = 'SA_SALES_BUSINESS';
    
    UPDATE PRM_MTA_INCREMENTAL_LOAD SET
        PREVIOUS_LOADED_DATE = (SELECT MAX(INSERT_DT) FROM CE_SALES
                                WHERE SOURCE_SYSTEM = 'SA_SOURCE_SYSTEM_RETAIL' AND 
                                SOURCE_TABLE = 'SA_SALES')
    WHERE TARGET_TABLE_NAME = 'CE_SALES' AND SA_TABLE_NAME = 'SA_SALES_RETAIL'; 
    
    
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'CE_SALES_MAIN',
                IN_OPERATION_STATUS => 'ERROR',
                IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                );
        ROLLBACK;
        RAISE;

END;



END PKG_ETL_CE;
