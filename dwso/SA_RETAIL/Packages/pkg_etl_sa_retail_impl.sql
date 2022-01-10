CREATE OR REPLACE PACKAGE BODY PKG_ETL_SA_RETAIL AS 
--------------------------------------------------------------------------------
PROCEDURE LD_SA_GEO
IS

    CURSOR CURR_GEO_DATA IS 
        SELECT
            CITY_ID,
            CITY_NAME,
            STATE_ID,
            STATE_NAME,
            COUNTRY_ID,
            COUNTRY_NAME
        FROM EXT_OLIST_GEO;
        
    C_CITY_ID       VARCHAR2(250);
    C_CITY_NAME     VARCHAR2(250);
    C_STATE_ID      VARCHAR2(250); 
    C_STATE_NAME    VARCHAR2(250);
    C_COUNTRY_ID    VARCHAR2(250); 
    C_COUNTRY_NAME  VARCHAR2(250);
    
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE_SA_RETAIL.NEXTVAL;
    VAR_COUNTER NUMBER:= 0;
    
BEGIN
    LOGGER_SA_RETAIL(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_OLIST_GEO',
                    IN_OPERATION_STATUS => 'BEGIN'
                    );
    OPEN CURR_GEO_DATA;
    LOOP
    FETCH CURR_GEO_DATA INTO C_CITY_ID, C_CITY_NAME, C_STATE_ID, C_STATE_NAME,
                             C_COUNTRY_ID, C_COUNTRY_NAME;
        EXIT WHEN CURR_GEO_DATA%NOTFOUND;
        MERGE INTO SA_OLIST_GEO SA_GEO
            USING (SELECT TO_NUMBER(C_CITY_ID) AS C_CITY_ID, C_CITY_NAME AS C_CITY_NAME,
                   TO_NUMBER(C_STATE_ID) AS C_STATE_ID, C_STATE_NAME AS C_STATE_NAME,
                   TO_NUMBER(C_COUNTRY_ID) AS C_COUNTRY_ID, C_COUNTRY_NAME AS C_COUNTRY_NAME FROM DUAL) C_GEO
            ON (SA_GEO.CITY_ID = C_GEO.C_CITY_ID AND SA_GEO.STATE_ID = C_GEO.C_STATE_ID
                AND SA_GEO.COUNTRY_ID = C_GEO.C_COUNTRY_ID)
        WHEN MATCHED THEN UPDATE SET SA_GEO.CITY_NAME = C_GEO.C_CITY_NAME,
                                     SA_GEO.STATE_NAME = C_GEO.C_STATE_NAME,
                                     SA_GEO.COUNTRY_NAME = C_GEO.C_COUNTRY_NAME,
                                     SA_GEO.INSERT_DATE = CURRENT_DATE
                                WHERE 
                                    SA_GEO.CITY_NAME <> C_GEO.C_CITY_NAME OR
                                     SA_GEO.STATE_NAME <> C_GEO.C_STATE_NAME OR
                                     SA_GEO.COUNTRY_NAME <> C_GEO.C_COUNTRY_NAME

        WHEN NOT MATCHED THEN INSERT (SA_GEO.CITY_ID, SA_GEO.CITY_NAME, 
                                      SA_GEO.STATE_ID, SA_GEO.STATE_NAME,
                                      SA_GEO.COUNTRY_ID, SA_GEO.COUNTRY_NAME, 
                                      SA_GEO.INSERT_DATE)
            VALUES (C_GEO.C_CITY_ID, C_GEO.C_CITY_NAME, C_GEO.C_STATE_ID, 
                    C_GEO.C_STATE_NAME, C_GEO.C_COUNTRY_ID, C_GEO.C_COUNTRY_NAME,
                    CURRENT_DATE);
        VAR_COUNTER := VAR_COUNTER + SQL%ROWCOUNT;
    END LOOP;
    CLOSE CURR_GEO_DATA;
    
    LOGGER_SA_RETAIL(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'SA_OLIST_GEO',
                IN_OPERATION_STATUS => 'FINISH',
                IN_ROW_HANDLED => VAR_COUNTER
                );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER_SA_RETAIL(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_OLIST_GEO',
                    IN_OPERATION_STATUS => 'ERROR',
                    IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                    );
        ROLLBACK;
        RAISE;

END; 

--------------------------------------------------------------------------------
PROCEDURE LD_SA_PAYMENTS_TYPES
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE_SA_RETAIL.NEXTVAL;
    VAR_COUNTER NUMBER:= 0;
    
BEGIN
    LOGGER_SA_RETAIL(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_PAYMENTS_TYPES',
                    IN_OPERATION_STATUS => 'BEGIN'
                    );
    FOR CURR IN (SELECT PAYMENT_TYPE_ID, PAYMENT_TYPE_NAME FROM EXT_PAYMENTS_TYPES)
    LOOP
    
        EXECUTE IMMEDIATE '
        MERGE INTO SA_PAYMENTS_TYPES SA_PAY
            USING (SELECT :1 AS PAYMENT_TYPE_ID, :2 AS PAYMENT_TYPE_NAME FROM DUAL) C_PAY
            ON (SA_PAY.PAYMENT_TYPE_ID = C_PAY.PAYMENT_TYPE_ID)
        WHEN MATCHED THEN UPDATE SET SA_PAY.PAYMENT_TYPE_NAME = C_PAY.PAYMENT_TYPE_NAME,
                                     SA_PAY.INSERT_DATE = CURRENT_DATE
                                 WHERE SA_PAY.PAYMENT_TYPE_NAME <> C_PAY.PAYMENT_TYPE_NAME
        
        WHEN NOT MATCHED THEN INSERT (SA_PAY.PAYMENT_TYPE_ID, SA_PAY.PAYMENT_TYPE_NAME,
                                      SA_PAY.INSERT_DATE)
            VALUES (C_PAY.PAYMENT_TYPE_ID, C_PAY.PAYMENT_TYPE_NAME, CURRENT_DATE)'
        USING CURR.PAYMENT_TYPE_ID, CURR.PAYMENT_TYPE_NAME;
        VAR_COUNTER := VAR_COUNTER + SQL%ROWCOUNT;
    END LOOP;
    
    LOGGER_SA_RETAIL(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'SA_PAYMENTS_TYPES',
                IN_OPERATION_STATUS => 'FINISH',
                IN_ROW_HANDLED => VAR_COUNTER
                );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER_SA_RETAIL(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_PAYMENTS_TYPES',
                    IN_OPERATION_STATUS => 'ERROR',
                    IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                    );
        ROLLBACK;
        RAISE;

END; 

--------------------------------------------------------------------------------
PROCEDURE LD_SA_CUSTOMERS_RETAIL
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE_SA_RETAIL.NEXTVAL;
    
BEGIN
    LOGGER_SA_RETAIL(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_CUSTOMERS_RETAIL',
                    IN_OPERATION_STATUS => 'BEGIN'
                    );

    MERGE INTO SA_CUSTOMERS_RETAIL SA_CUST
        USING (
                SELECT 
                    CUSTOMER_ID,
                    TO_NUMBER(CUSTOMER_ID_NUM) AS CUSTOMER_ID_NUM,
                    CUSTOMER_UNIQUE_ID,
                    TO_NUMBER(CUSTOMER_UNIQUE_ID_NUM) AS CUSTOMER_UNIQUE_ID_NUM,
                    CUSTOMER_CITY,
                    CUSTOMER_STATE,
                    FIRST_NAME,
                    LAST_NAME
                FROM EXT_CUSTOMERS_RETAIL) EXT_CUST
        ON (SA_CUST.CUSTOMER_ID = EXT_CUST.CUSTOMER_ID)
    WHEN MATCHED THEN UPDATE SET 
                        SA_CUST.CUSTOMER_CITY = EXT_CUST.CUSTOMER_CITY,
                        SA_CUST.CUSTOMER_STATE = EXT_CUST.CUSTOMER_STATE,
                        SA_CUST.FIRST_NAME = EXT_CUST.FIRST_NAME,
                        SA_CUST.LAST_NAME = EXT_CUST.LAST_NAME,
                        SA_CUST.INSERT_DATE = CURRENT_DATE
                            WHERE 
                        SA_CUST.CUSTOMER_CITY <> EXT_CUST.CUSTOMER_CITY OR 
                        SA_CUST.CUSTOMER_STATE <> EXT_CUST.CUSTOMER_STATE OR 
                        SA_CUST.FIRST_NAME <> EXT_CUST.FIRST_NAME OR
                        SA_CUST.LAST_NAME <> EXT_CUST.LAST_NAME
    WHEN NOT MATCHED THEN INSERT (
                                    SA_CUST.CUSTOMER_ID,
                                    SA_CUST.CUSTOMER_ID_NUM,
                                    SA_CUST.CUSTOMER_UNIQUE_ID,
                                    SA_CUST.CUSTOMER_UNIQUE_ID_NUM,
                                    SA_CUST.CUSTOMER_CITY,
                                    SA_CUST.CUSTOMER_STATE,
                                    SA_CUST.FIRST_NAME,
                                    SA_CUST.LAST_NAME,
                                    SA_CUST.INSERT_DATE
                                    )
        VALUES (
                    EXT_CUST.CUSTOMER_ID,
                    EXT_CUST.CUSTOMER_ID_NUM,
                    EXT_CUST.CUSTOMER_UNIQUE_ID,
                    EXT_CUST.CUSTOMER_UNIQUE_ID_NUM,
                    EXT_CUST.CUSTOMER_CITY,
                    EXT_CUST.CUSTOMER_STATE,
                    EXT_CUST.FIRST_NAME,
                    EXT_CUST.LAST_NAME,
                    CURRENT_DATE
                );
    
    LOGGER_SA_RETAIL(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'SA_CUSTOMERS_RETAIL',
                IN_OPERATION_STATUS => 'FINISH',
                IN_ROW_HANDLED => SQL%ROWCOUNT
                );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER_SA_RETAIL(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_CUSTOMERS_RETAIL',
                    IN_OPERATION_STATUS => 'ERROR',
                    IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                    );
        ROLLBACK;
        RAISE;

END;

--------------------------------------------------------------------------------
PROCEDURE LD_SA_LOGISTIC_PARTNERS
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE_SA_RETAIL.NEXTVAL;
    
BEGIN
    LOGGER_SA_RETAIL(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_LOGISTIC_PARTNERS',
                    IN_OPERATION_STATUS => 'BEGIN'
                    );

    MERGE INTO SA_LOGISTIC_PARTNERS SA_LP
        USING (
                SELECT 
                    TO_NUMBER(LOGISTIC_PARTNER_ID) AS LOGISTIC_PARTNER_ID,
                    TO_NUMBER(REGEXP_REPLACE(CITY_ID, '[^[:digit:]]', '')) AS CITY_ID,
                    LOGISTIC_PARTNER_NAME
                FROM EXT_LOGISTIC_PARTNERS) EXT_LP
        ON (SA_LP.LOGISTIC_PARTNER_ID = EXT_LP.LOGISTIC_PARTNER_ID)
    WHEN MATCHED THEN UPDATE SET 
                        SA_LP.CITY_ID = EXT_LP.CITY_ID,
                        SA_LP.LOGISTIC_PARTNER_NAME = EXT_LP.LOGISTIC_PARTNER_NAME,
                        SA_LP.INSERT_DATE = CURRENT_DATE
                            WHERE 
                        SA_LP.CITY_ID <> EXT_LP.CITY_ID OR
                        SA_LP.LOGISTIC_PARTNER_NAME <> EXT_LP.LOGISTIC_PARTNER_NAME
    WHEN NOT MATCHED THEN INSERT (
                                    SA_LP.LOGISTIC_PARTNER_ID,
                                    SA_LP.CITY_ID,
                                    SA_LP.LOGISTIC_PARTNER_NAME,
                                    SA_LP.INSERT_DATE
                                    )
        VALUES (
                    EXT_LP.LOGISTIC_PARTNER_ID,
                    EXT_LP.CITY_ID,
                    EXT_LP.LOGISTIC_PARTNER_NAME,
                    CURRENT_DATE
                );
    
    LOGGER_SA_RETAIL(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'SA_LOGISTIC_PARTNERS',
                IN_OPERATION_STATUS => 'FINISH',
                IN_ROW_HANDLED => SQL%ROWCOUNT
                );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER_SA_RETAIL(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_LOGISTIC_PARTNERS',
                    IN_OPERATION_STATUS => 'ERROR',
                    IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                    );
        ROLLBACK;
        RAISE;

END;

--------------------------------------------------------------------------------
PROCEDURE LD_SA_SUPPLIERS
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE_SA_RETAIL.NEXTVAL;
    
BEGIN
    LOGGER_SA_RETAIL(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_OLIST_SUPPLIERS_DATASET',
                    IN_OPERATION_STATUS => 'BEGIN'
                    );

    MERGE INTO SA_OLIST_SUPPLIERS_DATASET SA_SUPP
        USING (SELECT 
                    SUPPLIER_ID,
                    SUPPLIER_CITY,
                    SUPPLIER_STATE,
                    SUPPLIER_NAME,
                    SECTOR
                FROM EXT_OLIST_SUPPLIERS_DATASET) EXT_SUPP
        ON (SA_SUPP.SUPPLIER_ID = EXT_SUPP.SUPPLIER_ID)
    WHEN MATCHED THEN UPDATE SET 
                        SA_SUPP.SUPPLIER_CITY = EXT_SUPP.SUPPLIER_CITY,
                        SA_SUPP.SUPPLIER_STATE = EXT_SUPP.SUPPLIER_STATE,
                        SA_SUPP.SUPPLIER_NAME = EXT_SUPP.SUPPLIER_NAME,
                        SA_SUPP.SECTOR = EXT_SUPP.SECTOR,
                        SA_SUPP.INSERT_DATE = CURRENT_DATE
                            WHERE 
                        SA_SUPP.SUPPLIER_CITY <> EXT_SUPP.SUPPLIER_CITY OR
                        SA_SUPP.SUPPLIER_STATE <> EXT_SUPP.SUPPLIER_STATE OR
                        SA_SUPP.SUPPLIER_NAME <> EXT_SUPP.SUPPLIER_NAME OR
                        SA_SUPP.SECTOR <> EXT_SUPP.SECTOR
                        
    WHEN NOT MATCHED THEN INSERT (
                                    SA_SUPP.SUPPLIER_ID,
                                    SA_SUPP.SUPPLIER_CITY,
                                    SA_SUPP.SUPPLIER_STATE,
                                    SA_SUPP.SUPPLIER_NAME,
                                    SA_SUPP.SECTOR,
                                    SA_SUPP.INSERT_DATE
                                    )
        VALUES (
                EXT_SUPP.SUPPLIER_ID,
                EXT_SUPP.SUPPLIER_CITY,
                EXT_SUPP.SUPPLIER_STATE,
                EXT_SUPP.SUPPLIER_NAME,
                EXT_SUPP.SECTOR,
                CURRENT_DATE
                );
    
    LOGGER_SA_RETAIL(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'SA_OLIST_SUPPLIERS_DATASET',
                IN_OPERATION_STATUS => 'FINISH',
                IN_ROW_HANDLED => SQL%ROWCOUNT
                );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER_SA_RETAIL(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_OLIST_SUPPLIERS_DATASET',
                    IN_OPERATION_STATUS => 'ERROR',
                    IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                    );
        ROLLBACK;
        RAISE;

END;

-------------------------------------------------------------------------------
PROCEDURE LD_SA_PRODUCTS
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE_SA_RETAIL.NEXTVAL;
    
BEGIN
    LOGGER_SA_RETAIL(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_PRODUCTS',
                    IN_OPERATION_STATUS => 'BEGIN'
                    );

    MERGE INTO SA_PRODUCTS SA_P
        USING (
                SELECT
                    PRODUCT_ID, 
                    TO_NUMBER(PRODUCT_ID_NUM) AS PRODUCT_ID_NUM, 
                    PRODUCT_NAME, 
                    TO_NUMBER(PRODUCT_CATEGORY_ID) AS PRODUCT_CATEGORY_ID, 
                    NVL(PRODUCT_CATEGORY_NAME_ENGLISH, 'N/A') AS PRODUCT_CATEGORY_NAME_ENGLISH,
                    TO_NUMBER(NVL(PRODUCT_CATEGORY_MARGIN_RATE, '0.00'), '999999.99') AS PRODUCT_CATEGORY_MARGIN_RATE,
                    TO_NUMBER(NVL(PRODUCT_WEIGHT_G, '0'), '999999.99') AS PRODUCT_WEIGHT_G,
                    TO_NUMBER(NVL(PRODUCT_LENGTH_CM, '0'), '999999.99') AS PRODUCT_LENGTH_CM,
                    TO_NUMBER(NVL(PRODUCT_HEIGHT_CM, '0'), '999999.99') AS PRODUCT_HEIGHT_CM,
                    TO_NUMBER(NVL(PRODUCT_LENGTH_CM, '0'), '999999.99') AS PRODUCT_WIDTH_CM
                FROM EXT_PRODUCTS
                ) EXT_P
        ON (SA_P.PRODUCT_ID = EXT_P.PRODUCT_ID)
    WHEN MATCHED THEN UPDATE SET 
                        SA_P.PRODUCT_ID_NUM = EXT_P.PRODUCT_ID_NUM, 
                        SA_P.PRODUCT_NAME = EXT_P.PRODUCT_NAME, 
                        SA_P.PRODUCT_CATEGORY_ID = EXT_P.PRODUCT_CATEGORY_ID, 
                        SA_P.PRODUCT_CATEGORY_NAME_ENGLISH = EXT_P.PRODUCT_CATEGORY_NAME_ENGLISH, 
                        SA_P.PRODUCT_CATEGORY_MARGIN_RATE = EXT_P.PRODUCT_CATEGORY_MARGIN_RATE, 
                        SA_P.PRODUCT_WEIGHT_G = EXT_P.PRODUCT_WEIGHT_G, 
                        SA_P.PRODUCT_LENGTH_CM = EXT_P.PRODUCT_LENGTH_CM, 
                        SA_P.PRODUCT_HEIGHT_CM = EXT_P.PRODUCT_HEIGHT_CM, 
                        SA_P.PRODUCT_WIDTH_CM = EXT_P.PRODUCT_WIDTH_CM,
                        SA_P.INSERT_DATE = CURRENT_DATE
                            WHERE 
                        SA_P.PRODUCT_ID_NUM <> EXT_P.PRODUCT_ID_NUM OR 
                        SA_P.PRODUCT_NAME <> EXT_P.PRODUCT_NAME OR
                        SA_P.PRODUCT_CATEGORY_ID <> EXT_P.PRODUCT_CATEGORY_ID OR
                        SA_P.PRODUCT_CATEGORY_NAME_ENGLISH <> EXT_P.PRODUCT_CATEGORY_NAME_ENGLISH OR 
                        SA_P.PRODUCT_CATEGORY_MARGIN_RATE <> EXT_P.PRODUCT_CATEGORY_MARGIN_RATE OR
                        SA_P.PRODUCT_WEIGHT_G <> EXT_P.PRODUCT_WEIGHT_G OR
                        SA_P.PRODUCT_LENGTH_CM <> EXT_P.PRODUCT_LENGTH_CM OR
                        SA_P.PRODUCT_HEIGHT_CM <> EXT_P.PRODUCT_HEIGHT_CM OR 
                        SA_P.PRODUCT_WIDTH_CM <> EXT_P.PRODUCT_WIDTH_CM

    WHEN NOT MATCHED THEN INSERT (
                                    SA_P.PRODUCT_ID, 
                                    SA_P.PRODUCT_ID_NUM, 
                                    SA_P.PRODUCT_NAME, 
                                    SA_P.PRODUCT_CATEGORY_ID, 
                                    SA_P.PRODUCT_CATEGORY_NAME_ENGLISH, 
                                    SA_P.PRODUCT_CATEGORY_MARGIN_RATE, 
                                    SA_P.PRODUCT_WEIGHT_G, 
                                    SA_P.PRODUCT_LENGTH_CM, 
                                    SA_P.PRODUCT_HEIGHT_CM, 
                                    SA_P.PRODUCT_WIDTH_CM,
                                    SA_P.INSERT_DATE
                                 )
        VALUES (
                    EXT_P.PRODUCT_ID, 
                    EXT_P.PRODUCT_ID_NUM, 
                    EXT_P.PRODUCT_NAME, 
                    EXT_P.PRODUCT_CATEGORY_ID, 
                    EXT_P.PRODUCT_CATEGORY_NAME_ENGLISH, 
                    EXT_P.PRODUCT_CATEGORY_MARGIN_RATE, 
                    EXT_P.PRODUCT_WEIGHT_G, 
                    EXT_P.PRODUCT_LENGTH_CM, 
                    EXT_P.PRODUCT_HEIGHT_CM, 
                    EXT_P.PRODUCT_WIDTH_CM,
                    CURRENT_DATE
                );
    
    LOGGER_SA_RETAIL(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'SA_PRODUCTS',
                IN_OPERATION_STATUS => 'FINISH',
                IN_ROW_HANDLED => SQL%ROWCOUNT
                );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER_SA_RETAIL(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_PRODUCTS',
                    IN_OPERATION_STATUS => 'ERROR',
                    IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                    );
        ROLLBACK;
        RAISE;

END;

--------------------------------------------------------------------------------
PROCEDURE LD_SA_SALES
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE_SA_RETAIL.NEXTVAL;
    
BEGIN
    LOGGER_SA_RETAIL(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_SALES',
                    IN_OPERATION_STATUS => 'BEGIN'
                    );

    MERGE INTO SA_SALES SA_S
        USING (
                SELECT
                    ORDER_ID, 
                    PAYMENT_TYPE, 
                    PRODUCT_ID, 
                    CUSTOMER_ID, 
                    SUPPLIER_ID, 
                    TO_NUMBER(LOGISTIC_PARTNER_ID) AS LOGISTIC_PARTNER_ID, 
                    TO_NUMBER(PAYMENT_VALUE, '99999.99') AS PAYMENT_VALUE, 
                    TO_NUMBER(COST_VALUE, '99999.99') AS COST_VALUE, 
                    TO_DATE(PURCHASE_DATE, 'YYYY-MM-DD') AS PURCHASE_DATE
                FROM EXT_SALES
              ) EXT_S
        ON (SA_S.ORDER_ID = EXT_S.ORDER_ID)
    WHEN MATCHED THEN UPDATE SET             
                    SA_S.PAYMENT_TYPE = EXT_S.PAYMENT_TYPE, 
                    SA_S.PRODUCT_ID = EXT_S.PRODUCT_ID, 
                    SA_S.CUSTOMER_ID = EXT_S.CUSTOMER_ID, 
                    SA_S.SUPPLIER_ID = EXT_S.SUPPLIER_ID, 
                    SA_S.LOGISTIC_PARTNER_ID = EXT_S.LOGISTIC_PARTNER_ID, 
                    SA_S.PAYMENT_VALUE = EXT_S.PAYMENT_VALUE, 
                    SA_S.COST_VALUE = EXT_S.COST_VALUE, 
                    SA_S.PURCHASE_DATE = EXT_S.PURCHASE_DATE,
                    SA_S.INSERT_DATE = CURRENT_DATE
                        WHERE 
                    SA_S.PAYMENT_TYPE <> EXT_S.PAYMENT_TYPE OR
                    SA_S.PRODUCT_ID <> EXT_S.PRODUCT_ID OR
                    SA_S.CUSTOMER_ID <> EXT_S.CUSTOMER_ID OR 
                    SA_S.SUPPLIER_ID <> EXT_S.SUPPLIER_ID OR
                    SA_S.LOGISTIC_PARTNER_ID <> EXT_S.LOGISTIC_PARTNER_ID OR 
                    SA_S.PAYMENT_VALUE <> EXT_S.PAYMENT_VALUE OR
                    SA_S.COST_VALUE <> EXT_S.COST_VALUE OR
                    SA_S.PURCHASE_DATE <> EXT_S.PURCHASE_DATE

    WHEN NOT MATCHED THEN INSERT (
                                    SA_S.ORDER_ID, 
                                    SA_S.PAYMENT_TYPE, 
                                    SA_S.PRODUCT_ID, 
                                    SA_S.CUSTOMER_ID, 
                                    SA_S.SUPPLIER_ID, 
                                    SA_S.LOGISTIC_PARTNER_ID, 
                                    SA_S.PAYMENT_VALUE, 
                                    SA_S.COST_VALUE, 
                                    SA_S.PURCHASE_DATE,
                                    SA_S.INSERT_DATE
                                  )
        VALUES (
                    EXT_S.ORDER_ID, 
                    EXT_S.PAYMENT_TYPE, 
                    EXT_S.PRODUCT_ID, 
                    EXT_S.CUSTOMER_ID, 
                    EXT_S.SUPPLIER_ID, 
                    EXT_S.LOGISTIC_PARTNER_ID, 
                    EXT_S.PAYMENT_VALUE, 
                    EXT_S.COST_VALUE, 
                    EXT_S.PURCHASE_DATE,
                    CURRENT_DATE
                );
    
    LOGGER_SA_RETAIL(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'SA_SALES',
                IN_OPERATION_STATUS => 'FINISH',
                IN_ROW_HANDLED => SQL%ROWCOUNT
                );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER_SA_RETAIL(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_SALES',
                    IN_OPERATION_STATUS => 'ERROR',
                    IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                    );
        ROLLBACK;
        RAISE;

END;



END PKG_ETL_SA_RETAIL;