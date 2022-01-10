CREATE OR REPLACE PACKAGE BODY PKG_ETL_SA_BUSINESS AS 
--------------------------------------------------------------------------------
PROCEDURE LD_SA_CUSTOMERS_BUSINESS
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE_SA_BUSINESS.NEXTVAL;
    
BEGIN
    LOGGER_SA_BUSINESS(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_CUSTOMERS_BUSINESS',
                    IN_OPERATION_STATUS => 'BEGIN'
                    );

    MERGE INTO SA_CUSTOMERS_BUSINESS SA_CUST
        USING (SELECT
                    TO_NUMBER(CUSTOMER_ID) AS CUSTOMER_ID,
                    COMPANY_NAME,
                    COMPANY_CITY,
                    COMPANY_STATE
                FROM EXT_CUSTOMERS_BUSINESS
                ) EXT_CUST
        ON (SA_CUST.CUSTOMER_ID = EXT_CUST.CUSTOMER_ID)
    WHEN MATCHED THEN UPDATE SET 
                        SA_CUST.COMPANY_NAME = EXT_CUST.COMPANY_NAME,
                        SA_CUST.COMPANY_CITY = EXT_CUST.COMPANY_CITY,
                        SA_CUST.COMPANY_STATE = EXT_CUST.COMPANY_STATE,
                        SA_CUST.INSERT_DATE = CURRENT_DATE
                            WHERE 
                        SA_CUST.COMPANY_NAME <> EXT_CUST.COMPANY_NAME OR
                        SA_CUST.COMPANY_CITY <> EXT_CUST.COMPANY_CITY OR
                        SA_CUST.COMPANY_STATE <> EXT_CUST.COMPANY_STATE
    WHEN NOT MATCHED THEN INSERT (
                                    SA_CUST.CUSTOMER_ID, 
                                    SA_CUST.COMPANY_NAME, 
                                    SA_CUST.COMPANY_CITY, 
                                    SA_CUST.COMPANY_STATE,
                                    SA_CUST.INSERT_DATE
                                    )
        VALUES (
                    EXT_CUST.CUSTOMER_ID, 
                    EXT_CUST.COMPANY_NAME, 
                    EXT_CUST.COMPANY_CITY, 
                    EXT_CUST.COMPANY_STATE,
                    CURRENT_DATE    
                );
    
    LOGGER_SA_BUSINESS(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'SA_CUSTOMERS_BUSINESS',
                IN_OPERATION_STATUS => 'FINISH',
                IN_ROW_HANDLED => SQL%ROWCOUNT
                );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER_SA_BUSINESS(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_CUSTOMERS_BUSINESS',
                    IN_OPERATION_STATUS => 'ERROR',
                    IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                    );
        ROLLBACK;
        RAISE;

END;

--------------------------------------------------------------------------------
PROCEDURE LD_SA_SALES
IS  
    VAR_LOG_ID NUMBER := SEQ_LOG_TABLE_SA_BUSINESS.NEXTVAL;
    
BEGIN
    LOGGER_SA_BUSINESS(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_SALES',
                    IN_OPERATION_STATUS => 'BEGIN'
                    );

    MERGE INTO SA_SALES_BUSINESS SA_S
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
                FROM EXT_SALES_BUSINESS
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
    
    LOGGER_SA_BUSINESS(
                IN_LOG_ID => VAR_LOG_ID,
                IN_OPERATION_TYPE => 'MERGE',
                IN_TARGET_TABLE => 'SA_SALES',
                IN_OPERATION_STATUS => 'FINISH',
                IN_ROW_HANDLED => SQL%ROWCOUNT
                );
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN 
        LOGGER_SA_BUSINESS(
                    IN_LOG_ID => VAR_LOG_ID,
                    IN_OPERATION_TYPE => 'MERGE',
                    IN_TARGET_TABLE => 'SA_SALES',
                    IN_OPERATION_STATUS => 'ERROR',
                    IN_ERROR_NAME => 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM
                    );
        ROLLBACK;
        RAISE;

END;



END PKG_ETL_SA_BUSINESS;