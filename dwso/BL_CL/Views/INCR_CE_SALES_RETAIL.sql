CREATE OR REPLACE VIEW INCR_CE_SALES_RETAIL AS 
(
    SELECT
        ORDER_ID,
        'SA_SOURCE_SYSTEM_RETAIL' AS SOURCE_SYSTEM,
        'SA_SALES' AS SOURCE_TABLE,
        PROD.PRODUCT_ID,
        CUSTS.CUSTOMER_ID,
        SUPPLS.SUPPLIER_ID,
        LP.LOGISTIC_PARTNER_ID,
        PT.PAYMENT_TYPE_ID,
        PAYMENT_VALUE,
        COST_VALUE,
        PURCHASE_DATE
    FROM SA_SOURCE_SYSTEM_RETAIL.SA_SALES SA
    LEFT JOIN BL_3NF.CE_PRODUCTS_SCD PROD ON SA.PRODUCT_ID = PROD.PRODUCT_SRCID AND PROD.IS_ACTIVE = 'Y'
    LEFT JOIN BL_3NF.CE_CUSTOMERS CUSTS ON CUSTS.CUSTOMER_SRCID = SA.CUSTOMER_ID
    LEFT JOIN BL_3NF.CE_SUPPLIERS SUPPLS ON SUPPLS.SUPPLIER_SRCID = SA.SUPPLIER_ID
    LEFT JOIN BL_3NF.CE_LOGISTIC_PARTNERS LP ON LP.LOGISTIC_PARTNER_SRCID = SA.LOGISTIC_PARTNER_ID
    LEFT JOIN BL_3NF.CE_PAYMENT_TYPES PT ON REGEXP_SUBSTR(PT.PAYMENT_TYPE_NAME,'[A-Za-z]+') = REGEXP_SUBSTR(SA.PAYMENT_TYPE,'[A-Za-z]+')
    WHERE CUSTS.SOURCE_SYSTEM = 'SA_SOURCE_SYSTEM_RETAIL' AND 
          CUSTS.SOURCE_TABLE = 'SA_CUSTOMERS_RETAIL'
          AND SA.INSERT_DATE > (
                                    SELECT
                                        PREVIOUS_LOADED_DATE
                                    FROM PRM_MTA_INCREMENTAL_LOAD
                                    WHERE SA_TABLE_NAME = 'SA_SALES_RETAIL' AND 
                                        TARGET_TABLE_NAME = 'CE_SALES'
                                )

);