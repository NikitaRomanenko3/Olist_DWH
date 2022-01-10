INSERT INTO BL_3NF.CE_SALES
(
	SALE_SRCID, 
	SOURCE_SYSTEM, 
	SOURCE_TABLE, 
	PRODUCT_ID, 
	CUSTOMER_ID, 
	SUPPLIER_ID, 
	LOGISTIC_PARTNER_ID, 
	PAYMENT_TYPE_ID, 
	AMOUNT_VALUE, 
	COST_VALUE, 
	PURCHASE_DATE
)
WITH TEMP_Q AS 
(
    SELECT 
        MAX(INSERT_DT) AS MAX_DATE
    FROM BL_3NF.CE_SALES
)

SELECT
    ORDER_ID,
    'SA_SOURCE_SYSTEM_BUSINESS' AS SOURCE_SYSTEM,
    'SA_SALES_BUSINESS' AS SOURCE_TABLE,
    PROD.PRODUCT_ID,
    CUSTS.CUSTOMER_ID,
    SUPPLS.SUPPLIER_ID,
    LP.LOGISTIC_PARTNER_ID,
    PT.PAYMENT_TYPE_ID,
    PAYMENT_VALUE,
    COST_VALUE,
    PURCHASE_DATE
FROM SA_SOURCE_SYSTEM_BUSINESS.SA_SALES_BUSINESS SA
JOIN BL_3NF.CE_PRODUCTS_SCD PROD ON SA.PRODUCT_ID = PROD.PRODUCT_SRCID
JOIN BL_3NF.CE_CUSTOMERS CUSTS ON CUSTS.CUSTOMER_SRCID = SA.CUSTOMER_ID
JOIN BL_3NF.CE_SUPPLIERS SUPPLS ON SUPPLS.SUPPLIER_SRCID = SA.SUPPLIER_ID
JOIN BL_3NF.CE_LOGISTIC_PARTNERS LP ON LP.LOGISTIC_PARTNER_SRCID = SA.LOGISTIC_PARTNER_ID
JOIN BL_3NF.CE_PAYMENT_TYPES PT ON REGEXP_SUBSTR(PT.PAYMENT_TYPE_NAME,'[A-Za-z]+') = REGEXP_SUBSTR(SA.PAYMENT_TYPE,'[A-Za-z]+')
WHERE CUSTS.SOURCE_SYSTEM = 'SA_SOURCE_SYSTEM_BUSINESS' AND 
      CUSTS.SOURCE_TABLE = 'SA_CUSTOMERS_BUSINESS'
      AND SA.INSERT_DATE > (SELECT MAX_DATE FROM TEMP_Q)

UNION ALL

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
JOIN BL_3NF.CE_PRODUCTS_SCD PROD ON SA.PRODUCT_ID = PROD.PRODUCT_SRCID
JOIN BL_3NF.CE_CUSTOMERS CUSTS ON CUSTS.CUSTOMER_SRCID = SA.CUSTOMER_ID
JOIN BL_3NF.CE_SUPPLIERS SUPPLS ON SUPPLS.SUPPLIER_SRCID = SA.SUPPLIER_ID
JOIN BL_3NF.CE_LOGISTIC_PARTNERS LP ON LP.LOGISTIC_PARTNER_SRCID = SA.LOGISTIC_PARTNER_ID
JOIN BL_3NF.CE_PAYMENT_TYPES PT ON REGEXP_SUBSTR(PT.PAYMENT_TYPE_NAME,'[A-Za-z]+') = REGEXP_SUBSTR(SA.PAYMENT_TYPE,'[A-Za-z]+')
WHERE CUSTS.SOURCE_SYSTEM = 'SA_SOURCE_SYSTEM_RETAIL' AND 
      CUSTS.SOURCE_TABLE = 'SA_CUSTOMERS_RETAIL'
      AND SA.INSERT_DATE > (SELECT MAX_DATE FROM TEMP_Q)
;

COMMIT;