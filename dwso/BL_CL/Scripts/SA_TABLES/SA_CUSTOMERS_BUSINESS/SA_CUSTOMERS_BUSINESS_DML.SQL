INSERT INTO SA_SOURCE_SYSTEM_BUSINESS.SA_CUSTOMERS_BUSINESS 
(
    CUSTOMER_ID, 
	COMPANY_NAME, 
	COMPANY_CITY, 
	COMPANY_STATE
)
SELECT 
    TO_NUMBER(CUSTOMER_ID),
    COMPANY_NAME,
    COMPANY_CITY,
    COMPANY_STATE
FROM SA_SOURCE_SYSTEM_BUSINESS.EXT_CUSTOMERS_BUSINESS;

COMMIT;

