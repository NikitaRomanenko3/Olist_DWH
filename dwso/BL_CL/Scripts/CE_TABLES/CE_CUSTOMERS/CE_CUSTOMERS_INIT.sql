INSERT INTO BL_3NF.CE_CUSTOMERS 
(
    CUSTOMER_ID, 
	CUSTOMER_SRCID, 
	SOURCE_SYSTEM, 
	SOURCE_TABLE, 
	CUSTOMER_FIRST_NAME, 
	CUSTOMER_LAST_NAME, 
	CUSTOMER_COMPANY_NAME, 
	CUSTOMER_CITY_ID, 
	IS_COMPANY, 
	INSERT_DT, 
	UPDATE_DT
)
VALUES (-1, '-99', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', -1, 'N/A', 
TO_DATE('1900-01-01', 'YYYY-MM-DD'), TO_DATE('1900-01-01', 'YYYY-MM-DD'));

COMMIT;



