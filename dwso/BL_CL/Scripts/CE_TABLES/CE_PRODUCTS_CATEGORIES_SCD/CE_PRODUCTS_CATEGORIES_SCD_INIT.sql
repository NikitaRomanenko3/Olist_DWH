INSERT INTO BL_3NF.CE_PRODUCTS_CATEGORIES_SCD
(
    PRODUCT_CATEGORY_ID, 
	PRODUCT_CATEGORY_SRCID, 
	SOURCE_SYSTEM, 
	SOURCE_TABLE, 
	PRODUCT_CATEGORY_NAME, 
	PRODUCT_CATEGORY_MARGIN_RATE, 
	START_DT, 
	END_DT, 
	IS_ACTIVE, 
	INSERT_DT
)
VALUES (-1, -99, 'N/A', 'N/A', 'N/A', -99, TO_DATE('1990-01-01', 'YYYY-MM-DD'),
TO_DATE('9999-12-31', 'YYYY-MM-DD'), 'N/A', TO_DATE('1990-01-01', 'YYYY-MM-DD'))
;

COMMIT;