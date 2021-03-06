INSERT INTO SA_SOURCE_SYSTEM_RETAIL.SA_PRODUCTS 
(
    PRODUCT_ID, 
	PRODUCT_ID_NUM, 
	PRODUCT_NAME, 
	PRODUCT_CATEGORY_ID, 
	PRODUCT_CATEGORY_NAME_ENGLISH, 
	PRODUCT_CATEGORY_MARGIN_RATE, 
	PRODUCT_WEIGHT_G, 
	PRODUCT_LENGTH_CM, 
	PRODUCT_HEIGHT_CM, 
	PRODUCT_WIDTH_CM
)
SELECT
    PRODUCT_ID, 
	TO_NUMBER(PRODUCT_ID_NUM), 
	PRODUCT_NAME, 
	TO_NUMBER(PRODUCT_CATEGORY_ID), 
	NVL(PRODUCT_CATEGORY_NAME_ENGLISH, 'N/A'),
	TO_NUMBER(NVL(PRODUCT_CATEGORY_MARGIN_RATE, '0.00'), '999999.99'),
    TO_NUMBER(NVL(PRODUCT_WEIGHT_G, '0'), '999999.99'),
    TO_NUMBER(NVL(PRODUCT_LENGTH_CM, '0'), '999999.99'),
	TO_NUMBER(NVL(PRODUCT_HEIGHT_CM, '0'), '999999.99'),
    TO_NUMBER(NVL(PRODUCT_LENGTH_CM, '0'), '999999.99')
FROM SA_SOURCE_SYSTEM_RETAIL.EXT_PRODUCTS;

COMMIT;