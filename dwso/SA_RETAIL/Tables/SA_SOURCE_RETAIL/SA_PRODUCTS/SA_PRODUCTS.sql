CREATE TABLE SA_PRODUCTS (
  PRODUCT_ID		             VARCHAR2(250),
  PRODUCT_ID_NUM                 NUMBER,
  PRODUCT_NAME	                 VARCHAR2(250),
  PRODUCT_CATEGORY_ID            NUMBER,
  PRODUCT_CATEGORY_NAME_ENGLISH  VARCHAR2(250),
  PRODUCT_CATEGORY_MARGIN_RATE   NUMBER,
  PRODUCT_WEIGHT_G	             NUMBER,
  PRODUCT_LENGTH_CM              NUMBER,
  PRODUCT_HEIGHT_CM              NUMBER,
  PRODUCT_WIDTH_CM               NUMBER,
  INSERT_DATE   DATE DEFAULT CURRENT_DATE
);

CREATE OR REPLACE PUBLIC SYNONYM SA_PRODUCTS FOR SA_SOURCE_SYSTEM_RETAIL.SA_PRODUCTS;

GRANT SELECT, INSERT ON SA_PRODUCTS TO BL_CL;