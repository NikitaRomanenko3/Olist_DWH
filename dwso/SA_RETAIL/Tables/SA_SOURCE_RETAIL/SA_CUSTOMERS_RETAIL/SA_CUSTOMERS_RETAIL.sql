CREATE TABLE SA_CUSTOMERS_RETAIL (
  CUSTOMER_ID	          VARCHAR2(250),
  CUSTOMER_ID_NUM         NUMBER,
  CUSTOMER_UNIQUE_ID	  VARCHAR2(250),
  CUSTOMER_UNIQUE_ID_NUM  NUMBER,
  CUSTOMER_CITY	          VARCHAR2(250),
  CUSTOMER_STATE          VARCHAR2(250),
  FIRST_NAME              VARCHAR2(250),
  LAST_NAME               VARCHAR2(250),
  INSERT_DATE   DATE DEFAULT CURRENT_DATE
);

GRANT SELECT, INSERT ON SA_CUSTOMERS_RETAIL TO BL_CL;

CREATE OR REPLACE PUBLIC SYNONYM SA_CUSTOMERS_RETAIL FOR SA_SOURCE_SYSTEM_RETAIL.SA_CUSTOMERS_RETAIL;