CREATE TABLE SA_CUSTOMERS_BUSINESS (
  CUSTOMER_ID	          NUMBER,
  COMPANY_NAME            VARCHAR2(250),
  COMPANY_CITY	          VARCHAR2(250),
  COMPANY_STATE           VARCHAR2(250),
  INSERT_DATE             DATE DEFAULT CURRENT_DATE
);

CREATE OR REPLACE PUBLIC SYNONYM SA_CUSTOMERS_BUSINESS FOR SA_SOURCE_SYSTEM_BUSINESS.SA_CUSTOMERS_BUSINESS;
GRANT SELECT, INSERT, UPDATE, DELETE ON SA_CUSTOMERS_BUSINESS TO BL_CL;