CREATE TABLE SA_SALES_BUSINESS (
  ORDER_ID		       VARCHAR2(250),
  PAYMENT_TYPE         VARCHAR2(250),
  PRODUCT_ID	       VARCHAR2(250),
  CUSTOMER_ID	       VARCHAR2(250),
  SUPPLIER_ID          VARCHAR2(250),
  LOGISTIC_PARTNER_ID  NUMBER,
  PAYMENT_VALUE	       NUMBER,
  COST_VALUE           NUMBER,
  PURCHASE_DATE        DATE,
  INSERT_DATE   DATE DEFAULT CURRENT_DATE
);

CREATE OR REPLACE PUBLIC SYNONYM SA_SALES_BUSINESS FOR SA_SOURCE_SYSTEM_BUSINESS.SA_SALES_BUSINESS;
GRANT SELECT, INSERT, UPDATE, DELETE ON SA_SALES_BUSINESS TO BL_CL;