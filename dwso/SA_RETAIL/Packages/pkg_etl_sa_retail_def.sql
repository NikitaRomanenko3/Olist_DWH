CREATE OR REPLACE PACKAGE PKG_ETL_SA_RETAIL AS 
    PROCEDURE LD_SA_GEO;
    PROCEDURE LD_SA_PAYMENTS_TYPES;
    PROCEDURE LD_SA_CUSTOMERS_RETAIL;
    PROCEDURE LD_SA_LOGISTIC_PARTNERS;
    PROCEDURE LD_SA_SUPPLIERS;
    PROCEDURE LD_SA_PRODUCTS;
    PROCEDURE LD_SA_SALES;
END PKG_ETL_SA_RETAIL;