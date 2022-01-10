INSERT ALL 
    INTO PRM_MTA_INCREMENTAL_LOAD 
        (
            SA_TABLE_NAME,
            TARGET_TABLE_NAME,
            PACKAGE_NAME,
            PROCEDURE_NAME,
            PREVIOUS_LOADED_DATE
        )
    VALUES 
        ('SA_PRODUCTS', 'CE_PRODUCTS_SCD', 'PKG_ETL_CE', 'LD_CE_PRODUCTS_SCD',
        TO_DATE('1900-01-01', 'YYYY-MM-DD'))
--------------------------------------------------------------------------------
    INTO PRM_MTA_INCREMENTAL_LOAD 
        (
            SA_TABLE_NAME,
            TARGET_TABLE_NAME,
            PACKAGE_NAME,
            PROCEDURE_NAME,
            PREVIOUS_LOADED_DATE
        )
    VALUES 
        ('SA_SALES_RETAIL', 'CE_SALES', 'PKG_ETL_CE', 'LD_CE_SALES',
        TO_DATE('1900-01-01', 'YYYY-MM-DD'))
--------------------------------------------------------------------------------        
    INTO PRM_MTA_INCREMENTAL_LOAD 
        (
            SA_TABLE_NAME,
            TARGET_TABLE_NAME,
            PACKAGE_NAME,
            PROCEDURE_NAME,
            PREVIOUS_LOADED_DATE
        )
    VALUES 
        ('SA_SALES_BUSINESS', 'CE_SALES', 'PKG_ETL_CE', 'LD_CE_SALES',
        TO_DATE('1900-01-01', 'YYYY-MM-DD'))
--------------------------------------------------------------------------------
    INTO PRM_MTA_INCREMENTAL_LOAD 
        (
            SA_TABLE_NAME,
            TARGET_TABLE_NAME,
            PACKAGE_NAME,
            PROCEDURE_NAME,
            PREVIOUS_LOADED_DATE
        )
    VALUES 
        ('SA_PRODUCTS', 'CE_PRODUCTS_CATEGORIES_SCD', 'PKG_ETL_CE', 
        'LD_CE_PRODUCTS_CATEGORIES_SCD', TO_DATE('1900-01-01', 'YYYY-MM-DD'))


SELECT 1 FROM DUAL;
        