INSERT INTO SA_SOURCE_SYSTEM_RETAIL.SA_OLIST_SUPPLIERS_DATASET 
(
    SUPPLIER_ID,
    SUPPLIER_CITY,
    SUPPLIER_STATE,
    SUPPLIER_NAME,
    SECTOR
)
SELECT
    SUPPLIER_ID,
    SUPPLIER_CITY,
    SUPPLIER_STATE,
    SUPPLIER_NAME,
    SECTOR
FROM SA_SOURCE_SYSTEM_RETAIL.EXT_OLIST_SUPPLIERS_DATASET;

COMMIT;