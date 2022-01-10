CREATE OR REPLACE VIEW INCR_CE_PRODUCTS_CATEGORIES AS 
(
    SELECT *
    FROM SA_PRODUCTS
    WHERE INSERT_DATE > (
                            SELECT
                                PREVIOUS_LOADED_DATE
                            FROM PRM_MTA_INCREMENTAL_LOAD
                            WHERE SA_TABLE_NAME = 'SA_PRODUCTS' AND 
                                TARGET_TABLE_NAME = 'CE_PRODUCTS_CATEGORIES_SCD'
                        )
);