CREATE TABLE DIM_PRODUCTS_SCD (
    PRODUCT_SURR_ID        NUMBER,
    SOURCE_SYSTEM          VARCHAR2(100) NOT NULL,
    SOURCE_TABLE           VARCHAR2(100) NOT NULL,
    PRODUCT_ID             NUMBER NOT NULL,
    PRODUCT_NAME           VARCHAR2(100) NOT NULL,
    PRODUCT_CATEGORY_ID    NUMBER NOT NULL,
    PRODUCT_CATEGORY_NAME  VARCHAR2(100) NOT NULL,
    PRODUCT_WEIGHT         NUMBER NOT NULL,
    PRODUCT_LENGTH         NUMBER NOT NULL,
    PRODUCT_HEIGHT         NUMBER NOT NULL,
    PRODUCT_WIDTH          NUMBER NOT NULL,
    START_DT               DATE NOT NULL,
    END_DT                 DATE NOT NULL,
    IS_ACTIVE              VARCHAR2(4) NOT NULL,
    INSERT_DT              DATE NOT NULL,
    CONSTRAINT PK_DIM_PRODUCTS_SCD PRIMARY KEY (PRODUCT_SURR_ID, START_DT)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON DIM_PRODUCTS_SCD TO BL_CL;
CREATE PUBLIC SYNONYM DIM_PRODUCTS_SCD FOR BL_DM.DIM_PRODUCTS_SCD;