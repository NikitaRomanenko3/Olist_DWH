CREATE TABLE CE_LOGISTIC_PARTNERS (
    LOGISTIC_PARTNER_ID NUMBER,
    LOGISTIC_PARTNER_SRCID NUMBER,
    SOURCE_SYSTEM VARCHAR2(50) NOT NULL,
    SOURCE_TABLE VARCHAR2(50) NOT NULL,
    LOGISTIC_PARTNER_NAME VARCHAR2(50) NOT NULL,
    LOGISTIC_PARTNER_CITY_ID NUMBER,
    INSERT_DT DATE NOT NULL,
    UPDATE_DT DATE NOT NULL,
    CONSTRAINT PK_LOGISTIC_PARTNERS PRIMARY KEY(LOGISTIC_PARTNER_ID),
    CONSTRAINT FK_LOGISTIC_PARTNERS FOREIGN KEY (LOGISTIC_PARTNER_CITY_ID)
    REFERENCES CE_CITIES(CITY_ID)

);

GRANT SELECT, INSERT, UPDATE, DELETE ON BL_3NF.CE_LOGISTIC_PARTNERS TO BL_CL;
CREATE OR REPLACE PUBLIC SYNONYM CE_LOGISTIC_PARTNERS FOR BL_3NF.CE_LOGISTIC_PARTNERS;