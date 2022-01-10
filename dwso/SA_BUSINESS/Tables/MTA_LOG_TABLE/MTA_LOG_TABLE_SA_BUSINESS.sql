CREATE TABLE MTA_LOG_TABLE_SA_BUSINESS (
    LOG_ID            NUMBER,
    USER_NAME         VARCHAR2(200),
    PACKAGE_NAME      VARCHAR2(200),
    PROCEDURE_NAME    VARCHAR2(200),
    OPERATION_TYPE    VARCHAR2(200),
    TARGET_TABLE      VARCHAR2(200),
    OPERATION_DATE    TIMESTAMP,
    OPERATION_STATUS  VARCHAR2(200),
    ERROR_NAME        VARCHAR2(2000),
    ROWS_HANDLED      NUMBER
);
