CREATE OR REPLACE PROCEDURE LOGGER_SA_BUSINESS (
                                    IN_LOG_ID NUMBER,
                                    IN_OPERATION_TYPE IN VARCHAR2 DEFAULT NULL,
                                    IN_TARGET_TABLE IN VARCHAR2 DEFAULT NULL,
                                    IN_OPERATION_STATUS IN VARCHAR2 DEFAULT NULL,
                                    IN_ERROR_NAME IN VARCHAR2 DEFAULT NULL,
                                    IN_ROW_HANDLED IN NUMBER DEFAULT NULL)
IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    VAR_PACKAGE VARCHAR2(3000) := UTL_CALL_STACK.SUBPROGRAM(2)(1); 
    VAR_PROCEDURE VARCHAR2(3000) := UTL_CALL_STACK.SUBPROGRAM(2)(2); 
    
BEGIN
    INSERT INTO MTA_LOG_TABLE_SA_BUSINESS ( 
                                LOG_ID,
                                USER_NAME,
                                PACKAGE_NAME,
                                PROCEDURE_NAME,
                                OPERATION_TYPE,
                                TARGET_TABLE,
                                OPERATION_DATE,
                                OPERATION_STATUS,
                                ERROR_NAME,
                                ROWS_HANDLED
                                )
    VALUES (IN_LOG_ID,
            USER,
            VAR_PACKAGE,
            VAR_PROCEDURE,
            IN_OPERATION_TYPE,
            IN_TARGET_TABLE,
            CURRENT_TIMESTAMP,
            IN_OPERATION_STATUS,
            IN_ERROR_NAME,
            IN_ROW_HANDLED
            );
    COMMIT;
END;