ALTER SESSION SET NLS_DATE_LANGUAGE = ENGLISH;
ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD';

CREATE TABLE DIM_DATES AS (
SELECT 
    TO_DATE('01/01/1970', 'dd/mm/yyyy') + ROWNUM - 1 as EVENT_DT,
    TO_NUMBER(TO_CHAR(TO_DATE('01/01/1970', 'dd/mm/yyyy') + ROWNUM - 1, 'D')) AS DAY_OF_WEEK_NUMBER,
    TO_CHAR(TO_DATE('01/01/1970', 'dd/mm/yyyy') + ROWNUM - 1, 'DAY') AS DAY_OF_WEEK_NAME,
    EXTRACT(MONTH FROM TO_DATE('01/01/1970', 'dd/mm/yyyy') + ROWNUM - 1) AS MONTH_NUMBER,
    TO_CHAR(TO_DATE('01/01/1970', 'dd/mm/yyyy') + ROWNUM - 1, 'MONTH') AS MONTH_NAME,
     TO_NUMBER(TO_CHAR(TO_DATE('01/01/1970', 'dd/mm/yyyy') + ROWNUM - 1, 'Q')) AS QUARTER_NUMBER,
    EXTRACT(YEAR FROM TO_DATE('01/01/1970', 'dd/mm/yyyy') + ROWNUM - 1) AS YEAR_NUMBER
FROM dual
CONNECT BY LEVEL <= TO_DATE('01/01/2030', 'dd/mm/yyyy') - TO_DATE('01/01/1970', 'dd/mm/yyyy') + 1);

ALTER TABLE DIM_DATES ADD CONSTRAINT PK_DIM_DATES PRIMARY KEY (EVENT_DT);
GRANT SELECT, INSERT, UPDATE, DELETE ON DIM_DATES TO BL_CL;
CREATE PUBLIC SYNONYM DIM_DATES FOR BL_DM.DIM_DATES;