CREATE SEQUENCE BL_3NF.SEQ_SALES_SURR
    START WITH 1;
    
CREATE OR REPLACE PUBLIC SYNONYM SEQ_SALES_SURR FOR BL_3NF.SEQ_SALES_SURR;
GRANT SELECT ON BL_3NF.SEQ_SALES_SURR TO BL_CL;