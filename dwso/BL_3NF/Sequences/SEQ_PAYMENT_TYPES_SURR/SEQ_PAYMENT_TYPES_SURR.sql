CREATE SEQUENCE BL_3NF.SEQ_PAYMENT_TYPES_SURR
    START WITH 1;
    
CREATE OR REPLACE PUBLIC SYNONYM SEQ_PAYMENT_TYPES_SURR FOR BL_3NF.SEQ_PAYMENT_TYPES_SURR;
GRANT SELECT ON BL_3NF.SEQ_PAYMENT_TYPES_SURR TO BL_CL;