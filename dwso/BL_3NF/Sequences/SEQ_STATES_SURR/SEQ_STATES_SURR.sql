CREATE SEQUENCE BL_3NF.SEQ_STATES_SURR
    START WITH 1;
    
CREATE OR REPLACE PUBLIC SYNONYM SEQ_STATES_SURR FOR BL_3NF.SEQ_STATES_SURR;
GRANT SELECT ON BL_3NF.SEQ_STATES_SURR TO BL_CL;