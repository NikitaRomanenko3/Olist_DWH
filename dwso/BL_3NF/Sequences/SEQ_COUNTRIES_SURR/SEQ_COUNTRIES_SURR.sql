CREATE SEQUENCE BL_3NF.SEQ_COUNTRIES_SURR
    START WITH 1;

GRANT SELECT ON BL_3NF.SEQ_COUNTRIES_SURR TO BL_CL;
CREATE OR REPLACE PUBLIC SYNONYM SEQ_COUNTRIES_SURR FOR BL_3NF.SEQ_COUNTRIES_SURR;