ALTER SESSION SET "_ORACLE_SCRIPT" = true;

CREATE USER SA_SOURCE_SYSTEM_BUSINESS IDENTIFIED BY SA_SOURCE_SYSTEM_BUSINESS
 DEFAULT TABLESPACE users
 TEMPORARY TABLESPACE temp
 PROFILE default;
GRANT connect, resource TO SA_SOURCE_SYSTEM_BUSINESS;
GRANT UNLIMITED TABLESPACE TO SA_SOURCE_SYSTEM_BUSINESS;

CREATE OR REPLACE DIRECTORY EXT_DIR_BUSINESS_DATA as 
'/opt/oracle/oradata/business_data_set';

GRANT READ, WRITE ON DIRECTORY EXT_DIR_BUSINESS_DATA TO SA_SOURCE_SYSTEM_BUSINESS;
GRANT READ, WRITE ON DIRECTORY EXT_DIR_BUSINESS_DATA TO BL_CL;
GRANT CREATE PUBLIC SYNONYM TO SA_SOURCE_SYSTEM_BUSINESS;