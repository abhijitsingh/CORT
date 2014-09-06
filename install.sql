/*
CORT - Oracle database deployment and continuous integration tool

Copyright (C) 2013-2014  Softcraft Ltd - Rustam Kafarov

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

/*
  Description: standard install script. To install call: sqlplus /nolog @install.sql
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.01.1 | Rustam Kafarov    | Removed dependencies on SQLPlus Extensions
  14.02   | Rustam Kafarov    | Added support of single schema mode and explain plan option
  ----------------------------------------------------------------------------------------------------------------------  
*/

SET SERVEROUTPUT ON
SET FEEDBACK OFF
SET VERIFY OFF
SET LINESIZE 400
WHENEVER SQLERROR EXIT

SPOOL install.log

PROMPT ..... CORT INSTALLATION .....
PROMPT 
PROMPT CORT could be installed in following modes:
PROMPT 1. for single schema only
PROMPT 2. for entire database
PROMPT 
PROMPT CORT schema requires following privileges: 
PROMPT   CREATE SESSION, CREATE TABLE, CREATE PROCEDURE, CREATE TRIGGER, CREATE ANY CONTEXT.
PROMPT   CREATE JOB (for single schema mode only) 
PROMPT   CREATE ANY JOB, ADMINISTER DATABASE TRIGGER, CREATE PUBLIC SYNONYM, SELECT ON DBA_OBJECTS, SELECT ON DBA_SEGMENTS (for entire database mode only) 
PROMPT It also need to have access to default tablespace or UNLIMITED TABLESPACE privilege.
PROMPT To support explain plan for CORT statements it is required SELECT ON V_$SQLTEXT_WITH_NEWLINES (optionally) 
PROMPT 
PROMPT Enter Database service/instance name
ACCEPT db_inst CHAR PROMPT "[ORCL] > " DEFAULT "ORCL"

---------------------------------
DEFINE default_schema=cort
---------------------------------

PROMPT Enter user for CORT schema
ACCEPT cort_user CHAR PROMPT "[&default_schema] > " DEFAULT "&default_schema" 

PROMPT Enter Password for schema &cort_user
ACCEPT cort_psw CHAR PROMPT " > " DEFAULT "&default_schema" HIDE 

PROMPT Connecting ...

CONNECT &cort_user/&cort_psw@&db_inst

PROMPT CORT installation moide

@accept.sql install_mode "Enter [1] to install for single schema [2] to install for entire database [0] for exit"

PROMPT Explain plan option

@accept.sql explan_mode "Enter [1] to install option [2] to skip option [0] for exit"

COLUMN c_is_single_schema NEW_VALUE is_single_schema NOPRINT
COLUMN c_trigger_scope NEW_VALUE trigger_scope NOPRINT
COLUMN c_is_explain_plan NEW_VALUE is_explain_plan NOPRINT

SET TERM OFF
SPOOL OFF
SELECT CASE 
       WHEN '&install_mode' = '1' THEN 'true' 
       WHEN '&install_mode' = '2' THEN 'false'
       END AS c_is_single_schema,
       CASE 
       WHEN '&install_mode' = '1' THEN 'schema' 
       WHEN '&install_mode' = '2' THEN 'database'
       END AS c_trigger_scope,
       CASE 
       WHEN '&explan_mode' = '1' THEN 'true' 
       WHEN '&explan_mode' = '2' THEN 'false'
       END AS c_is_explain_plan
  FROM DUAL;     
SPOOL install.log APPEND
SET TERM ON

-- check sys privs

PROMPT Checking required privileges ...

-- common privs  
--               privilege                   regexp mask
@check_priv.sql "CREATE SESSION"            "CREATE SESSION"
@check_priv.sql "CREATE TABLE"              "CREATE (ANY )?TABLE"
@check_priv.sql "CREATE PROCEDURE"          "CREATE (ANY )?PROCEDURE"
@check_priv.sql "CREATE TRIGGER"            "CREATE (ANY )?TRIGGER"
@check_priv.sql "CREATE ANY CONTEXT"        "CREATE ANY CONTEXT"

@check_tbs.sql

@run_if.sql "&install_mode=1" "check_priv.sql     'CREATE JOB'                  'CREATE (ANY )?JOB'"
@run_if.sql "&install_mode=2" "check_priv.sql     'CREATE ANY JOB'              'CREATE ANY JOB'"
@run_if.sql "&install_mode=2" "check_priv.sql     'ADMINISTER DATABASE TRIGGER' 'ADMINISTER DATABASE TRIGGER'"
@run_if.sql "&install_mode=2" "check_priv.sql     'CREATE PUBLIC SYNONYM'       'CREATE PUBLIC SYNONYM'"
@run_if.sql "&install_mode=2" "check_obj_priv.sql 'SELECT'                      'DBA_OBJECTS'"
@run_if.sql "&install_mode=2" "check_obj_priv.sql 'SELECT'                      'DBA_SEGMENTS'"
@run_if.sql "&explan_mode=1"  "check_obj_priv.sql 'SELECT'                      'V_$SQLTEXT_WITH_NEWLINES'"

PROMPT 
PROMPT Start installation ...
WHENEVER SQLERROR CONTINUE
    
-- drop cort triggers first

@DROP TRIGGER cort_create_trg
@DROP TRIGGER cort_lock_table_trg
@DROP TRIGGER cort_before_create_trg 
@DROP TRIGGER cort_before_xplan_trg


@run_script.sql "../PLSQL_Utilities/Source/arrays.pks"
@run_script.sql "../PLSQL_Utilities/Source/partition_utils.pks"
@run_script.sql "../PLSQL_Utilities/Source/partition_utils.pkb"
@run_script.sql "../PLSQL_Utilities/Source/xml_utils.pks"
@run_script.sql "../PLSQL_Utilities/Source/xml_utils.pkb"

@"Source/cort_schema.sql"

@run_script.sql "Source/cort_options_pkg.pks"
@run_script.sql "Source/cort_params_pkg.pks"
@run_script.sql "Source/cort_log_pkg.pks"
@run_script.sql "Source/cort_exec_pkg.pks"
@run_script.sql "Source/cort_comp_pkg.pks"
@run_script.sql "Source/cort_parse_pkg.pks"
@run_script.sql "Source/cort_xml_pkg.pks"
@run_script.sql "Source/cort_aux_pkg.pks"
@run_script.sql "Source/cort_pkg.pks"
@run_script.sql "Source/cort_session_pkg.pks"
@run_script.sql "Source/cort_trg_pkg.pks"
@run_script.sql "Source/cort_job_pkg.pks"

@run_script.sql "Source/cort_params_pkg.pkb"
@run_script.sql "Source/cort_log_pkg.pkb"
@run_script.sql "Source/cort_exec_pkg.pkb"
@run_script.sql "Source/cort_comp_pkg.pkb"
@run_script.sql "Source/cort_parse_pkg.pkb"
@run_script.sql "Source/cort_xml_pkg.pkb"
@run_script.sql "Source/cort_aux_pkg.pkb"
@run_script.sql "Source/cort_pkg.pkb"
@run_script.sql "Source/cort_session_pkg.pkb"
@run_script.sql "Source/cort_trg_pkg.pkb"
@run_script.sql "Source/cort_job_pkg.pkb"

@run_script.sql "Source/cort_create_trg.trg"
@run_script.sql "Source/cort_before_create_trg.trg"
@run_script.sql "Source/cort_lock_table_trg.trg"

@run_if.sql "&explan_mode=1"  "run_script.sql 'Source/cort_before_xplan_trg.trg'"
@run_if.sql "&install_mode=2" "Source/cort_grants.sql"

@exit.sql "Installation complete"