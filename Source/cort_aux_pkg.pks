CREATE OR REPLACE PACKAGE cort_aux_pkg 
AS

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
  Description: Auxilary functionality executed with CORT user privileges
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added support of explain plan option, single schema mode and build functionality
  ----------------------------------------------------------------------------------------------------------------------  
*/

  gc_cort_schema constant VARCHAR2(30) := SYS_CONTEXT('USERENV','CURRENT_USER');

  /* Converts CLOB to strings arrays dbms_sql.varchar2a */
  PROCEDURE clob_to_varchar2a(
    in_clob     IN CLOB,
    out_str_arr OUT NOCOPY dbms_sql.varchar2a
  );

  /* Converts CLOB to strings arrays dbms_output.chararr */
  PROCEDURE clob_to_lines(
    in_clob     IN CLOB,
    out_str_arr OUT NOCOPY dbms_output.chararr
  );
  
  /* Converts strings arrays dbms_output.chararr to CLOB */
  PROCEDURE lines_to_clob(
    in_str_arr  IN dbms_output.chararr,
    in_delim    IN VARCHAR2 DEFAULT CHR(10),
    out_clob    OUT NOCOPY CLOB
  );

  PROCEDURE lines_to_clob(
    in_str_arr  IN arrays.gt_lstr_arr,
    in_delim    IN VARCHAR2 DEFAULT CHR(10),
    out_clob    OUT NOCOPY CLOB
  );

  -- Check if executed SQL and objects's last_ddl_time match to the last registered corresponded parameters 
  FUNCTION is_object_modified(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_sql_text     IN CLOB
  )
  RETURN BOOLEAN;

  FUNCTION reverse_array(
    in_array IN arrays.gt_clob_arr
  )
  RETURN arrays.gt_clob_arr;

  -- delete all entries for given object from cort_objects table 
  PROCEDURE cleanup_history(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  );

  -- retrun record for the last registered change for given object
  FUNCTION get_last_change(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  )
  RETURN cort_objects%ROWTYPE;

  -- return the name of the last backup (revert) table name
  FUNCTION get_last_revert_name(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  )
  RETURN VARCHAR2;

  -- register object change in cort_objects table
  PROCEDURE register_change(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sid           IN VARCHAR2,
    in_sql           IN CLOB,
    in_change_type   IN NUMBER,
    in_revert_name   IN VARCHAR2,
    in_frwd_stmt_arr IN arrays.gt_clob_arr,
    in_rlbk_stmt_arr IN arrays.gt_clob_arr
  );

  -- unregister object last change from cort_objects table
  PROCEDURE unregister_change(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_exec_time     IN TIMESTAMP
  );
  
  PROCEDURE copy_stats(
    in_source_table_name IN VARCHAR2,
    in_target_table_name IN VARCHAR2
  );
  
  -- returns retention attributes for segment
  -- Workaround for absence of ALL_SEGMENTS view
  PROCEDURE read_seg_retention(
    in_segment_type   IN  VARCHAR2,
    in_segment_name   IN  VARCHAR2,
    in_segment_owner  IN  VARCHAR2,
    out_retention     OUT VARCHAR2,
    out_minretention  OUT NUMBER
  );

  -- enable CORT
  -- Wrapper for CORT API executed with CORT user privileges
  PROCEDURE enable_cort;
  
  -- disable CORT
  -- Wrapper for CORT API executed with CORT user privileges
  PROCEDURE disable_cort;
  
  -- get CORt status (ENABLED/DISABLED)
  FUNCTION get_cort_status
  RETURN VARCHAR2;
  
  -- register index temp table 
  PROCEDURE register_index_temp_table(
    in_sid               IN VARCHAR2,
    in_table_name        IN VARCHAR2,
    in_table_owner       IN VARCHAR2,
    in_temp_table_name   IN VARCHAR2,
    in_old_table_xml     IN XMLTYPE,
    in_new_table_xml     IN XMLTYPE
  );

  -- unregister index temp table 
  PROCEDURE unregister_index_temp_table(
    in_table_name        IN VARCHAR2,
    in_table_owner       IN VARCHAR2
  );

  -- unregister index temp table 
  PROCEDURE update_index_temp_table(
    in_table_name        IN VARCHAR2,
    in_table_owner       IN VARCHAR2,
    in_new_table_rec     IN XMLTYPE
  );

  -- return index temp table record 
  FUNCTION get_index_temp_table_rec(
    in_table_name  IN VARCHAR2,
    in_table_owner IN VARCHAR2
  )
  RETURN cort_index_temp_tables%ROWTYPE;

  -- return compact unique string for current date-time  
  FUNCTION get_time_str
  RETURN VARCHAR2;
  
  -- Generates globally unique build ID
  FUNCTION gen_build_id
  RETURN VARCHAR2;

  -- Find and return active or stale build for given application
  FUNCTION find_build(
    in_application IN VARCHAR2
  )
  RETURN cort_builds%ROWTYPE;

  -- Return build record by build ID
  FUNCTION get_build_rec(
    in_build IN VARCHAR2
  )
  RETURN cort_builds%ROWTYPE;

  -- Return previous build ID for the given application
  FUNCTION get_prev_build(
    in_build       IN VARCHAR2,
    in_application IN VARCHAR2
  )
  RETURN VARCHAR2;

  -- Adds new records into CORT_BUILDS table
  PROCEDURE create_build(
    in_row IN cort_builds%ROWTYPE
  );

  -- Update record in CORT_BUILDS table
  PROCEDURE update_build(
    in_row IN cort_builds%ROWTYPE
  );

  -- Adds new record into CORT_BUILD_SESSIONS table
  PROCEDURE start_build_session(
    in_build IN VARCHAR2
  );

  -- Update record in CORT_BUILD_SESSIONS table
  PROCEDURE stop_build_session(
    in_build IN VARCHAR2
  );

  -- Check is any of session (exception current one) linked to given build active  
  FUNCTION is_build_active(
    in_build IN VARCHAR2
  )
  RETURN BOOLEAN;

  -- Run create or replace for non recreatable objects
  PROCEDURE instead_of_create(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sql           IN CLOB
  );

  -- Register metadata of recreatable objects
  PROCEDURE before_create(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sql           IN CLOB
  );

  -- Test CORT changes and add results into PLAN_TABLE$
  PROCEDURE explain_cort_sql(
    in_statement_id  IN VARCHAR2,
    in_plan_id       IN NUMBER,
    in_timestamp     IN DATE,
    out_sql          OUT CLOB,
    out_last_id      OUT NUMBER
 );
  
END cort_aux_pkg;
/