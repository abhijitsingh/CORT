CREATE OR REPLACE PACKAGE BODY cort_aux_pkg 
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

  g_counter PLS_INTEGER := 0;

  /* Converts CLOB to strings arrays dbms_sql.varchar2a */
  PROCEDURE clob_to_varchar2a(
    in_clob     IN CLOB,
    out_str_arr OUT NOCOPY dbms_sql.varchar2a
  )
  AS
    l_len      PLS_INTEGER;
    l_offset   PLS_INTEGER;
    l_amount   PLS_INTEGER;
    l_str      VARCHAR2(32767);
  BEGIN
    l_offset := 1;
    l_amount := 32767;
    l_len := dbms_lob.getlength(in_clob);
    WHILE l_offset <= l_len LOOP
      l_str := dbms_lob.substr(in_clob,l_amount,l_offset);
      out_str_arr(out_str_arr.COUNT+1) := l_str;
      l_offset := l_offset + l_amount;
    END LOOP;
  END clob_to_varchar2a;

  /* Converts CLOB to strings arrays dbms_output.chararr */
  PROCEDURE clob_to_lines(
    in_clob     IN CLOB,
    out_str_arr OUT NOCOPY dbms_output.chararr
  )
  AS
    l_len      PLS_INTEGER;
    l_offset   PLS_INTEGER;
    l_amount   PLS_INTEGER;
    l_cr_pos   PLS_INTEGER;
    l_str      VARCHAR2(32767);
  BEGIN
    l_offset := 1;
    l_amount := 32767;
    l_len := dbms_lob.getlength(in_clob);
    WHILE l_offset <= l_len LOOP
      l_cr_pos := dbms_lob.instr(in_clob,chr(10),l_offset);
      IF l_cr_pos = 0 THEN
        l_cr_pos := LEAST(l_offset + l_amount, l_len + 1);
      ELSE  
        l_cr_pos := l_cr_pos; 
      END IF;
      l_str := dbms_lob.substr(in_clob,l_cr_pos-l_offset,l_offset);
      out_str_arr(out_str_arr.COUNT+1) := l_str;
      l_offset := l_cr_pos + 1;
    END LOOP;
  END clob_to_lines;

  /* Converts strings arrays dbms_output.chararr to CLOB */
  PROCEDURE lines_to_clob(
    in_str_arr  IN dbms_output.chararr,
    in_delim    IN VARCHAR2 DEFAULT CHR(10),
    out_clob    OUT NOCOPY CLOB
  )
  AS
  BEGIN
    FOR i IN 1..in_str_arr.COUNT LOOP
      out_clob := out_clob||in_str_arr(i);
      IF i < in_str_arr.COUNT THEN
        out_clob := out_clob||in_delim;
      END IF;  
    END LOOP;
  END lines_to_clob;

  PROCEDURE lines_to_clob(
    in_str_arr  IN arrays.gt_lstr_arr,
    in_delim    IN VARCHAR2 DEFAULT CHR(10),
    out_clob    OUT NOCOPY CLOB
  )
  AS
  BEGIN
    FOR i IN 1..in_str_arr.COUNT LOOP
      out_clob := out_clob||in_str_arr(i);
      IF i < in_str_arr.COUNT THEN
        out_clob := out_clob||in_delim;
      END IF;  
    END LOOP;
  END lines_to_clob;
  
  -- return compact unique string for current date-time  
  FUNCTION get_systimestamp
  RETURN TIMESTAMP
  AS
    l_systime TIMESTAMP(9);
  BEGIN
    g_counter := g_counter + 1;
    IF g_counter > 999999 THEN
      g_counter := 0;
    END IF; 

    l_systime := SYSTIMESTAMP + TO_DSINTERVAL('PT0.'||TO_CHAR(g_counter, 'fm000000009')||'S');

    RETURN l_systime;
  END get_systimestamp;

  FUNCTION get_object_last_ddl_time(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2
  )
  RETURN DATE
  AS
    l_last_ddl_time DATE;
  BEGIN
    BEGIN
      SELECT last_ddl_time
        INTO l_last_ddl_time 
        $IF cort_options_pkg.gc_single_schema $THEN
        FROM all_objects
        $ELSE
        FROM dba_objects
        $END  
       WHERE owner = in_object_owner
         AND object_name = in_object_name
         AND object_type = in_object_type;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_last_ddl_time := NULL;
    END;
    RETURN l_last_ddl_time;
  END get_object_last_ddl_time;

  FUNCTION reverse_array(
    in_array IN arrays.gt_clob_arr
  )
  RETURN arrays.gt_clob_arr
  AS
    l_indx_frwd PLS_INTEGER;
    l_indx_bkwd PLS_INTEGER;
    l_cnt       PLS_INTEGER;
    l_swap      CLOB;
    l_result    arrays.gt_clob_arr;
  BEGIN
    l_result := in_array;
    l_cnt := 1;
    l_indx_frwd := l_result.FIRST;
    l_indx_bkwd := l_result.LAST;
    WHILE l_cnt <= TRUNC(l_result.COUNT/2) AND 
          l_indx_frwd IS NOT NULL AND 
          l_indx_bkwd IS NOT NULL 
    LOOP
      l_swap := l_result(l_indx_frwd);
      l_result(l_indx_frwd) := l_result(l_indx_bkwd);
      l_result(l_indx_bkwd) := l_swap; 
      l_indx_frwd := l_result.NEXT(l_indx_frwd);
      l_indx_bkwd := l_result.PRIOR(l_indx_bkwd);
      l_cnt := l_cnt + 1;
    END LOOP;
    RETURN l_result;
  END reverse_array;

  PROCEDURE cleanup_history(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    DELETE FROM cort_objects 
     WHERE object_owner = in_object_owner 
       AND object_name = in_object_name 
       AND object_type = in_object_type
       AND NVL(build,'NULL') <> NVL(SUBSTR(SYS_CONTEXT('USERENV','CLIENT_INFO'), 12),'{null}');

    COMMIT;
  END cleanup_history;

  FUNCTION get_last_change(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  )
  RETURN cort_objects%ROWTYPE
  AS
    l_rec              cort_objects%ROWTYPE;
  BEGIN
    BEGIN
      SELECT *
        INTO l_rec 
        FROM (SELECT * 
                FROM cort_objects 
               WHERE object_owner = in_object_owner 
                 AND object_name = in_object_name 
                 AND object_type = in_object_type
               ORDER BY exec_time DESC
             )
       WHERE ROWNUM = 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;    
  
    RETURN l_rec;
  END get_last_change; 

  FUNCTION get_last_revert_name(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_revert_name    cort_objects.revert_name%TYPE;
  BEGIN
    BEGIN
      SELECT revert_name
        INTO l_revert_name 
        FROM (SELECT * 
                FROM cort_objects 
               WHERE object_owner = in_object_owner 
                 AND object_name = in_object_name 
                 AND object_type = in_object_type
                 AND revert_name IS NOT NULL
               ORDER BY exec_time DESC
             )
       WHERE ROWNUM = 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;    
  
    RETURN l_revert_name;
  END get_last_revert_name; 

  PROCEDURE update_change(
    in_rec IN cort_objects%ROWTYPE
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    l_rec cort_objects%ROWTYPE;
  BEGIN
    l_rec := in_rec;
    IF 'CORT_BUILD='||l_rec.build = NVL(SYS_CONTEXT('USERENV','CLIENT_INFO'),'CORT_BUILD=')
    THEN
      -- update sql text
      UPDATE cort_objects 
         SET sql_text = in_rec.sql_text,
             application = cort_exec_pkg.g_params.application, 
             release = cort_exec_pkg.g_params.release  
       WHERE object_owner = in_rec.object_owner 
         AND object_name = in_rec.object_name 
         AND object_type = in_rec.object_type
         AND exec_time = in_rec.exec_time;

    ELSE
      l_rec.sid := dbms_session.unique_session_id;
      l_rec.exec_time := get_systimestamp;
      l_rec.change_type := cort_comp_pkg.gc_result_nochange;
      l_rec.application := cort_exec_pkg.g_params.application; 
      l_rec.release := cort_exec_pkg.g_params.release;  
      l_rec.build := SUBSTR(SYS_CONTEXT('USERENV','CLIENT_INFO'), 12); 
      l_rec.revert_name := NULL;
      l_rec.forward_ddl := NULL;
      l_rec.revert_ddl := NULL;
        
      INSERT INTO cort_objects VALUES l_rec;

    END IF;  

    COMMIT;
  END update_change;

  -- register object change in cort-metadata table
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
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    l_old_rec cort_objects%ROWTYPE;
    l_new_rec cort_objects%ROWTYPE;
  BEGIN
    l_old_rec := get_last_change(
                   in_object_owner => in_object_owner,
                   in_object_type  => in_object_type,
                   in_object_name  => in_object_name
                 );
    IF in_change_type <> cort_comp_pkg.gc_result_nochange OR -- actual change
       l_old_rec.object_name IS NULL OR -- no history for the object
       SYS_CONTEXT('USERENV','CLIENT_INFO') IS NOT NULL OR -- change is part of the build
       l_old_rec.build IS NOT NULL -- previous version was part of the build
    THEN
      IF l_old_rec.build = SUBSTR(SYS_CONTEXT('USERENV','CLIENT_INFO'), 12) AND 
         l_old_rec.application = cort_exec_pkg.g_params.application
      THEN
        IF in_frwd_stmt_arr.COUNT > 0 THEN
          cort_xml_pkg.write_to_xml(
            in_value => in_frwd_stmt_arr,
            out_xml  => l_new_rec.forward_ddl 
          );
        END IF;
        l_new_rec.last_ddl_time := NVL(get_object_last_ddl_time(
                                         in_object_owner => in_object_owner, 
                                         in_object_name  => in_object_name,
                                         in_object_type  => in_object_type      
                                       ),SYSDATE);
        l_new_rec.exec_time := get_systimestamp;                               
        UPDATE cort_objects
           SET sql_text = in_sql
             , last_ddl_time = l_new_rec.last_ddl_time            
             , forward_ddl = l_new_rec.forward_ddl
             , exec_time = l_new_rec.exec_time                      
             , change_type = in_change_type
         WHERE object_owner = l_old_rec.object_owner 
           AND object_name = l_old_rec.object_name 
           AND object_type = l_old_rec.object_type
           AND exec_time = l_old_rec.exec_time;       
      ELSE
        l_new_rec.object_owner := in_object_owner; 
        l_new_rec.object_name := in_object_name; 
        l_new_rec.object_type := in_object_type;
        l_new_rec.sid := in_sid;
        l_new_rec.exec_time := get_systimestamp;
        l_new_rec.sql_text := in_sql; 
        l_new_rec.last_ddl_time := NVL(get_object_last_ddl_time(
                                         in_object_owner => in_object_owner, 
                                         in_object_name  => in_object_name,
                                         in_object_type  => in_object_type      
                                       ),SYSDATE); 
        l_new_rec.change_type := in_change_type;
        l_new_rec.application := cort_exec_pkg.g_params.application;
        l_new_rec.release := cort_exec_pkg.g_params.release;  
        l_new_rec.build := SUBSTR(SYS_CONTEXT('USERENV','CLIENT_INFO'), 12); 
        l_new_rec.revert_name := in_revert_name;

        IF in_change_type <> cort_comp_pkg.gc_result_nochange AND
           in_frwd_stmt_arr.COUNT > 0 
        THEN
          cort_xml_pkg.write_to_xml(
            in_value => in_frwd_stmt_arr,
            out_xml  => l_new_rec.forward_ddl 
          );
        END IF;
          
        IF in_change_type <> cort_comp_pkg.gc_result_nochange AND
           in_rlbk_stmt_arr.COUNT > 0 
        THEN
          cort_xml_pkg.write_to_xml(  
            in_value => reverse_array(in_rlbk_stmt_arr),
            out_xml  => l_new_rec.revert_ddl
          );
        END IF;
        
        INSERT INTO cort_objects VALUES l_new_rec;
      END IF;  
    ELSE
      -- just update sql text
      l_old_rec.sql_text := in_sql; 
      update_change(l_old_rec);
    END IF;
    
    COMMIT;
  END register_change;
  
  PROCEDURE unregister_change(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_exec_time     IN TIMESTAMP
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    DELETE FROM cort_objects 
     WHERE object_owner = in_object_owner 
       AND object_name = in_object_name 
       AND object_type = in_object_type
       AND exec_time = in_exec_time;

    COMMIT;
  END unregister_change;

  -- Check if executed SQL and objects's last_ddl_time match to the last registered corresponded parameters 
  FUNCTION is_object_modified(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_sql_text     IN CLOB
  )
  RETURN BOOLEAN
  AS
    l_rec           cort_objects%ROWTYPE;
    l_result        BOOLEAN;
  BEGIN
    l_result := TRUE;
    l_rec := get_last_change(
               in_object_owner => in_object_owner,
               in_object_type  => in_object_type,
               in_object_name  => in_object_name
             );
    IF l_rec.sql_text = in_sql_text THEN
      IF l_rec.last_ddl_time = get_object_last_ddl_time(
                                  in_object_owner => in_object_owner, 
                                  in_object_name  => in_object_name,
                                  in_object_type  => in_object_type      
                                ) 
      THEN
        update_change(l_rec);
        l_result := FALSE;
      END IF;
    END IF;
    RETURN l_result;
  END is_object_modified;
  
  PROCEDURE copy_stats(
    in_source_table_name IN VARCHAR2,
    in_target_table_name IN VARCHAR2
  )
  AS
  BEGIN
    UPDATE cort_stat
       SET C1 = in_target_table_name
     WHERE C1 = in_source_table_name;
    COMMIT;  
  END copy_stats;

  -- returns retention attributes for segment
  -- Workaround for absence of ALL_SEGMENTS view 
  PROCEDURE read_seg_retention(
    in_segment_type   IN VARCHAR2,
    in_segment_name   IN VARCHAR2,
    in_segment_owner  IN VARCHAR2,
    out_retention     OUT VARCHAR2,
    out_minretention  OUT NUMBER
  )
  AS
  BEGIN
    BEGIN
      SELECT NVL(retention,'NONE'), NULLIF(minretention,0) 
        INTO out_retention, out_minretention   
      $IF cort_options_pkg.gc_single_schema $THEN
        FROM user_segments
       WHERE segment_name = in_segment_name
         AND segment_type = in_segment_type;
      $ELSE 
        FROM dba_segments
       WHERE segment_name = in_segment_name
         AND owner = in_segment_owner
         AND segment_type = in_segment_type;
      $END
    EXCEPTION
      WHEN NO_DATA_FOUND THEN 
        out_retention := 'NONE'; 
        out_minretention := 0;
    END;
  END read_seg_retention;

  -- Wrapper for CORT API executed with CORT user privileges
  PROCEDURE enable_cort
  AS
  BEGIN
    cort_exec_pkg.execute_immediate('ALTER TRIGGER CORT_CREATE_TRG ENABLE', NULL);
  END enable_cort;
  
  -- Wrapper for CORT API executed with CORT user privileges
  PROCEDURE disable_cort
  AS
  BEGIN
    cort_exec_pkg.execute_immediate('ALTER TRIGGER CORT_CREATE_TRG DISABLE', NULL);
  END disable_cort;
  
  FUNCTION get_cort_status
  RETURN VARCHAR2
  AS
    l_status VARCHAR2(30);
  BEGIN
    BEGIN
      SELECT status
        INTO l_status
        FROM user_triggers
       WHERE trigger_name = 'CORT_CREATE_TRG';    
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_status := NULL;
    END;
    RETURN l_status;
  END get_cort_status;
  
  -- register index temp table 
  PROCEDURE register_index_temp_table(
    in_sid               IN VARCHAR2,
    in_table_name        IN VARCHAR2,
    in_table_owner       IN VARCHAR2,
    in_temp_table_name   IN VARCHAR2,
    in_old_table_xml     IN XMLTYPE,
    in_new_table_xml     IN XMLTYPE
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    l_rec             cort_index_temp_tables%ROWTYPE;
  BEGIN
    l_rec.sid := in_sid;
    l_rec.table_owner := in_table_owner;
    l_rec.table_name := in_table_name;
    l_rec.temp_table_name := in_temp_table_name;
    l_rec.old_table_rec := in_old_table_xml;
    l_rec.new_table_rec := in_new_table_xml;
    
    DELETE FROM cort_index_temp_tables
     WHERE table_owner = in_table_owner
       AND table_name = in_table_name; 

    INSERT INTO cort_index_temp_tables VALUES l_rec;
    
    COMMIT;
  END register_index_temp_table;
  
  -- unregister index temp table 
  PROCEDURE unregister_index_temp_table(
    in_table_name        IN VARCHAR2,
    in_table_owner       IN VARCHAR2
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    DELETE FROM cort_index_temp_tables
     WHERE table_owner = in_table_owner
       AND table_name = in_table_name; 

    COMMIT;
  END unregister_index_temp_table;

  -- unregister index temp table 
  PROCEDURE update_index_temp_table(
    in_table_name        IN VARCHAR2,
    in_table_owner       IN VARCHAR2,
    in_new_table_rec     IN XMLTYPE
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    UPDATE cort_index_temp_tables
       SET new_table_rec = in_new_table_rec 
     WHERE table_owner = in_table_owner
       AND table_name = in_table_name; 

    COMMIT;
  END update_index_temp_table;


  -- return index temp table record 
  FUNCTION get_index_temp_table_rec(
    in_table_name  IN VARCHAR2,
    in_table_owner IN VARCHAR2
  )
  RETURN cort_index_temp_tables%ROWTYPE
  AS
    l_rec  cort_index_temp_tables%ROWTYPE;
  BEGIN
    BEGIN
      SELECT * 
        INTO l_rec 
        FROM cort_index_temp_tables
       WHERE table_owner = in_table_owner
         AND table_name = in_table_name; 
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;
    RETURN l_rec;
  END get_index_temp_table_rec;
  
  -- return compact unique string for current date-time  
  FUNCTION get_time_str
  RETURN VARCHAR2
  AS
    l_sysdate TIMESTAMP(2) := SYSTIMESTAMP;
  BEGIN
    RETURN TO_CHAR((TO_NUMBER(TO_CHAR(l_sysdate, 'J')) - TO_NUMBER(TO_CHAR(date'2001-01-01', 'J')))*8640000 + TO_NUMBER(TO_CHAR(l_sysdate, 'SSSSS'))*100 + TO_NUMBER(TO_CHAR(l_sysdate,'FF2')),'fm0XXXXXXXXX');     
  END get_time_str;

  -- Generates globally unique build ID
  FUNCTION gen_build_id
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN get_time_str||TO_CHAR(SYS_CONTEXT('USERENV','SID'),'fm0XXXXX');
  END gen_build_id;
  
  -- Find and return active or stale build for given application
  FUNCTION find_build(
    in_application IN VARCHAR2
  )
  RETURN cort_builds%ROWTYPE
  AS
    l_rec cort_builds%ROWTYPE;
  BEGIN
    BEGIN
      SELECT *
        INTO l_rec 
        FROM cort_builds
       WHERE application = in_application
         AND DECODE(STATUS, 'ACTIVE', '0', 'STALE', '0', build) = '0'; 
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;  
    RETURN l_rec;
  END find_build;
  
  -- Return build record by build ID
  FUNCTION get_build_rec(
    in_build IN VARCHAR2
  )
  RETURN cort_builds%ROWTYPE
  AS
    l_rec cort_builds%ROWTYPE;
  BEGIN
    BEGIN
      SELECT *
        INTO l_rec 
        FROM cort_builds
       WHERE build = in_build; 
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;  
    RETURN l_rec;
  END get_build_rec;

  -- Return previous build ID for the given application
  FUNCTION get_prev_build(
    in_build       IN VARCHAR2,
    in_application IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_build cort_builds.build%TYPE;
  BEGIN
    BEGIN
      SELECT MAX(build)
        INTO l_build 
        FROM cort_builds
       WHERE build < in_build
         AND application = in_application
         AND status = 'COMPLETED';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;  
    RETURN l_build;
  END get_prev_build;

  -- Adds new record into CORT_BUILDS table
  PROCEDURE create_build(
    in_row IN cort_builds%ROWTYPE
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    INSERT INTO cort_builds VALUES in_row;
    COMMIT;
  END create_build;

  -- Update record in CORT_BUILDS table
  PROCEDURE update_build(
    in_row IN cort_builds%ROWTYPE
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    UPDATE cort_builds 
       SET ROW = in_row
     WHERE build = in_row.build;
    COMMIT;
  END update_build;

  -- Adds new record into CORT_BUILD_SESSIONS table
  PROCEDURE start_build_session(
    in_build IN VARCHAR2
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    l_row cort_build_sessions%ROWTYPE;
  BEGIN
    l_row.build := in_build;
    l_row.session_id := dbms_session.unique_session_id;
    l_row.start_time := SYSDATE;
    INSERT INTO cort_build_sessions VALUES l_row;
    dbms_application_info.set_client_info('CORT_BUILD='||in_build);
    COMMIT;
  END start_build_session;

  -- Update record in CORT_BUILD_SESSIONS table
  PROCEDURE stop_build_session(
    in_build IN VARCHAR2
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    UPDATE cort_build_sessions 
       SET end_time = SYSDATE
     WHERE build = in_build
       AND session_id = dbms_session.unique_session_id;
    dbms_application_info.set_client_info(NULL);
    COMMIT;
  END stop_build_session;
  

  -- Check is any of session (exception current one) linked to given build active  
  FUNCTION is_build_active(
    in_build IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    l_cnt             PLS_INTEGER;
    l_current_session VARCHAR(30) := dbms_session.unique_session_id;
    
    CURSOR cur_sessions 
    IS
    SELECT session_id 
      FROM cort_build_sessions
     WHERE build = in_build
       AND session_id <> l_current_session
       AND end_time IS NULL
       FOR UPDATE;
    
  BEGIN
    l_cnt := 0;
    FOR x IN cur_sessions
    LOOP
      IF dbms_session.is_session_alive(x.session_id) THEN
        l_cnt := l_cnt + 1;
      ELSE
        UPDATE cort_build_sessions 
           SET end_time = SYSDATE
         WHERE CURRENT OF cur_sessions;
      END IF; 
    END LOOP;
    COMMIT;
    RETURN l_cnt > 0; 
  END is_build_active;

  -- Run create or replace for non recreatable objects
  PROCEDURE instead_of_create(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sql           IN CLOB
  )
  AS
  BEGIN
    cort_exec_pkg.g_params := cort_session_pkg.get_params;
    dbms_output.enable(buffer_size => 1000000);
    
    IF cort_exec_pkg.gc_begin_message IS NOT NULL THEN 
      dbms_output.put_line(
        cort_exec_pkg.format_message(
          in_template      => cort_exec_pkg.gc_begin_message,
          in_object_name   => in_object_name,
          in_object_owner  => in_object_owner,
          in_object_type   => in_object_type
        )
      );
    END IF;

    -- if test mode is enabled on session level
    IF cort_exec_pkg.g_params.test OR 
       -- or object has been changed 
       is_object_modified(
         in_object_type  => in_object_type,
         in_object_name  => in_object_name,
         in_object_owner => in_object_owner,
         in_sql_text     => in_sql
       )
    THEN
      cort_job_pkg.start_job(
        in_action       => 'CREATE_OR_REPLACE',
        in_object_type  => in_object_type,
        in_object_name  => in_object_name,
        in_object_owner => in_object_owner,
        in_sql          => in_sql
      );
    ELSE
      -- do not execute the job 
      cort_exec_pkg.debug('No changes since last operation applied');
    END IF; 
     
    IF cort_exec_pkg.gc_end_message IS NOT NULL THEN 
      dbms_output.put_line(
        cort_exec_pkg.format_message(
          in_template      => cort_exec_pkg.gc_end_message,
          in_object_name   => in_object_name,
          in_object_owner  => in_object_owner,
          in_object_type   => in_object_type
        )
      );
    END IF;  
  END instead_of_create;

  -- Register metadata of recreatable objects
  PROCEDURE before_create(
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sql           IN CLOB
  )
  AS
  BEGIN
    cort_exec_pkg.g_params := cort_session_pkg.get_params;
    dbms_output.enable(buffer_size => 1000000);
    
    IF cort_exec_pkg.gc_begin_message IS NOT NULL THEN 
      dbms_output.put_line(
        cort_exec_pkg.format_message(
          in_template      => cort_exec_pkg.gc_begin_message,
          in_object_name   => in_object_name,
          in_object_owner  => in_object_owner,
          in_object_type   => in_object_type
        )
      );
    END IF;  
    
    BEGIN
      cort_job_pkg.run_as_user(
        in_action       => 'REGISTER',
        in_object_type  => in_object_type,
        in_object_name  => in_object_name,
        in_object_owner => in_object_owner,
        in_sql          => in_sql
      );
    EXCEPTION
      WHEN OTHERS THEN
        dbms_output.put_line(sqlerrm);
    END;  

    IF cort_exec_pkg.gc_end_message IS NOT NULL THEN 
      dbms_output.put_line(
        cort_exec_pkg.format_message(
          in_template      => cort_exec_pkg.gc_end_message,
          in_object_name   => in_object_name,
          in_object_owner  => in_object_owner,
          in_object_type   => in_object_type
        )
      );
    END IF;  
  END before_create;

  FUNCTION get_xplan_statement(in_xplan_id IN VARCHAR2)
  RETURN CLOB
  AS
    l_explan       VARCHAR2(32767);
    l_sql_id       VARCHAR2(30);
    l_sql_arr      arrays.gt_lstr_arr;
    l_sql_text     CLOB;
    l_owner        VARCHAR2(30);
  BEGIN
    $IF cort_options_pkg.gc_explain_plan $THEN
    l_owner := cort_parse_pkg.get_regexp_const(gc_cort_schema);
    l_explan := 'explain\s+plan\s+set\s+statement_id\s*=\s*'''||cort_parse_pkg.get_regexp_const(in_xplan_id)||'''\s*(into\s+("?'||l_owner||'"?\s*.\s*)?"?PLAN_TABLE"?\s*)?for\s+';
    BEGIN
      SELECT sql_id 
        INTO l_sql_id
        FROM (SELECT sql_id, 
                     MAX(DECODE(piece, 0, sql_text))||
                     MAX(DECODE(piece, 1, sql_text))||
                     MAX(DECODE(piece, 2, sql_text))||
                     MAX(DECODE(piece, 3, sql_text))||
                     MAX(DECODE(piece, 4, sql_text))||
                     MAX(DECODE(piece, 5, sql_text))||
                     MAX(DECODE(piece, 6, sql_text))||
                     MAX(DECODE(piece, 7, sql_text))||
                     MAX(DECODE(piece, 8, sql_text))||
                     MAX(DECODE(piece, 9, sql_text)) as sql_text
                FROM v$sqltext_with_newlines
               WHERE command_type = 50 
               GROUP BY sql_id
             )
       WHERE REGEXP_LIKE(sql_text, l_explan, 'i');
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
      dbms_output.put_line('NO_DATA_FOUND');
        l_sql_id := NULL; 
      WHEN TOO_MANY_ROWS THEN
      dbms_output.put_line('TOO_MANY_ROWS');
        l_sql_id := NULL; 
    END;     
    
    IF l_sql_id IS NULL THEN
      RETURN NULL;
    END IF;
    
    SELECT sql_text 
      BULK COLLECT 
      INTO l_sql_arr
      FROM v$sqltext_with_newlines
     WHERE command_type = 50
       AND sql_id = l_sql_id
     ORDER BY piece;
     
    lines_to_clob(
      in_str_arr  => l_sql_arr,
      in_delim    => NULL,
      out_clob    => l_sql_text
    );
    
    IF REGEXP_INSTR(l_sql_text, l_explan, 1, 1, 0, 'i') = 1 THEN
      l_sql_text := REGEXP_REPLACE(l_sql_text, l_explan, NULL, 1, 1, 'i');
    ELSE
      l_sql_text := NULL;
    END IF;
    $END
    RETURN l_sql_text; 
  END get_xplan_statement;

  -- Test CORT changes and add results into PLAN_TABLE$
  PROCEDURE explain_cort_sql(
    in_statement_id  IN VARCHAR2,
    in_plan_id       IN NUMBER,
    in_timestamp     IN DATE,
    out_sql          OUT CLOB,
    out_last_id      OUT NUMBER
  )
  AS
    l_sid           VARCHAR2(30);
    l_sql           CLOB;
    l_object_type   VARCHAR2(100);
    l_object_name   VARCHAR2(100);
    l_object_owner  VARCHAR2(100);
    l_job_time      TIMESTAMP(9);
    l_text_arr      arrays.gt_clob_arr;
    l_id_arr        arrays.gt_num_arr;
    l_last_id       number;
  BEGIN
    IF cort_pkg.get_status = 'ENABLED' AND 
       $IF cort_options_pkg.gc_single_schema $THEN
       SYS_CONTEXT('USERENV', 'SESSION_USER') = gc_cort_schema AND
       $END
       cort_session_pkg.get_status = 'ENABLED'
    THEN
      l_sql := get_xplan_statement(in_statement_id);

      IF cort_trg_pkg.get_execution_mode(l_sql) = 'REPLACE' THEN
        cort_parse_pkg.parse_create_statement(
          in_sql           => l_sql,
          out_object_type  => l_object_type,
          out_object_owner => l_object_owner,
          out_object_name  => l_object_name
        );

        l_object_owner := NVL(l_object_owner,user);
        IF l_object_type IN ('TABLE','SEQUENCE') AND
           $IF cort_options_pkg.gc_single_schema $THEN
           l_object_owner = gc_cort_schema AND
           $END
           l_object_name IS NOT NULL 
        THEN
          out_sql := l_sql;

          -- Enable TEST mode for the current session
          cort_exec_pkg.g_params := cort_session_pkg.get_params;
          cort_exec_pkg.g_params.test := TRUE;

          cort_job_pkg.run_as_user(
            in_action       => 'EXPLAIN_PLAN_FOR',
            in_object_type  => l_object_type,
            in_object_name  => l_object_name,
            in_object_owner => l_object_owner,
            in_sql          => l_sql
          );
          
          l_sid := dbms_session.unique_session_id;
          l_job_time := cort_job_pkg.get_last_job_timestamp;

          SELECT text
            BULK COLLECT
            INTO l_text_arr
            FROM cort_log
           WHERE sid = l_sid
             AND action = 'EXPLAIN_PLAN_FOR'
             AND job_time = l_job_time
             AND log_type = 'TEST EXECUTE'
           ORDER BY log_time;

          -- disable cort triggers
          cort_session_pkg.disable;        
          BEGIN
            l_id_arr(1) := 1;

            FOR i IN 2..l_text_arr.COUNT LOOP
              l_id_arr(i) := 1000000 + i;
            END LOOP;  

            FORALL i IN 1..l_text_arr.COUNT
              INSERT INTO plan_table(statement_id, plan_id, timestamp, parent_id, id, operation, depth)
              VALUES(in_statement_id, in_plan_id, in_timestamp, 0, l_id_arr(i), REPLACE(SUBSTR(l_text_arr(i),1,4000),CHR(10),CHR(32)), 1);
            
            l_last_id := 1;  

          EXCEPTION
            WHEN OTHERS THEN
              -- enable cort triggers before raise exception
              cort_session_pkg.enable;
              RAISE;
          END;
          cort_session_pkg.enable;
          out_last_id := l_last_id;  
        END IF;
      
      END IF;
    END IF;
    
         
  
  END explain_cort_sql;
  

END cort_aux_pkg;
/
