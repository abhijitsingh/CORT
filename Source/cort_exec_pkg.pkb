CREATE OR REPLACE PACKAGE BODY cort_exec_pkg
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
  Description: Main package executing table recreation and rollback with current user privileges
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.01   | Rustam Kafarov    | Added support for sequences, indexes, builds, create table as select. Orevall improvements
  ----------------------------------------------------------------------------------------------------------------------  
*/

  TYPE    gt_index_compare_rec        IS RECORD(
    index_name       VARCHAR2(30),
    index_owner      VARCHAR2(30),
    temp_name        VARCHAR2(30),
    source_tab_indx  PLS_INTEGER,
    target_tab_indx  PLS_INTEGER,
    compare_result   PLS_INTEGER,
    frwd_ddl_arr     arrays.gt_clob_arr,
    rlbk_ddl_arr     arrays.gt_clob_arr
  );
  TYPE    gt_index_compare_arr        IS TABLE OF gt_index_compare_rec        INDEX BY PLS_INTEGER;
  
  SUBTYPE gt_all_indexes_rec          IS all_indexes%ROWTYPE;
  SUBTYPE gt_all_part_indexes_rec     IS all_part_indexes%ROWTYPE;
  SUBTYPE gt_all_join_ind_columns_rec IS all_join_ind_columns%ROWTYPE;
  SUBTYPE gt_all_constraints_rec      IS all_constraints%ROWTYPE;

  TYPE    gt_all_indexes_arr          IS TABLE OF gt_all_indexes_rec          INDEX BY PLS_INTEGER;
  TYPE    gt_all_join_ind_columns_arr IS TABLE OF gt_all_join_ind_columns_rec INDEX BY PLS_INTEGER;
  TYPE    gt_all_constraints_arr      IS TABLE OF gt_all_constraints_rec      INDEX BY PLS_INTEGER;

  g_event_id             NUMBER;

  gc_cort_debug_prefix   CONSTANT VARCHAR2(30) := '==cort debug==:';
  

  g_error_stack          VARCHAR2(32767);
  g_ddl_arr              arrays.gt_clob_arr;
  g_overall_timer        PLS_INTEGER;
  
  -- returns error stack
  FUNCTION get_error_stack
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN g_error_stack;
  END get_error_stack;

  -- output
  PROCEDURE output(
    in_text IN CLOB
  ) 
  AS
    l_arr dbms_output.chararr;
  BEGIN
    IF LENGTH(in_text) > 32767 THEN
      cort_aux_pkg.clob_to_lines(
        in_clob     => in_text,
        out_str_arr => l_arr 
      );
      FOR i IN 1..l_arr.COUNT LOOP
        dbms_output.put_line(l_arr(i));
      END LOOP;
    ELSE
      dbms_output.put_line(in_text);
    END IF;
  END output;

  -- output debug text
  PROCEDURE debug(
    in_text  IN CLOB
  )
  AS
  BEGIN
    IF g_params.debug THEN
      output(gc_cort_debug_prefix||in_text);
    END IF;
  END debug;
  
  -- format begin/end message
  FUNCTION format_message(
    in_template     IN VARCHAR2,
    in_object_type  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_object_name  IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_result VARCHAR2(1000);
  BEGIN
    l_result := in_template;
    l_result := REPLACE(l_result, '[object_type]', in_object_type);
    l_result := REPLACE(l_result, '[object_owner]', in_object_owner);
    l_result := REPLACE(l_result, '[object_name]', in_object_name);
    RETURN l_result;
  END format_message;

  -- output sql statement
  PROCEDURE echo_sql(
    in_sql IN CLOB
  )
  AS
  BEGIN
    IF g_params.echo OR g_params.debug OR g_params.test THEN
      IF SUBSTR(in_sql,-1) = ';' THEN
        -- PL/SQL BLOCK
        output(in_sql||CHR(10)||'/');
      ELSE
        -- DDL or SQL
        output(in_sql||';');
      END IF;
    END IF;  
  END echo_sql;


  PROCEDURE start_timer(out_timer OUT PLS_INTEGER)
  AS
  BEGIN
    out_timer := dbms_utility.get_time;
  END start_timer;

  PROCEDURE stop_timer(
    in_timer IN PLS_INTEGER,
    in_text  IN VARCHAR2
  )
  AS
    l_time    NUMBER;
  BEGIN
    l_time := (dbms_utility.get_time - in_timer)/100;
    debug(in_text||' execution time - '||TO_CHAR(l_time)||' sec.');
  END stop_timer;

  PROCEDURE raise_error(
    in_msg  IN VARCHAR,
    in_code IN NUMBER DEFAULT -20000
  )
  AS
    l_caller_owner    VARCHAR2(100);
    l_caller_type     VARCHAR2(100);
    l_package_name    VARCHAR2(100);
    l_line_number     NUMBER;
    l_error_backtrace VARCHAR2(4000);
  BEGIN
    owa_util.who_called_me(
      owner      => l_caller_owner,
      name       => l_package_name,
      lineno     => l_line_number,
      caller_t   => l_caller_type
    );
    l_error_backtrace := 'ORA'||in_code||': at "'||l_caller_owner||'.'||l_package_name||'", line '||l_line_number;
    g_error_stack := g_error_stack||l_error_backtrace||CHR(10);
    RAISE_APPLICATION_ERROR(in_code, in_msg, TRUE);
  END raise_error;

  -- wrapper for oracle 10g to support CLOB
  PROCEDURE execute_immediate_10g(
    in_sql IN CLOB
  )
  AS
    l_ddl      VARCHAR2(32767);
    l_str_arr  dbms_sql.varchar2a;
    h          INTEGER;
    l_cnt      NUMBER;
  BEGIN
    IF LENGTH(in_sql) <= 32767 THEN
      l_ddl := in_sql;
      EXECUTE IMMEDIATE l_ddl;
    ELSE
      cort_aux_pkg.clob_to_varchar2a(
        in_clob     => in_sql,
        out_str_arr => l_str_arr
      );
      h := dbms_sql.open_cursor;
      BEGIN
        dbms_sql.parse(h, l_str_arr, 1, l_str_arr.COUNT, FALSE, dbms_sql.native);
        l_cnt := dbms_sql.execute(h);
      EXCEPTION
        WHEN OTHERS THEN
          g_error_stack := g_error_stack||dbms_utility.format_error_backtrace||CHR(10);
          IF dbms_sql.is_open(h) THEN
            dbms_sql.close_cursor(h);
          END IF;  
          RAISE;
      END;
      IF dbms_sql.is_open(h) THEN
        dbms_sql.close_cursor(h);
      END IF;  
    END IF;
  END execute_immediate_10g;

  -- executes DDL command
  PROCEDURE execute_immediate(
    in_sql   IN CLOB,
    in_echo  IN BOOLEAN DEFAULT TRUE,
    in_test  IN BOOLEAN DEFAULT FALSE 
  )
  AS
  PRAGMA autonomous_transaction;
    l_log_time     TIMESTAMP(9); 
  BEGIN
    IF in_sql IS NOT NULL AND LENGTH(in_sql) > 0 THEN
    
      debug('EXECUTE IMMEDIATE: '||in_sql);

      IF in_echo THEN
        echo_sql(in_sql);  
      ELSIF NOT in_echo THEN 
        g_ddl_arr(g_ddl_arr.COUNT+1) := in_sql;
      END IF;
      
      IF in_test THEN
        l_log_time := cort_log_pkg.log(
                        in_log_type => 'TEST EXECUTE',
                        in_text     => in_sql
                      );
      ELSE
        l_log_time := cort_log_pkg.log(
                        in_log_type => 'EXECUTE IMMEDIATE',
                        in_text     => in_sql
                      );
        BEGIN
        $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
          EXECUTE IMMEDIATE in_sql;
        $ELSE
          execute_immediate_10g(in_sql);
        $END
        EXCEPTION
          WHEN OTHERS THEN
            IF in_echo OR g_params.debug THEN
              output(sqlerrm);
            END IF;  
            cort_log_pkg.update_exec_time(
              in_log_time  => l_log_time
            );
            g_error_stack := g_error_stack||dbms_utility.format_error_backtrace||CHR(10);
            RAISE;
        END;
      END IF;  
      cort_log_pkg.update_exec_time(
        in_log_time  => l_log_time
      );
    END IF;
    COMMIT;
  END execute_immediate;
  
  PROCEDURE print_ddl
  AS
  BEGIN
    FOR i IN 1..g_ddl_arr.COUNT LOOP
      echo_sql(g_ddl_arr(i));
      IF g_params.test THEN  
        cort_log_pkg.log(
          in_log_type => 'TEST EXECUTE',
          in_text     => g_ddl_arr(i)
        );
      END IF;  
    END LOOP;
    g_ddl_arr.DELETE;
  END print_ddl;

  -- execute DDL changes
  PROCEDURE int_apply_changes(
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    in_rlbk_alter_stmt_arr IN arrays.gt_clob_arr, -- rollback alter statements
    in_raise_error         IN BOOLEAN,
    in_test                IN BOOLEAN, 
    in_echo                IN BOOLEAN
  )
  AS
    l_indx                 PLS_INTEGER;
    e_compiled_with_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_compiled_with_errors,-24344);
  BEGIN
    l_indx := io_frwd_alter_stmt_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      IF LENGTH(io_frwd_alter_stmt_arr(l_indx)) > 0 THEN
        BEGIN
          execute_immediate(io_frwd_alter_stmt_arr(l_indx), in_echo, in_test);
        EXCEPTION
          -- if trigger created with errors then do not raise exception and carry on
          WHEN e_compiled_with_errors THEN
            NULL;
          WHEN OTHERS THEN
            IF in_raise_error THEN
              debug('Error. Rolling back...');
              l_indx := in_rlbk_alter_stmt_arr.PRIOR(l_indx);
              WHILE l_indx IS NOT NULL LOOP
                execute_immediate(in_rlbk_alter_stmt_arr(l_indx), in_echo, in_test);
                l_indx := in_rlbk_alter_stmt_arr.PRIOR(l_indx);
              END LOOP;
              debug('End of rollback...');
              g_error_stack := g_error_stack||dbms_utility.format_error_backtrace||CHR(10);
              RAISE;
            ELSE
              io_frwd_alter_stmt_arr.DELETE(l_indx);
            END IF;
        END;   
      END IF;
      -- go to next element
      l_indx := io_frwd_alter_stmt_arr.NEXT(l_indx);
    END LOOP;
  END int_apply_changes;

  -- execute DDL changes
  PROCEDURE apply_changes(
    in_frwd_alter_stmt_arr IN arrays.gt_clob_arr, -- forward alter statements
    in_rlbk_alter_stmt_arr IN arrays.gt_clob_arr, -- rollback alter statements
    in_test                IN BOOLEAN DEFAULT FALSE, 
    in_echo                IN BOOLEAN DEFAULT TRUE
  )
  AS
   l_frwd_alter_stmt_arr arrays.gt_clob_arr;
  BEGIN
    l_frwd_alter_stmt_arr := in_frwd_alter_stmt_arr;
    int_apply_changes(
      io_frwd_alter_stmt_arr => l_frwd_alter_stmt_arr,
      in_rlbk_alter_stmt_arr => in_rlbk_alter_stmt_arr,
      in_raise_error         => TRUE,
      in_test                => in_test, 
      in_echo                => in_echo
    );
  END apply_changes;
  
  PROCEDURE apply_changes_ignore_errors(
    io_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    in_test        IN BOOLEAN DEFAULT FALSE, 
    in_echo        IN BOOLEAN DEFAULT TRUE
  )
  AS
    l_rlbk_alter_stmt_arr arrays.gt_clob_arr;
  BEGIN
    int_apply_changes(
      io_frwd_alter_stmt_arr => io_stmt_arr,
      in_rlbk_alter_stmt_arr => l_rlbk_alter_stmt_arr, 
      in_raise_error         => FALSE,
      in_test                => in_test,
      in_echo                => in_echo
    );
  END apply_changes_ignore_errors;

  -- Returns TRUE if table exists otherwise returns FALSE
  FUNCTION object_exists(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
    l_cnt PLS_INTEGER;
  BEGIN
    SELECT COUNT(*)
      INTO l_cnt
      FROM all_objects
     WHERE owner = in_object_owner
       AND object_name = in_object_name
       AND object_type = in_object_type;
    RETURN l_cnt > 0;
  END object_exists;

  -- Returns TRUE if constraint exists otherwise returns FALSE. If in_table_name is NULL then checks across all constraints
  FUNCTION constraint_exists(
    in_cons_name   IN VARCHAR2,
    in_owner       IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
    l_cnt PLS_INTEGER;
  BEGIN
    SELECT COUNT(*)
      INTO l_cnt
      FROM all_constraints
     WHERE owner = in_owner
       AND constraint_name = in_cons_name;
    RETURN l_cnt > 0;
  END constraint_exists;

  -- Returns TRUE if log group exists otherwise returns FALSE. If in_table_name is NULL then checks across all constraints
  FUNCTION log_group_exists(
    in_log_group_name IN VARCHAR2,
    in_owner          IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
    l_cnt PLS_INTEGER;
  BEGIN
    SELECT COUNT(*)
      INTO l_cnt
      FROM all_log_groups
     WHERE owner = in_owner
       AND log_group_name = in_log_group_name;
    RETURN l_cnt > 0;
  END log_group_exists;
  
  -- return DROP object DDL
  FUNCTION get_drop_object_ddl(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_sql VARCHAR2(32767);
  BEGIN
    l_sql :=  'DROP '||in_object_type||' "'||in_object_owner||'"."'||in_object_name||'"';
    IF in_object_type = 'TABLE' THEN
      l_sql := l_sql||' CASCADE CONSTRAINTS';
    END IF;
    RETURN l_sql;
  END get_drop_object_ddl;  

  -- warpper for TABLE
  FUNCTION get_drop_table_ddl(
    in_table_name  IN VARCHAR2,
    in_owner       IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN get_drop_object_ddl(
             in_object_type  => 'TABLE',
             in_object_name  => in_table_name,
             in_object_owner => in_owner
           );
  END get_drop_table_ddl;

  -- warpper for INDEX
  FUNCTION get_drop_index_ddl(
    in_index_name  IN VARCHAR2,
    in_owner       IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN get_drop_object_ddl(
             in_object_type  => 'INDEX',
             in_object_name  => in_index_name,
             in_object_owner => in_owner
           );
  END get_drop_index_ddl;  

  -- warpper for SEQUENCE
  FUNCTION get_drop_sequence_ddl(
    in_sequence_name  IN VARCHAR2,
    in_owner          IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN get_drop_object_ddl(
             in_object_type  => 'SEQUENCE',
             in_object_name  => in_sequence_name,
             in_object_owner => in_owner
           );
  END get_drop_sequence_ddl;  

  -- Drops object if it exists
  PROCEDURE exec_drop_object(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_echo         IN BOOLEAN DEFAULT TRUE,
    in_test         IN BOOLEAN DEFAULT FALSE
  )
  AS
    l_sql           VARCHAR2(1000);
  BEGIN
    IF object_exists(
         in_object_type  => in_object_type,
         in_object_name  => in_object_name,
         in_object_owner => in_object_owner
       )
    THEN
      l_sql := get_drop_object_ddl(
                 in_object_type  => in_object_type,
                 in_object_name  => in_object_name,
                 in_object_owner => in_object_owner
               );
      execute_immediate(
        in_sql  => l_sql, 
        in_echo => in_echo, 
        in_test => in_test 
      );
    END IF;
  END exec_drop_object;

  -- wrapper for table
  PROCEDURE exec_drop_table(
    in_table_name   IN VARCHAR2,
    in_owner        IN VARCHAR2,
    in_echo         IN BOOLEAN DEFAULT TRUE,
    in_test         IN BOOLEAN DEFAULT FALSE
  )
  AS
  BEGIN
    exec_drop_object(
      in_object_type  => 'TABLE',
      in_object_owner => in_owner,
      in_object_name  => in_table_name,
      in_echo         => in_echo,
      in_test         => in_test 
    );
  END exec_drop_table;


  PROCEDURE cleanup_history(
    in_object_owner  IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_type   IN VARCHAR2
  )
  AS
    l_revert_object_arr arrays.gt_str_arr;
  BEGIN
    SELECT revert_name
      BULK COLLECT
      INTO l_revert_object_arr  
      FROM cort_objects 
     WHERE object_owner = in_object_owner 
       AND object_name = in_object_name 
       AND object_type = in_object_type
       AND revert_name IS NOT NULL;

    FOR i IN 1..l_revert_object_arr.COUNT LOOP
      exec_drop_object(
        in_object_type  => in_object_type,
        in_object_name  => l_revert_object_arr(i),
        in_object_owner => in_object_owner
      );
    END LOOP;
    -- delete any previous records for given object
    cort_aux_pkg.cleanup_history(
      in_object_type   => in_object_type,
      in_object_name   => in_object_name,
      in_object_owner  => in_object_owner
    );
  END cleanup_history;     

  -- return string higher than given but not longer than 30 chars
  FUNCTION get_next_name(in_name IN VARCHAR2)
  RETURN VARCHAR2
  AS
    l_next_name VARCHAR2(24);
    l_substr    VARCHAR2(24);
    l_number    BINARY_INTEGER;
  BEGIN
    l_substr := SUBSTR(in_name, 1, 24);
    l_substr := SUBSTR(in_name, -4, 4);
    l_number := utl_raw.cast_to_binary_integer(utl_raw.cast_to_raw(l_substr));
    l_number := l_number + 1;
    l_substr := utl_raw.cast_to_varchar2(utl_raw.cast_from_binary_integer(l_number));
    l_substr := REPLACE(l_substr, '"', '#');
    l_next_name := SUBSTR(in_name, 1, LENGTH(in_name)-4)||l_substr;
    RETURN l_next_name;
  END get_next_name;

  FUNCTION check_object_exist(
    in_owner IN VARCHAR2, 
    in_name  IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
    l_cnt PLS_INTEGER;
  BEGIN
    BEGIN
      SELECT 1
        INTO l_cnt
        FROM all_objects
       WHERE owner = in_owner
         AND object_name = in_name
         AND ROWNUM <= 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_cnt := 0;
    END;    
    RETURN l_cnt = 1;
  END check_object_exist;
  
  -- Returns temp name for given object (table|index|trigger|lob)
  FUNCTION get_object_temp_name(
    in_object_id   IN VARCHAR2,
    in_owner       IN VARCHAR2,
    in_prefix      IN VARCHAR2 DEFAULT gc_cort_temp_prefix
  )
  RETURN VARCHAR2
  AS
    l_temp_name VARCHAR2(24);
  BEGIN
    l_temp_name := in_prefix||TO_CHAR(in_object_id, 'fm0XXXXXXXXXXX');
    WHILE check_object_exist(in_owner, l_temp_name) LOOP
      l_temp_name := get_next_name(l_temp_name);
    END LOOP;
    RETURN l_temp_name;
  END get_object_temp_name;

  -- Returns temp name for current session (tab|ind|trg|lob)
  FUNCTION get_object_temp_name(
    in_object_type IN VARCHAR2,
    in_owner       IN VARCHAR2,
    in_prefix      IN VARCHAR2 DEFAULT gc_cort_temp_prefix
  )
  RETURN VARCHAR2
  AS
    l_object_type VARCHAR2(3);
    l_temp_name   VARCHAR2(24);
  BEGIN
    l_object_type := CASE in_object_type 
                       WHEN 'TABLE'    THEN 'TAB' 
                       WHEN 'INDEX'    THEN 'IND' 
                       WHEN 'TRIGGER'  THEN 'TRG'
                       WHEN 'LOB'      THEN 'LOB'
                       WHEN 'SEQUENCE' THEN 'SEQ'
                     END;
                     
                    --    AAAAA(5)   TTT(3)          XXXXXXXXXX(10)                    FFFFFF(6)
    l_temp_name := SUBSTR(in_prefix||l_object_type||cort_aux_pkg.get_time_str||TO_CHAR(SYS_CONTEXT('USERENV','SID'),'fm0XXXXX'),1, 24);
    WHILE check_object_exist(in_owner, l_temp_name) LOOP
      l_temp_name := get_next_name(l_temp_name);
    END LOOP;
    RETURN l_temp_name;
  END get_object_temp_name;
  
  -- Initializes and return gt_rename_rec record
  FUNCTION get_rename_rec(
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_object_type  IN VARCHAR2
  )
  RETURN gt_rename_rec
  AS
    l_rec gt_rename_rec;
  BEGIN
    l_rec.object_name  := cort_parse_pkg.get_original_name(
                            in_object_type => in_object_type, 
                            in_object_name => in_object_name 
                          );     
    l_rec.object_owner := in_object_owner;  
    l_rec.current_name := in_object_name;    
    
    SELECT object_id, generated
      INTO l_rec.object_id, l_rec.generated
      FROM all_objects
     WHERE owner = in_object_owner
       AND object_type = in_object_type
       AND object_name = in_object_name;
       
    l_rec.temp_name := get_object_temp_name(
                         in_object_type => in_object_type, 
                         in_owner       => l_rec.object_owner,
                         in_prefix      => gc_cort_temp_prefix
                       );
    l_rec.cort_name := get_object_temp_name(
                         in_object_id   => l_rec.object_id, 
                         in_owner       => l_rec.object_owner, 
                         in_prefix      => gc_cort_rlbk_prefix
                       );
    
    RETURN l_rec;
  END get_rename_rec;
  
  -- Initializes and return gt_rename_rec record for constraints and log groups
  FUNCTION get_rename_rec(
    in_table_name_rec IN gt_rename_rec,
    in_object_name    IN VARCHAR2,
    in_object_owner   IN VARCHAR2,
    in_object_type    IN VARCHAR2,
    in_index          IN PLS_INTEGER,
    in_generated      IN VARCHAR2
  )
  RETURN gt_rename_rec
  AS
    l_rec     gt_rename_rec;
    l_prefix  VARCHAR2(10);
  BEGIN
    l_rec.object_name  := cort_parse_pkg.get_original_name(
                            in_object_type => in_object_type, 
                            in_object_name => in_object_name 
                          );     
    l_rec.object_owner := in_object_owner;
    l_rec.object_id := NULL; 
    l_rec.generated := in_generated;    
    l_rec.current_name := in_object_name;    
    l_prefix := '$'||SUBSTR(in_object_type,1,1);
    l_rec.temp_name := in_table_name_rec.temp_name||l_prefix||TO_CHAR(in_index,'fm0XXX');
    l_rec.cort_name := in_table_name_rec.cort_name||l_prefix||TO_CHAR(in_index,'fm0XXX');
    RETURN l_rec;
  END get_rename_rec; 
  
  -- return column indx in table column_arr by name
  FUNCTION get_column_indx(
    in_table_rec   IN cort_exec_pkg.gt_table_rec,
    in_column_name IN VARCHAR2
  )
  RETURN PLS_INTEGER
  AS
  BEGIN
    IF in_table_rec.column_indx_arr.EXISTS(in_column_name) THEN
      RETURN in_table_rec.column_indx_arr(in_column_name);
    ELSE
      IF in_table_rec.column_qualified_indx_arr.EXISTS(in_column_name) THEN
        RETURN in_table_rec.column_qualified_indx_arr(in_column_name);
      ELSE
        RETURN NULL;
      END IF;
    END IF;
  END get_column_indx;


  PROCEDURE read_subpartition_template(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE t_subpartition_templates_arr IS TABLE OF all_subpartition_templates%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE t_lob_templates_arr          IS TABLE OF all_lob_templates%ROWTYPE INDEX BY PLS_INTEGER;
    l_subpartition_templates_arr  t_subpartition_templates_arr;
    l_subpart_template_indx       arrays.gt_str_indx;
    l_lob_templates_arr           t_lob_templates_arr;
    l_lob_template_rec            gt_lob_template_rec;
    l_indx                        PLS_INTEGER;
    l_cnt                         PLS_INTEGER;
  BEGIN
    SELECT *
      BULK COLLECT
      INTO l_subpartition_templates_arr
      FROM all_subpartition_templates
     WHERE user_name = io_table_rec.owner
       AND table_name = io_table_rec.table_name
     ORDER BY subpartition_position;

    FOR i IN 1..l_subpartition_templates_arr.COUNT LOOP
      io_table_rec.subpartition_template_arr(i).subpartition_type := io_table_rec.subpartitioning_type;
      io_table_rec.subpartition_template_arr(i).subpartition_name := l_subpartition_templates_arr(i).subpartition_name;
      io_table_rec.subpartition_template_arr(i).subpartition_position := l_subpartition_templates_arr(i).subpartition_position;
      io_table_rec.subpartition_template_arr(i).tablespace_name := l_subpartition_templates_arr(i).tablespace_name;
      io_table_rec.subpartition_template_arr(i).high_bound := l_subpartition_templates_arr(i).high_bound;
      l_subpart_template_indx(l_subpartition_templates_arr(i).subpartition_name) := i;
    END LOOP;

    SELECT DISTINCT *   -- use distinct as a workaround for Oracle bug - missing join condition which lead to rows duplication
      BULK COLLECT
      INTO l_lob_templates_arr
      FROM all_lob_templates
     WHERE user_name = io_table_rec.owner
       AND table_name = io_table_rec.table_name;

    FOR i IN 1..l_lob_templates_arr.COUNT LOOP
      l_lob_template_rec.subpartition_name := l_lob_templates_arr(i).subpartition_name;
      l_lob_template_rec.lob_column_name := l_lob_templates_arr(i).lob_col_name;
      l_lob_template_rec.lob_segment_name := l_lob_templates_arr(i).lob_segment_name;
      l_lob_template_rec.tablespace_name := l_lob_templates_arr(i).tablespace_name;
      IF l_subpart_template_indx.EXISTS(l_lob_template_rec.subpartition_name) THEN
        l_indx := l_subpart_template_indx(l_lob_template_rec.subpartition_name);
        l_cnt := io_table_rec.subpartition_template_arr(l_indx).lob_template_arr.COUNT;
        io_table_rec.subpartition_template_arr(l_indx).lob_template_arr(l_cnt+1) := l_lob_template_rec;
      END IF;
    END LOOP;

  END read_subpartition_template;

  -- read table attributes from all_tables/all_part_tables
  PROCEDURE read_table(
    in_table_name      IN VARCHAR2,
    in_owner           IN VARCHAR2,
    out_table_rec      OUT NOCOPY gt_table_rec
  )
  AS
    l_all_tables_rec           all_all_tables%ROWTYPE;
    l_indexes_rec              all_indexes%ROWTYPE;
    l_part_tables_rec          all_part_tables%ROWTYPE;
    l_part_indexes_rec         all_part_indexes%ROWTYPE;
    l_tablespace_name_arr      arrays.gt_str_arr; 
    l_block_size_arr           arrays.gt_num_arr;
--    l_object_id                all_objects.object_id%TYPE;
    l_sql                      VARCHAR2(1000);
    l_cnt                      PLS_INTEGER;
  BEGIN
    BEGIN
      SELECT *
        INTO l_all_tables_rec
        FROM all_all_tables
       WHERE owner = in_owner
         AND table_name = in_table_name;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        raise_error('Table "'||in_owner||'"."'||in_table_name||'" not found');
        RETURN;
    END;

    out_table_rec.owner :=  l_all_tables_rec.owner;
    out_table_rec.table_name := l_all_tables_rec.table_name;
    out_table_rec.tablespace_name := l_all_tables_rec.tablespace_name;
    out_table_rec.cluster_name := l_all_tables_rec.cluster_name;
    out_table_rec.cluster_owner := l_all_tables_rec.cluster_owner;
    out_table_rec.iot_name := l_all_tables_rec.iot_name;
    out_table_rec.iot_type := l_all_tables_rec.iot_type;
    out_table_rec.partitioned := l_all_tables_rec.partitioned;
    out_table_rec.temporary := l_all_tables_rec.temporary;
    out_table_rec.duration := l_all_tables_rec.duration;
    out_table_rec.secondary := l_all_tables_rec.secondary;
    out_table_rec.nested := l_all_tables_rec.nested;
    out_table_rec.object_id_type := l_all_tables_rec.object_id_type;
    out_table_rec.table_type_owner := l_all_tables_rec.table_type_owner;
    out_table_rec.table_type := l_all_tables_rec.table_type;
    out_table_rec.row_movement := l_all_tables_rec.row_movement;
    out_table_rec.dependencies := l_all_tables_rec.dependencies;
    out_table_rec.physical_attr_rec.pct_free := NVL(l_all_tables_rec.pct_free,10);
    out_table_rec.physical_attr_rec.pct_used := NVL(l_all_tables_rec.pct_used,10);
    out_table_rec.physical_attr_rec.ini_trans := NVL(l_all_tables_rec.ini_trans,1);
    out_table_rec.physical_attr_rec.max_trans := NVL(l_all_tables_rec.max_trans,255);
    out_table_rec.physical_attr_rec.storage.initial_extent := l_all_tables_rec.initial_extent;
--    out_table_rec.physical_attr_rec.storage.next_extent := l_all_tables_rec.next_extent;
    out_table_rec.physical_attr_rec.storage.min_extents := l_all_tables_rec.min_extents;
--    out_table_rec.physical_attr_rec.storage.max_extents := l_all_tables_rec.max_extents;
    out_table_rec.physical_attr_rec.storage.pct_increase := l_all_tables_rec.pct_increase;
    out_table_rec.physical_attr_rec.storage.freelists := l_all_tables_rec.freelists;
    out_table_rec.physical_attr_rec.storage.freelist_groups := l_all_tables_rec.freelist_groups;
    out_table_rec.physical_attr_rec.storage.buffer_pool := NULLIF(l_all_tables_rec.buffer_pool,'DEFAULT');
    out_table_rec.compression_rec.compression := l_all_tables_rec.compression;
    $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
    out_table_rec.compression_rec.compress_for := l_all_tables_rec.compress_for;
    $END
    out_table_rec.parallel_rec.degree := l_all_tables_rec.degree;
    out_table_rec.parallel_rec.instances := l_all_tables_rec.instances;
    out_table_rec.logging := l_all_tables_rec.logging;
    out_table_rec.cache := l_all_tables_rec.cache;
    out_table_rec.monitoring := l_all_tables_rec.monitoring;
    $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=2 $THEN
    BEGIN
      SELECT read_only, result_cache
        INTO out_table_rec.read_only, out_table_rec.result_cache
        FROM all_tables
       WHERE owner = in_owner
         AND table_name = in_table_name;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;    
    $END

    out_table_rec.rename_rec := get_rename_rec(
                                  in_object_name  => out_table_rec.table_name,
                                  in_object_owner => out_table_rec.owner, 
                                  in_object_type  => 'TABLE'
                                );

    -- Read IOT attributes for IOT tables
    IF out_table_rec.iot_type IS NOT NULL THEN
      BEGIN
        SELECT *
          INTO l_indexes_rec
          FROM all_indexes
         WHERE table_owner = in_owner
           AND table_name = in_table_name
           AND index_type = 'IOT - TOP';
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          RETURN;
      END;
      out_table_rec.iot_index_name := l_indexes_rec.index_name;
      out_table_rec.iot_index_owner := l_indexes_rec.owner;
      out_table_rec.iot_pct_threshold := l_indexes_rec.pct_threshold;
      out_table_rec.iot_include_column := l_indexes_rec.include_column;
      out_table_rec.iot_key_compression := l_indexes_rec.compression;
      out_table_rec.iot_prefix_length := l_indexes_rec.prefix_length;
      out_table_rec.physical_attr_rec.pct_free := l_indexes_rec.pct_free;
      out_table_rec.physical_attr_rec.ini_trans := l_indexes_rec.ini_trans;
      out_table_rec.physical_attr_rec.max_trans := l_indexes_rec.max_trans;
      out_table_rec.physical_attr_rec.storage.initial_extent := l_indexes_rec.initial_extent;
--      out_table_rec.physical_attr_rec.storage.next_extent := l_indexes_rec.next_extent;
      out_table_rec.physical_attr_rec.storage.min_extents := l_indexes_rec.min_extents;
--      out_table_rec.physical_attr_rec.storage.max_extents := l_indexes_rec.max_extents;
      out_table_rec.physical_attr_rec.storage.pct_increase := l_indexes_rec.pct_increase;
      out_table_rec.physical_attr_rec.storage.freelists := l_indexes_rec.freelists;
      out_table_rec.physical_attr_rec.storage.freelist_groups := l_indexes_rec.freelist_groups;
      out_table_rec.physical_attr_rec.storage.buffer_pool := NULLIF(l_indexes_rec.buffer_pool,'DEFAULT');
      out_table_rec.tablespace_name := l_indexes_rec.tablespace_name;
      out_table_rec.logging := l_indexes_rec.logging;

      SELECT column_name, descend
        BULK COLLECT
        INTO out_table_rec.iot_pk_column_arr, out_table_rec.iot_pk_column_sort_type_arr
        FROM all_ind_columns
       WHERE table_owner = in_owner
         AND table_name = in_table_name
         AND index_name = out_table_rec.iot_index_name
         AND index_owner = out_table_rec.iot_index_owner
       ORDER BY column_position;

      BEGIN
        SELECT *
          INTO l_all_tables_rec
          FROM all_all_tables
         WHERE owner = in_owner
           AND iot_name = in_table_name
           AND iot_type = 'IOT_OVERFLOW';
           
        out_table_rec.overflow_table_name := l_all_tables_rec.table_name;   
        out_table_rec.overflow_physical_attr_rec.pct_free := l_all_tables_rec.pct_free;
        out_table_rec.overflow_physical_attr_rec.pct_used := l_all_tables_rec.pct_used;
        out_table_rec.overflow_physical_attr_rec.ini_trans := l_all_tables_rec.ini_trans;
        out_table_rec.overflow_physical_attr_rec.max_trans := l_all_tables_rec.max_trans;
        out_table_rec.overflow_physical_attr_rec.storage.initial_extent := l_all_tables_rec.initial_extent;
--        out_table_rec.overflow_physical_attr_rec.storage.next_extent := l_all_tables_rec.next_extent;
        out_table_rec.overflow_physical_attr_rec.storage.min_extents := l_all_tables_rec.min_extents;
--        out_table_rec.overflow_physical_attr_rec.storage.max_extents := l_all_tables_rec.max_extents;
        out_table_rec.overflow_physical_attr_rec.storage.pct_increase := l_all_tables_rec.pct_increase;
        out_table_rec.overflow_physical_attr_rec.storage.freelists := l_all_tables_rec.freelists;
        out_table_rec.overflow_physical_attr_rec.storage.freelist_groups := l_all_tables_rec.freelist_groups;
        out_table_rec.overflow_physical_attr_rec.storage.buffer_pool := NULLIF(l_all_tables_rec.buffer_pool,'DEFAULT');
        out_table_rec.overflow_tablespace := l_indexes_rec.tablespace_name;
        out_table_rec.overflow_logging := l_indexes_rec.logging;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          NULL;
      END;

      SELECT DECODE(COUNT(*), 0, 'N', 'Y')
        INTO out_table_rec.mapping_table
        FROM all_all_tables
       WHERE owner = in_owner
         AND iot_name = in_table_name
         AND iot_type = 'IOT_MAPPING';
    END IF;

    -- Read attribures for partitioned tables (non IOT)
    IF out_table_rec.partitioned = 'YES' AND out_table_rec.iot_type IS NULL THEN
      BEGIN
        SELECT *
          INTO l_part_tables_rec
          FROM all_part_tables
         WHERE owner = in_owner
           AND table_name = in_table_name;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          RETURN;
      END;

      out_table_rec.tablespace_name := l_part_tables_rec.def_tablespace_name;
      out_table_rec.partitioning_type := l_part_tables_rec.partitioning_type;
      out_table_rec.subpartitioning_type := l_part_tables_rec.subpartitioning_type;
      $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
      out_table_rec.ref_ptn_constraint_name := l_part_tables_rec.ref_ptn_constraint_name;
      out_table_rec.interval := l_part_tables_rec.interval;
      $END
      out_table_rec.physical_attr_rec.pct_free := l_part_tables_rec.def_pct_free;
      out_table_rec.physical_attr_rec.pct_used := l_part_tables_rec.def_pct_used;
      out_table_rec.physical_attr_rec.ini_trans := l_part_tables_rec.def_ini_trans;
      out_table_rec.physical_attr_rec.max_trans := l_part_tables_rec.def_max_trans;
      out_table_rec.physical_attr_rec.storage.initial_extent := NULLIF(l_part_tables_rec.def_initial_extent,'DEFAULT');
--      out_table_rec.physical_attr_rec.storage.next_extent := NULLIF(l_part_tables_rec.def_next_extent,'DEFAULT');
      out_table_rec.physical_attr_rec.storage.min_extents := NULLIF(l_part_tables_rec.def_min_extents,'DEFAULT');
--      out_table_rec.physical_attr_rec.storage.max_extents := NULLIF(l_part_tables_rec.def_max_extents,'DEFAULT');
      out_table_rec.physical_attr_rec.storage.pct_increase := NULLIF(l_part_tables_rec.def_pct_increase,'DEFAULT');
      out_table_rec.physical_attr_rec.storage.freelists := l_part_tables_rec.def_freelists;
      out_table_rec.physical_attr_rec.storage.freelist_groups := l_part_tables_rec.def_freelist_groups;
      out_table_rec.physical_attr_rec.storage.buffer_pool := NULLIF(l_part_tables_rec.def_buffer_pool,'DEFAULT');
      out_table_rec.compression_rec.compression := l_part_tables_rec.def_compression;
      $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
      out_table_rec.compression_rec.compress_for := l_part_tables_rec.def_compress_for;
      $END
      out_table_rec.logging := l_part_tables_rec.def_logging;

      SELECT column_name
        BULK COLLECT
        INTO out_table_rec.part_key_column_arr
        FROM all_part_key_columns
       WHERE owner = in_owner
         AND name = in_table_name
         AND object_type = 'TABLE'
       ORDER BY column_position;

      IF out_table_rec.subpartitioning_type <> 'NONE' THEN
        SELECT column_name
          BULK COLLECT
          INTO out_table_rec.subpart_key_column_arr
          FROM all_subpart_key_columns
         WHERE owner = in_owner
           AND name = in_table_name
           AND object_type = 'TABLE'
         ORDER BY column_position;

        -- read subpartition template
        read_subpartition_template(out_table_rec);
      END IF;

    END IF;

    -- Read attribures for partitioned tables (IOT)
    IF out_table_rec.partitioned = 'YES' AND out_table_rec.iot_type IS NOT NULL THEN
      BEGIN
        SELECT *
          INTO l_part_indexes_rec
          FROM all_part_indexes
         WHERE owner = in_owner
           AND table_name = in_table_name
           AND index_name = out_table_rec.iot_index_name;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          RETURN;
      END;

      out_table_rec.tablespace_name := l_part_indexes_rec.def_tablespace_name;
      out_table_rec.partitioning_type := l_part_indexes_rec.partitioning_type;
      out_table_rec.subpartitioning_type := l_part_indexes_rec.subpartitioning_type;
      $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
      out_table_rec.interval := l_part_indexes_rec.interval;
      $END
      out_table_rec.physical_attr_rec.pct_free := l_part_indexes_rec.def_pct_free;
      out_table_rec.physical_attr_rec.ini_trans := l_part_indexes_rec.def_ini_trans;
      out_table_rec.physical_attr_rec.max_trans := l_part_indexes_rec.def_max_trans;
      out_table_rec.physical_attr_rec.storage.initial_extent := NULLIF(l_part_indexes_rec.def_initial_extent,'DEFAULT');
--      out_table_rec.physical_attr_rec.storage.next_extent := NULLIF(l_part_indexes_rec.def_next_extent,'DEFAULT');
      out_table_rec.physical_attr_rec.storage.min_extents := NULLIF(l_part_indexes_rec.def_min_extents,'DEFAULT');
--      out_table_rec.physical_attr_rec.storage.max_extents := NULLIF(l_part_indexes_rec.def_max_extents,'DEFAULT');
      out_table_rec.physical_attr_rec.storage.pct_increase := NULLIF(l_part_indexes_rec.def_pct_increase,'DEFAULT');
      out_table_rec.physical_attr_rec.storage.freelists := l_part_indexes_rec.def_freelists;
      out_table_rec.physical_attr_rec.storage.freelist_groups := l_part_indexes_rec.def_freelist_groups;
      out_table_rec.physical_attr_rec.storage.buffer_pool := NULLIF(l_part_indexes_rec.def_buffer_pool,'DEFAULT');

      SELECT column_name
        BULK COLLECT
        INTO out_table_rec.part_key_column_arr
        FROM all_part_key_columns
       WHERE owner = out_table_rec.iot_index_owner
         AND name = out_table_rec.iot_index_name
         AND object_type = 'INDEX'
       ORDER BY column_position;

      IF out_table_rec.subpartitioning_type <> 'NONE' THEN
        SELECT column_name
          BULK COLLECT
          INTO out_table_rec.subpart_key_column_arr
          FROM all_subpart_key_columns
         WHERE owner = out_table_rec.iot_index_owner
           AND name = out_table_rec.iot_index_name
           AND object_type = 'INDEX'
         ORDER BY column_position;

        -- read subpartition template
        read_subpartition_template(out_table_rec);
      END IF;
    END IF;
    
    SELECT tablespace_name, block_size
      BULK COLLECT
      INTO l_tablespace_name_arr, l_block_size_arr 
      FROM user_tablespaces;
      
    FOR i IN 1..l_tablespace_name_arr.COUNT LOOP
      out_table_rec.tablespace_block_size_indx(l_tablespace_name_arr(i)) := l_block_size_arr(i); 
    END LOOP;
    
--    out_table_rec.table_name := out_table_rec.rename_rec.object_name;
    l_sql := 'SELECT COUNT(*) FROM "'||in_owner||'"."'||in_table_name||'" WHERE ROWNUM = 1';
    debug('rowcount sql = '||l_sql);
    EXECUTE IMMEDIATE l_sql INTO l_cnt;
    out_table_rec.is_table_empty := l_cnt = 0;
  END read_table;

  -- read table attributes from all_tables/all_part_tables
  PROCEDURE read_table_columns(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE t_all_tab_cols_arr IS TABLE OF all_tab_cols%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE t_all_enc_cols_arr IS TABLE OF all_encrypted_columns%ROWTYPE INDEX BY PLS_INTEGER;
    l_tab_cols_arr             t_all_tab_cols_arr;
    l_enc_cols_arr             t_all_enc_cols_arr;
    l_indx                     PLS_INTEGER;
    l_part_key_col_indx_arr    arrays.gt_int_indx; -- index by column name of partition key columns 
    l_subpart_key_col_indx_arr arrays.gt_int_indx; -- index by column name of of subpartition key columns    
  BEGIN
    SELECT *
      BULK COLLECT
      INTO l_tab_cols_arr
      FROM all_tab_cols
     WHERE owner = io_table_rec.owner
       AND table_name = io_table_rec.table_name
     ORDER BY internal_column_id;

    FOR i IN 1..io_table_rec.part_key_column_arr.COUNT LOOP
      l_part_key_col_indx_arr(io_table_rec.part_key_column_arr(i)) := i;
    END LOOP;  
    
    FOR i IN 1..io_table_rec.subpart_key_column_arr.COUNT LOOP
      l_subpart_key_col_indx_arr(io_table_rec.subpart_key_column_arr(i)) := i;
    END LOOP;  
 

    FOR i IN 1..l_tab_cols_arr.COUNT LOOP
      io_table_rec.column_indx_arr(l_tab_cols_arr(i).column_name) := i;
      io_table_rec.column_qualified_indx_arr(l_tab_cols_arr(i).qualified_col_name) := i;
      io_table_rec.column_arr(i).column_name := l_tab_cols_arr(i).column_name;
      io_table_rec.column_arr(i).column_indx := i;
      io_table_rec.column_arr(i).data_type := l_tab_cols_arr(i).data_type;
      io_table_rec.column_arr(i).data_type_mod := l_tab_cols_arr(i).data_type_mod;
      io_table_rec.column_arr(i).data_type_owner := l_tab_cols_arr(i).data_type_owner;
      io_table_rec.column_arr(i).data_length := l_tab_cols_arr(i).data_length;
      io_table_rec.column_arr(i).data_precision := l_tab_cols_arr(i).data_precision;
      io_table_rec.column_arr(i).data_scale := l_tab_cols_arr(i).data_scale;
      io_table_rec.column_arr(i).nullable := l_tab_cols_arr(i).nullable;
      io_table_rec.column_arr(i).column_id := l_tab_cols_arr(i).column_id;
      io_table_rec.column_arr(i).data_default := REGEXP_REPLACE(l_tab_cols_arr(i).data_default, '\s$', '');
      io_table_rec.column_arr(i).character_set_name := l_tab_cols_arr(i).character_set_name;
      io_table_rec.column_arr(i).char_col_decl_length := l_tab_cols_arr(i).char_col_decl_length;
      io_table_rec.column_arr(i).char_length := l_tab_cols_arr(i).char_length;
      io_table_rec.column_arr(i).char_used := l_tab_cols_arr(i).char_used;
      io_table_rec.column_arr(i).internal_column_id := l_tab_cols_arr(i).internal_column_id;
      io_table_rec.column_arr(i).hidden_column := l_tab_cols_arr(i).hidden_column;
      io_table_rec.column_arr(i).qualified_col_name := l_tab_cols_arr(i).qualified_col_name;
      $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
      io_table_rec.column_arr(i).virtual_column := l_tab_cols_arr(i).virtual_column;
      -- XMLTYPE columns marked as virtual columns based on hidden lob column or scalar column(s).
      -- But XMLTYPE behaves as regular column
      IF io_table_rec.column_arr(i).data_type_owner IS NOT NULL AND 
         io_table_rec.column_arr(i).virtual_column = 'YES'
      THEN
        io_table_rec.column_arr(i).virtual_column := 'NO';
      END IF;
      $END
      io_table_rec.column_arr(i).segment_column_id := l_tab_cols_arr(i).segment_column_id;
      io_table_rec.column_arr(i).partition_key := l_part_key_col_indx_arr.EXISTS(io_table_rec.column_arr(i).column_name) OR 
                                                  l_subpart_key_col_indx_arr.EXISTS(io_table_rec.column_arr(i).column_name);
      IF io_table_rec.column_arr(i).data_type_owner IS NULL THEN
        CASE
        WHEN io_table_rec.column_arr(i).data_type LIKE 'TIMESTAMP(_)' THEN
          io_table_rec.column_arr(i).data_type := 'TIMESTAMP';
        WHEN io_table_rec.column_arr(i).data_type LIKE 'TIMESTAMP(_) WITH TIME ZONE' THEN
          io_table_rec.column_arr(i).data_type := 'TIMESTAMP WITH TIME ZONE';
        WHEN io_table_rec.column_arr(i).data_type LIKE 'TIMESTAMP(_) WITH LOCAL TIME ZONE' THEN
          io_table_rec.column_arr(i).data_type := 'TIMESTAMP WITH LOCAL TIME ZONE';
        WHEN io_table_rec.column_arr(i).data_type LIKE 'INTERVAL YEAR()) TO MONTH' THEN
          io_table_rec.column_arr(i).data_type := 'INTERVAL YEAR TO MONTH';
        WHEN io_table_rec.column_arr(i).data_type LIKE 'INTERVAL DAY(_) TO SECOND(_)' THEN
          io_table_rec.column_arr(i).data_type := 'INTERVAL DAY TO SECOND';
        ELSE
          NULL;
        END CASE;   
      END IF;
      io_table_rec.column_arr(i).temp_column_name := gc_cort_temp_prefix||'COLUMN_'||TO_CHAR(i,'fm0XXX');
    END LOOP;

    $IF dbms_db_version.version >= 10 AND dbms_db_version.release >=2 $THEN
    SELECT *
      BULK COLLECT
      INTO l_enc_cols_arr
      FROM all_encrypted_columns
     WHERE owner = io_table_rec.owner
       AND table_name = io_table_rec.table_name;

    FOR i IN 1..l_enc_cols_arr.COUNT LOOP
      IF io_table_rec.column_indx_arr.EXISTS(l_enc_cols_arr(i).column_name) THEN
        l_indx := io_table_rec.column_indx_arr(l_enc_cols_arr(i).column_name);
        io_table_rec.column_arr(l_indx).encryption_alg := l_enc_cols_arr(i).encryption_alg;
        io_table_rec.column_arr(l_indx).salt := l_enc_cols_arr(i).salt;
      END IF;
    END LOOP;
    $END
  END read_table_columns;

  -- read index columns/expressions
  PROCEDURE read_index_cols(
    io_index_rec   IN OUT NOCOPY gt_index_rec
  )  
  AS
    l_indx       PLS_INTEGER;
    l_column_arr arrays.gt_lstr_arr; 
  BEGIN
    -- Read index columns
    IF io_index_rec.index_type = 'BITMAP' AND io_index_rec.join_index =  'YES' THEN
      SELECT table_owner, table_name, column_name
        BULK COLLECT
        INTO io_index_rec.column_table_owner_arr, io_index_rec.column_table_arr, io_index_rec.column_arr
        FROM all_ind_columns
       WHERE index_owner = io_index_rec.owner
         AND index_name = io_index_rec.index_name
       ORDER BY column_position;

      SELECT inner_table_owner, inner_table_name, inner_table_column,
             outer_table_owner, outer_table_name, outer_table_column
        BULK COLLECT
        INTO io_index_rec.join_inner_owner_arr,
             io_index_rec.join_inner_table_arr,
             io_index_rec.join_inner_column_arr,
             io_index_rec.join_outer_owner_arr,
             io_index_rec.join_outer_table_arr,
             io_index_rec.join_outer_column_arr
        FROM all_join_ind_columns
       WHERE index_owner = io_index_rec.owner
         AND index_name = io_index_rec.index_name;
    ELSE
      IF io_index_rec.table_object_type IS NOT NULL THEN
        SELECT c.column_name, c.descend, ex.column_expression 
          BULK COLLECT
          INTO l_column_arr, io_index_rec.sort_order_arr, io_index_rec.column_expr_arr
          FROM all_ind_columns c
          LEFT JOIN all_ind_expressions ex
            ON ex.index_owner = c.index_owner
           AND ex.index_name = c.index_name
           AND ex.column_position = c.column_position       
         WHERE c.index_owner = io_index_rec.owner
           AND c.index_name = io_index_rec.index_name
         ORDER BY c.column_position;
        -- Store long names for indexes on onbject tables in expressions 
        FOR i IN 1..l_column_arr.COUNT LOOP
          IF INSTR(l_column_arr(i),'"') > 0 THEN
            io_index_rec.column_expr_arr(i) := l_column_arr(i);
          ELSE
            io_index_rec.column_arr(i) := l_column_arr(i);
          END IF;  
        END LOOP;   
      ELSE
        SELECT c.column_name, c.descend, ex.column_expression
          BULK COLLECT
          INTO io_index_rec.column_arr, io_index_rec.sort_order_arr, io_index_rec.column_expr_arr
          FROM all_ind_columns c
          LEFT JOIN all_ind_expressions ex
            ON ex.index_owner = c.index_owner
           AND ex.index_name = c.index_name
           AND ex.column_position = c.column_position       
         WHERE c.index_owner = io_index_rec.owner
           AND c.index_name = io_index_rec.index_name
         ORDER BY c.column_position;
      END IF;   
    END IF;
  END read_index_cols;
  
  
  -- read attributes for partitioned index
  PROCEDURE read_part_index(
    io_index_rec   IN OUT NOCOPY gt_index_rec
  )
  AS
    l_part_indexes_rec         gt_all_part_indexes_rec;
  BEGIN
    -- Read attribures for partitioned indexes
    IF io_index_rec.partitioned = 'YES' THEN
      BEGIN
        SELECT *
          INTO l_part_indexes_rec
          FROM all_part_indexes
         WHERE owner = io_index_rec.owner
           AND index_name = io_index_rec.index_name;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          RETURN;
      END;

      io_index_rec.tablespace_name   := l_part_indexes_rec.def_tablespace_name;
      io_index_rec.physical_attr_rec.pct_free  := l_part_indexes_rec.def_pct_free;
      io_index_rec.physical_attr_rec.ini_trans := l_part_indexes_rec.def_ini_trans;
      io_index_rec.physical_attr_rec.max_trans := l_part_indexes_rec.def_max_trans;
      io_index_rec.physical_attr_rec.storage.initial_extent := NULLIF(l_part_indexes_rec.def_initial_extent,'DEFAULT');
  --    io_index_rec.physical_attr_rec.storage.next_extent := l_part_indexes_rec.def_next_extent;
      io_index_rec.physical_attr_rec.storage.min_extents := NULLIF(l_part_indexes_rec.def_min_extents,'DEFAULT');
  --    io_index_rec.physical_attr_rec.storage.max_extents := l_part_indexes_rec.def_max_extents;
      io_index_rec.physical_attr_rec.storage.pct_increase := NULLIF(l_part_indexes_rec.def_pct_increase,'DEFAULT');
      io_index_rec.physical_attr_rec.storage.freelists := l_part_indexes_rec.def_freelists;
      io_index_rec.physical_attr_rec.storage.freelist_groups := l_part_indexes_rec.def_freelist_groups;
      io_index_rec.physical_attr_rec.storage.buffer_pool := NULLIF(l_part_indexes_rec.def_buffer_pool,'DEFAULT');
      io_index_rec.logging                := l_part_indexes_rec.def_logging;
      io_index_rec.partitioning_type      := l_part_indexes_rec.partitioning_type;
      io_index_rec.subpartitioning_type   := l_part_indexes_rec.subpartitioning_type;
      io_index_rec.locality               := l_part_indexes_rec.locality;
      io_index_rec.alignment              := l_part_indexes_rec.alignment;
      $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
      io_index_rec.interval               := l_part_indexes_rec.interval;
      $END

      SELECT column_name
        BULK COLLECT
        INTO io_index_rec.part_key_column_arr
        FROM all_part_key_columns
       WHERE owner = io_index_rec.owner
         AND name = io_index_rec.index_name
         AND object_type = 'INDEX'
       ORDER BY column_position;

      IF io_index_rec.subpartitioning_type <> 'NONE' THEN
        SELECT column_name
          BULK COLLECT
          INTO io_index_rec.subpart_key_column_arr
          FROM all_subpart_key_columns
         WHERE owner = io_index_rec.owner
           AND name = io_index_rec.index_name
           AND object_type = 'INDEX'
         ORDER BY column_position;
      END IF;
    END IF;    
  END read_part_index;

  PROCEDURE assign_index_rec(
    io_index_rec         IN OUT NOCOPY gt_index_rec,
    in_all_indexes_rec   IN gt_all_indexes_rec
  )
  AS
    l_object_id INTEGER;
  BEGIN
    io_index_rec.owner             := in_all_indexes_rec.owner;
    io_index_rec.index_name        := in_all_indexes_rec.index_name;
    io_index_rec.index_type        := in_all_indexes_rec.index_type;
    io_index_rec.table_owner       := in_all_indexes_rec.table_owner;
    io_index_rec.table_name        := in_all_indexes_rec.table_name;
    io_index_rec.table_type        := in_all_indexes_rec.table_type;
    io_index_rec.uniqueness        := in_all_indexes_rec.uniqueness;
    io_index_rec.compression       := in_all_indexes_rec.compression;
    io_index_rec.prefix_length     := in_all_indexes_rec.prefix_length;
    io_index_rec.tablespace_name   := in_all_indexes_rec.tablespace_name;
    io_index_rec.physical_attr_rec.pct_free  := in_all_indexes_rec.pct_free;
    io_index_rec.physical_attr_rec.ini_trans := in_all_indexes_rec.ini_trans;
    io_index_rec.physical_attr_rec.max_trans := in_all_indexes_rec.max_trans;
    io_index_rec.physical_attr_rec.storage.initial_extent := in_all_indexes_rec.initial_extent;
--    io_index_rec.physical_attr_rec.storage.next_extent := in_all_indexes_rec.next_extent;
    io_index_rec.physical_attr_rec.storage.min_extents := in_all_indexes_rec.min_extents;
--    io_index_rec.physical_attr_rec.storage.max_extents := in_all_indexes_rec.max_extents;
    io_index_rec.physical_attr_rec.storage.pct_increase := in_all_indexes_rec.pct_increase;
    io_index_rec.physical_attr_rec.storage.freelists := in_all_indexes_rec.freelists;
    io_index_rec.physical_attr_rec.storage.freelist_groups := in_all_indexes_rec.freelist_groups;
    io_index_rec.physical_attr_rec.storage.buffer_pool := NULLIF(in_all_indexes_rec.buffer_pool,'DEFAULT');
    io_index_rec.pct_threshold          := in_all_indexes_rec.pct_threshold;
    io_index_rec.include_column         := in_all_indexes_rec.include_column;
    io_index_rec.parallel_rec.degree    := in_all_indexes_rec.degree;
    io_index_rec.parallel_rec.instances := in_all_indexes_rec.instances;
    io_index_rec.logging                := in_all_indexes_rec.logging;
    io_index_rec.partitioned            := in_all_indexes_rec.partitioned;
    io_index_rec.temporary              := in_all_indexes_rec.temporary;
    io_index_rec.duration               := in_all_indexes_rec.duration;
    io_index_rec.generated              := in_all_indexes_rec.generated;
    io_index_rec.secondary              := in_all_indexes_rec.secondary;
    io_index_rec.pct_direct_access      := in_all_indexes_rec.pct_direct_access;
    io_index_rec.ityp_owner             := in_all_indexes_rec.ityp_owner;
    io_index_rec.ityp_name              := in_all_indexes_rec.ityp_name;
    io_index_rec.parameters             := in_all_indexes_rec.parameters;
    io_index_rec.domidx_status          := in_all_indexes_rec.domidx_status;
    io_index_rec.domidx_opstatus        := in_all_indexes_rec.domidx_opstatus;
    io_index_rec.funcidx_status         := in_all_indexes_rec.funcidx_status;
    io_index_rec.join_index             := in_all_indexes_rec.join_index;
    $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
    io_index_rec.visibility             := in_all_indexes_rec.visibility;
    $END
    io_index_rec.recreate_flag := FALSE;
    io_index_rec.drop_flag     := FALSE;

    io_index_rec.rename_rec := get_rename_rec(
                                 in_object_name  => io_index_rec.index_name,
                                 in_object_owner => io_index_rec.owner, 
                                 in_object_type  => 'INDEX'
                               );

    read_index_cols(io_index_rec => io_index_rec);

    read_part_index(io_index_rec => io_index_rec);

  END assign_index_rec;

  --  read individual index
  PROCEDURE read_index(
    in_index_name  IN VARCHAR2,
    in_owner       IN VARCHAR2,
    out_index_rec  OUT NOCOPY gt_index_rec
  )
  AS
    l_all_indexes_rec   gt_all_indexes_rec;
  BEGIN
    BEGIN
      SELECT *
        INTO l_all_indexes_rec
        FROM all_indexes i
       WHERE owner = in_owner
         AND index_name = in_index_name;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RETURN;
    END;

    assign_index_rec(
      io_index_rec       => out_index_rec,
      in_all_indexes_rec => l_all_indexes_rec
    );

    SELECT table_type_owner, table_type
      INTO out_index_rec.table_object_owner, out_index_rec.table_object_type
      FROM all_all_tables
     WHERE owner = out_index_rec.table_owner
       AND table_name = out_index_rec.table_name;
    
  END read_index;

  -- read all indexes on to given table
  PROCEDURE read_table_indexes(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    l_index_rec                gt_index_rec;
    l_all_indexes_arr          gt_all_indexes_arr;
    l_index_full_name          VARCHAR2(65);
  BEGIN
    SELECT *
      BULK COLLECT
      INTO l_all_indexes_arr
      FROM all_indexes i
     WHERE table_owner = io_table_rec.owner
       AND table_name = io_table_rec.table_name;

    FOR i IN 1..l_all_indexes_arr.COUNT LOOP
      l_index_full_name := '"'||l_all_indexes_arr(i).owner||'"."'||l_all_indexes_arr(i).index_name||'"';
      io_table_rec.index_indx_arr(l_index_full_name) := i;

      assign_index_rec(
        io_index_rec       => l_index_rec,
        in_all_indexes_rec => l_all_indexes_arr(i)
      );
      
      l_index_rec.table_object_owner := io_table_rec.table_type_owner;
      l_index_rec.table_object_type := io_table_rec.table_type;
      
      io_table_rec.index_arr(i) := l_index_rec;
    END LOOP;
  END read_table_indexes;

  PROCEDURE read_table_join_indexes(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    l_index_rec                gt_index_rec;
    l_all_indexes_arr          gt_all_indexes_arr;
    l_index_full_name          VARCHAR2(65);
  BEGIN
    SELECT i.*
      BULK COLLECT
      INTO l_all_indexes_arr
      FROM all_indexes i
     WHERE EXISTS(SELECT 1 
                    FROM all_join_ind_columns ic
                   WHERE ic.index_owner = i.owner
                     AND ic.index_name = i.index_name 
                     AND (ic.inner_table_owner = io_table_rec.owner
                     AND ic.inner_table_name = io_table_rec.table_name)
                      OR (ic.outer_table_owner = io_table_rec.owner
                     AND ic.outer_table_name = io_table_rec.table_name));
    
    FOR i IN 1..l_all_indexes_arr.COUNT LOOP
      l_index_full_name := '"'||l_all_indexes_arr(i).owner||'"."'||l_all_indexes_arr(i).index_name||'"';
      io_table_rec.join_index_indx_arr(l_index_full_name) := i;

      assign_index_rec(
        io_index_rec       => l_index_rec,
        in_all_indexes_rec => l_all_indexes_arr(i)
      );

      l_index_rec.table_object_owner := io_table_rec.table_type_owner;
      l_index_rec.table_object_type := io_table_rec.table_type;

      io_table_rec.join_index_arr(i) := l_index_rec;
    END LOOP;
  END read_table_join_indexes;

  PROCEDURE assign_constraint_rec(
    io_constraint_rec      IN OUT NOCOPY gt_constraint_rec,
    in_all_constraints_rec IN gt_all_constraints_rec
  )
  AS
  BEGIN
    io_constraint_rec.owner             := in_all_constraints_rec.owner;
    io_constraint_rec.constraint_name   := in_all_constraints_rec.constraint_name;
    io_constraint_rec.constraint_type   := in_all_constraints_rec.constraint_type;
    io_constraint_rec.table_name        := in_all_constraints_rec.table_name;
    io_constraint_rec.search_condition  := in_all_constraints_rec.search_condition;
    io_constraint_rec.r_owner           := in_all_constraints_rec.r_owner;
    io_constraint_rec.r_constraint_name := in_all_constraints_rec.r_constraint_name;
    io_constraint_rec.delete_rule       := in_all_constraints_rec.delete_rule;
    io_constraint_rec.status            := in_all_constraints_rec.status;
    io_constraint_rec.deferrable        := in_all_constraints_rec.deferrable;
    io_constraint_rec.deferred          := in_all_constraints_rec.deferred;
    io_constraint_rec.validated         := in_all_constraints_rec.validated;
    io_constraint_rec.generated         := in_all_constraints_rec.generated;
    io_constraint_rec.rely              := in_all_constraints_rec.rely;
    io_constraint_rec.index_owner       := in_all_constraints_rec.index_owner;  -- Oracle bug in all_constraints
    io_constraint_rec.index_name        := in_all_constraints_rec.index_name;
    io_constraint_rec.has_references    := FALSE;
    io_constraint_rec.drop_flag         := FALSE;
  END assign_constraint_rec;

  PROCEDURE update_constraint_index_owner(
    in_table_rec           IN gt_table_rec,
    io_constraint_rec      IN OUT NOCOPY gt_constraint_rec
  )
  AS
  BEGIN
    FOR i IN 1..in_table_rec.index_arr.COUNT LOOP
      IF in_table_rec.index_arr(i).index_name = io_constraint_rec.index_name AND
         cort_comp_pkg.comp_array(in_table_rec.index_arr(i).column_arr, io_constraint_rec.column_arr) = 0
      THEN
        io_constraint_rec.index_owner := in_table_rec.index_arr(i).owner;
        EXIT; 
      END IF;    
    END LOOP;    
  END update_constraint_index_owner;

  -- read constraint details from cort_all_constraints
  PROCEDURE read_table_constraints(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    l_constraint_rec           gt_constraint_rec;
    l_constraints_arr          gt_all_constraints_arr;
    l_col_cons_name_arr        arrays.gt_str_arr;
    l_col_name_arr             arrays.gt_str_arr;
    l_col_position_arr         arrays.gt_num_arr;
    l_indx                     PLS_INTEGER;
    l_index_full_name          VARCHAR2(65);
    l_temp_name                VARCHAR2(30);
    l_ddl                      VARCHAR2(1000);
  BEGIN
    SELECT c.*
      BULK COLLECT
      INTO l_constraints_arr
      FROM all_constraints c
     WHERE c.owner = io_table_rec.owner
       AND c.table_name = io_table_rec.table_name
       AND c.constraint_type IN ('P','U','R','C','F')
       AND c.view_related IS NULL;

    FOR i IN 1..l_constraints_arr.COUNT LOOP
      io_table_rec.constraint_indx_arr(l_constraints_arr(i).constraint_name) := i;

      assign_constraint_rec(
        io_constraint_rec      => l_constraint_rec,
        in_all_constraints_rec => l_constraints_arr(i)
      );

      io_table_rec.constraint_arr(i) := l_constraint_rec;

      io_table_rec.constraint_arr(i).rename_rec := get_rename_rec(
                                                     in_table_name_rec => io_table_rec.rename_rec,
                                                     in_object_name    => l_constraint_rec.constraint_name,
                                                     in_object_owner   => l_constraint_rec.owner,
                                                     in_object_type    => 'CONSTRAINT',
                                                     in_index          => i,
                                                     in_generated      => CASE l_constraint_rec.generated WHEN 'USER NAME' THEN 'N' ELSE 'Y' END 
                                                   );
/*
      IF io_table_rec.constraint_arr(i).constraint_type = 'C' THEN
        cort_parse_pkg.normalize_search_condition(io_table_rec.constraint_arr(i));
      END IF;
*/      
      IF io_table_rec.constraint_arr(i).constraint_type = 'R' THEN
        BEGIN
          SELECT r.table_name
            INTO io_table_rec.constraint_arr(i).r_table_name
            FROM all_constraints r
           WHERE r.owner = io_table_rec.constraint_arr(i).r_owner
             AND r.constraint_name = io_table_rec.constraint_arr(i).r_constraint_name;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            RETURN;
        END; 
        -- Read ref table columns
        SELECT column_name
          BULK COLLECT
          INTO io_table_rec.constraint_arr(i).r_column_arr
          FROM all_cons_columns
         WHERE owner = io_table_rec.constraint_arr(i).r_owner
           AND constraint_name = io_table_rec.constraint_arr(i).r_constraint_name
         ORDER BY NVL(position,1);
/*
        -- for self-referencing foreign keys
        IF io_table_rec.constraint_arr(i).r_table_name = io_table_rec.table_name THEN
          FOR j IN 1..io_table_rec.constraint_arr(i).r_column_arr.COUNT LOOP
            io_table_rec.constraint_arr(i).column_id_arr(j) := get_column_indx(io_table_rec, io_table_rec.constraint_arr(i).r_column_arr(j));
          END LOOP;
        END IF;
*/        
      END IF;

    END LOOP;

    -- read constraint columns
    IF l_constraints_arr.COUNT > 0 THEN
      SELECT constraint_name, column_name, NVL(position,1)
        BULK COLLECT
        INTO l_col_cons_name_arr, l_col_name_arr, l_col_position_arr
        FROM all_cons_columns
       WHERE owner = io_table_rec.owner
         AND table_name = io_table_rec.table_name;

      FOR i IN 1..l_col_name_arr.COUNT LOOP
        l_indx := io_table_rec.constraint_indx_arr(l_col_cons_name_arr(i));
        io_table_rec.constraint_arr(l_indx).column_arr(l_col_position_arr(i)) := l_col_name_arr(i);
--        io_table_rec.constraint_arr(l_indx).column_id_arr(l_col_position_arr(i)) := get_column_indx(io_table_rec, l_col_name_arr(i));
      END LOOP;
    END IF;

    FOR i IN 1..io_table_rec.constraint_arr.COUNT LOOP
      IF io_table_rec.constraint_arr(i).constraint_type IN ('P', 'U') THEN

        -- find index owner. A workaround for oracle ALL_CONSTRAINS bug (index owner is NULL)        
        update_constraint_index_owner(
          in_table_rec      => io_table_rec,
          io_constraint_rec => io_table_rec.constraint_arr(i)
        );
        
        l_index_full_name := '"'||io_table_rec.constraint_arr(i).index_owner||'"."'||io_table_rec.constraint_arr(i).index_name||'"';
        IF io_table_rec.index_indx_arr.EXISTS(l_index_full_name) THEN
          l_indx := io_table_rec.index_indx_arr(l_index_full_name);
          io_table_rec.index_arr(l_indx).constraint_name := io_table_rec.constraint_arr(i).constraint_name;
          IF io_table_rec.index_arr(l_indx).rename_rec.object_name = io_table_rec.constraint_arr(i).constraint_name AND 
             io_table_rec.constraint_arr(i).constraint_name <> io_table_rec.constraint_arr(i).rename_rec.object_name
          THEN
            io_table_rec.index_arr(l_indx).rename_rec.object_name := io_table_rec.constraint_arr(i).rename_rec.object_name;
          END IF;                           
          
        END IF;
      END IF;

    END LOOP;

  END read_table_constraints;


  PROCEDURE read_table_references(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    l_all_constraints_arr      gt_all_constraints_arr;
    l_indx                     PLS_INTEGER;
    l_temp_name                VARCHAR2(30);
  BEGIN
    FOR j IN 1..io_table_rec.constraint_arr.COUNT LOOP
      IF io_table_rec.constraint_arr(j).constraint_type IN ('P','U') THEN
        SELECT r.*
          BULK COLLECT
          INTO l_all_constraints_arr
          FROM all_constraints r
         WHERE r.r_owner = io_table_rec.constraint_arr(j).owner
           AND r.r_constraint_name = io_table_rec.constraint_arr(j).constraint_name
           AND r.constraint_type = 'R'
           AND '"'||r.owner||'"."'||r.table_name||'"' <> '"'||io_table_rec.owner||'"."'||io_table_rec.table_name||'"'
           -- self referenced FK
        ;

        FOR i IN 1..l_all_constraints_arr.COUNT LOOP
          io_table_rec.constraint_arr(j).has_references := TRUE;

          io_table_rec.ref_constraint_indx_arr(l_all_constraints_arr(i).constraint_name) := i;

          io_table_rec.ref_constraint_arr(i).r_table_name := io_table_rec.table_name;
          io_table_rec.ref_constraint_arr(i).r_column_arr := io_table_rec.constraint_arr(j).column_arr;

          assign_constraint_rec(
            io_constraint_rec      => io_table_rec.ref_constraint_arr(i),
            in_all_constraints_rec => l_all_constraints_arr(i)
          );

          SELECT column_name
            BULK COLLECT
            INTO io_table_rec.ref_constraint_arr(i).column_arr
            FROM all_cons_columns
           WHERE owner = l_all_constraints_arr(i).owner
             AND table_name = l_all_constraints_arr(i).table_name
             AND constraint_name = l_all_constraints_arr(i).constraint_name
           ORDER BY position;

          io_table_rec.ref_constraint_arr(i).rename_rec := get_rename_rec(
                                                             in_table_name_rec => io_table_rec.rename_rec,
                                                             in_object_name    => l_all_constraints_arr(i).constraint_name,
                                                             in_object_owner   => l_all_constraints_arr(i).owner,
                                                             in_object_type    => 'REFERENCE',
                                                             in_index          => i,
                                                             in_generated      => CASE l_all_constraints_arr(i).generated WHEN 'USER NAME' THEN 'N' ELSE 'Y' END 
                                                           );
        END LOOP;
      END IF;
    END LOOP;
  END read_table_references;


  -- read log groups details from all_log_groups
  PROCEDURE read_table_log_groups(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE gt_all_log_groups_arr IS TABLE OF all_log_groups%ROWTYPE INDEX BY PLS_INTEGER;
    l_all_log_groups_arr       gt_all_log_groups_arr;
    l_col_log_name_arr         arrays.gt_str_arr;
    l_col_name_arr             arrays.gt_lstr_arr;
    l_col_log_arr              arrays.gt_str_arr;
    l_col_position_arr         arrays.gt_num_arr;
    l_indx                     PLS_INTEGER;
    l_temp_name                VARCHAR2(30);
  BEGIN
    SELECT *
      BULK COLLECT
      INTO l_all_log_groups_arr
      FROM all_log_groups
     WHERE owner = io_table_rec.owner
       AND table_name = io_table_rec.table_name;

    FOR i IN 1..l_all_log_groups_arr.COUNT LOOP
      io_table_rec.log_group_indx_arr(l_all_log_groups_arr(i).log_group_name) := i;
      io_table_rec.log_group_arr(i).owner             := l_all_log_groups_arr(i).owner;
      io_table_rec.log_group_arr(i).log_group_name    := l_all_log_groups_arr(i).log_group_name;
      io_table_rec.log_group_arr(i).log_group_type    := l_all_log_groups_arr(i).log_group_type;
      io_table_rec.log_group_arr(i).table_name        := l_all_log_groups_arr(i).table_name;
      io_table_rec.log_group_arr(i).always            := l_all_log_groups_arr(i).always;
      io_table_rec.log_group_arr(i).generated         := l_all_log_groups_arr(i).generated;
      io_table_rec.log_group_arr(i).drop_flag         := FALSE;

      io_table_rec.log_group_arr(i).rename_rec := get_rename_rec(
                                                    in_table_name_rec => io_table_rec.rename_rec,
                                                    in_object_name    => l_all_log_groups_arr(i).log_group_name,
                                                    in_object_owner   => l_all_log_groups_arr(i).owner,
                                                    in_object_type    => 'LOG GROUP',
                                                    in_index          => i,
                                                    in_generated      => CASE l_all_log_groups_arr(i).generated WHEN 'USER NAME' THEN 'N' ELSE 'Y' END 
                                                  );
    END LOOP;

    IF l_all_log_groups_arr.COUNT > 0 THEN
      SELECT log_group_name, column_name, logging_property, NVL(position,1)
        BULK COLLECT
        INTO l_col_log_name_arr, l_col_name_arr, l_col_log_arr, l_col_position_arr
        FROM all_log_group_columns
       WHERE owner = io_table_rec.owner
         AND table_name = io_table_rec.table_name;

      FOR i IN 1..l_col_name_arr.COUNT LOOP
        l_indx := io_table_rec.log_group_indx_arr(l_col_log_name_arr(i));
        io_table_rec.log_group_arr(l_indx).column_arr(l_col_position_arr(i)) := l_col_name_arr(i);
        io_table_rec.log_group_arr(l_indx).column_log_arr(l_col_position_arr(i)) := l_col_log_arr(i);
      END LOOP;
    END IF;

  END read_table_log_groups;

  -- reads table lobs
  PROCEDURE read_table_lobs(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE t_all_lobs     IS TABLE OF all_lobs%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE t_part_lob_arr IS TABLE OF all_part_lobs%ROWTYPE INDEX BY PLS_INTEGER;
    l_lob_arr           t_all_lobs;
    l_part_lob_arr      t_part_lob_arr;
    l_column_indx       PLS_INTEGER;
  BEGIN
    IF io_table_rec.partitioned = 'YES' THEN
      SELECT *
        BULK COLLECT
        INTO l_part_lob_arr
        FROM all_part_lobs
       WHERE table_owner = io_table_rec.owner
         AND table_name = io_table_rec.table_name
         AND INSTR(column_name, '"') = 0; -- ignore LOB columns of object data types

      FOR i IN 1..l_part_lob_arr.COUNT LOOP
        l_column_indx := get_column_indx(io_table_rec, l_part_lob_arr(i).column_name);
        io_table_rec.lob_indx_arr(l_part_lob_arr(i).column_name) := l_column_indx;
        io_table_rec.lob_arr(l_column_indx).column_indx     := l_column_indx;
        io_table_rec.lob_arr(l_column_indx).owner           := l_part_lob_arr(i).table_owner;
        io_table_rec.lob_arr(l_column_indx).table_name      := l_part_lob_arr(i).table_name;
        io_table_rec.lob_arr(l_column_indx).column_name     := l_part_lob_arr(i).column_name;
        io_table_rec.lob_arr(l_column_indx).lob_name        := l_part_lob_arr(i).lob_name;
        io_table_rec.lob_arr(l_column_indx).lob_index_name  := l_part_lob_arr(i).lob_index_name;
        io_table_rec.lob_arr(l_column_indx).tablespace_name := l_part_lob_arr(i).def_tablespace_name;
        -- Oracle bug workaround
        IF l_part_lob_arr(i).def_tablespace_name IS NOT NULL AND 
           io_table_rec.tablespace_block_size_indx.EXISTS(l_part_lob_arr(i).def_tablespace_name) 
        THEN
          io_table_rec.lob_arr(l_column_indx).chunk         := l_part_lob_arr(i).def_chunk * io_table_rec.tablespace_block_size_indx(l_part_lob_arr(i).def_tablespace_name);
        ELSE
          io_table_rec.lob_arr(l_column_indx).chunk         := l_part_lob_arr(i).def_chunk;  
        END IF;
        io_table_rec.lob_arr(l_column_indx).pctversion      := l_part_lob_arr(i).def_pctversion;
        io_table_rec.lob_arr(l_column_indx).retention       := NVL(l_part_lob_arr(i).def_retention,'NONE');
        io_table_rec.lob_arr(l_column_indx).cache           := l_part_lob_arr(i).def_cache;
        io_table_rec.lob_arr(l_column_indx).logging         := l_part_lob_arr(i).def_logging;
        io_table_rec.lob_arr(l_column_indx).in_row          := l_part_lob_arr(i).def_in_row;
        io_table_rec.lob_arr(l_column_indx).minretention    := NULLIF(NULLIF(l_part_lob_arr(i).def_minret,'DEFAULT'),0);

        io_table_rec.lob_arr(l_column_indx).storage.initial_extent  := NULLIF(l_part_lob_arr(i).def_initial_extent,'DEFAULT');
--        io_table_rec.lob_arr(l_column_indx).storage.next_extent     := l_part_lob_arr(i).def_next_extent;
        io_table_rec.lob_arr(l_column_indx).storage.min_extents     := NULLIF(l_part_lob_arr(i).def_min_extents,'DEFAULT');
--        io_table_rec.lob_arr(l_column_indx).storage.max_extents     := l_part_lob_arr(i).def_max_extents;
        io_table_rec.lob_arr(l_column_indx).storage.max_size        := NULLIF(l_part_lob_arr(i).def_max_size,'DEFAULT');
        io_table_rec.lob_arr(l_column_indx).storage.pct_increase    := NULLIF(l_part_lob_arr(i).def_pct_increase,'DEFAULT');
        io_table_rec.lob_arr(l_column_indx).storage.freelists       := l_part_lob_arr(i).def_freelists;
        io_table_rec.lob_arr(l_column_indx).storage.freelist_groups := l_part_lob_arr(i).def_freelist_groups;
        io_table_rec.lob_arr(l_column_indx).storage.buffer_pool     := NULLIF(l_part_lob_arr(i).def_buffer_pool,'DEFAULT');


        $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
        io_table_rec.lob_arr(l_column_indx).encrypt         := l_part_lob_arr(i).def_encrypt;
        io_table_rec.lob_arr(l_column_indx).compression     := l_part_lob_arr(i).def_compress;
        io_table_rec.lob_arr(l_column_indx).deduplication   := l_part_lob_arr(i).def_deduplicate;
        io_table_rec.lob_arr(l_column_indx).securefile      := l_part_lob_arr(i).def_securefile;
        $END
        $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=2 $THEN
        io_table_rec.lob_arr(l_column_indx).flash_cache      := l_part_lob_arr(i).def_flash_cache;
        io_table_rec.lob_arr(l_column_indx).cell_flash_cache := l_part_lob_arr(i).def_cell_flash_cache;
        $END

        io_table_rec.lob_arr(l_column_indx).rename_rec := get_rename_rec(
                                                            in_object_name  => io_table_rec.lob_arr(l_column_indx).lob_name,
                                                            in_object_owner => io_table_rec.lob_arr(l_column_indx).owner, 
                                                            in_object_type  => 'LOB'
                                                          );
      END LOOP;

    ELSE
      SELECT *
        BULK COLLECT
        INTO l_lob_arr
        FROM all_lobs
       WHERE owner = io_table_rec.owner
         AND table_name = io_table_rec.table_name
         AND INSTR(column_name, '"') = 0; -- ignore LOB columns of object data types

      FOR i IN 1..l_lob_arr.COUNT LOOP
        l_column_indx := get_column_indx(io_table_rec, l_lob_arr(i).column_name);
        io_table_rec.lob_arr(l_column_indx).column_indx     := l_column_indx;
        io_table_rec.lob_indx_arr(l_lob_arr(i).column_name) := l_column_indx;
        io_table_rec.lob_arr(l_column_indx).owner           := l_lob_arr(i).owner;
        io_table_rec.lob_arr(l_column_indx).table_name      := l_lob_arr(i).table_name;
        io_table_rec.lob_arr(l_column_indx).column_name     := l_lob_arr(i).column_name;
        io_table_rec.lob_arr(l_column_indx).lob_name        := l_lob_arr(i).segment_name;
        io_table_rec.lob_arr(l_column_indx).lob_index_name  := l_lob_arr(i).index_name;
        io_table_rec.lob_arr(l_column_indx).tablespace_name := l_lob_arr(i).tablespace_name;
        io_table_rec.lob_arr(l_column_indx).chunk           := l_lob_arr(i).chunk;
        io_table_rec.lob_arr(l_column_indx).pctversion      := l_lob_arr(i).pctversion;
        io_table_rec.lob_arr(l_column_indx).freepools       := l_lob_arr(i).freepools;
        io_table_rec.lob_arr(l_column_indx).cache           := l_lob_arr(i).cache;
        io_table_rec.lob_arr(l_column_indx).logging         := l_lob_arr(i).logging;
        io_table_rec.lob_arr(l_column_indx).in_row          := l_lob_arr(i).in_row;
        io_table_rec.lob_arr(l_column_indx).partitioned     := l_lob_arr(i).partitioned;
        $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
        io_table_rec.lob_arr(l_column_indx).encrypt         := l_lob_arr(i).encrypt;
        io_table_rec.lob_arr(l_column_indx).compression     := l_lob_arr(i).compression;
        io_table_rec.lob_arr(l_column_indx).deduplication   := l_lob_arr(i).deduplication;
        io_table_rec.lob_arr(l_column_indx).securefile      := l_lob_arr(i).securefile;
        $END
        
        IF l_lob_arr(i).owner = user THEN
          BEGIN
            SELECT NVL(retention,'NONE'), NULLIF(minretention,0) 
              INTO io_table_rec.lob_arr(l_column_indx).retention, 
                   io_table_rec.lob_arr(l_column_indx).minretention   
              FROM user_segments
             WHERE segment_name = l_lob_arr(i).segment_name
               AND segment_type = 'LOBSEGMENT';
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              io_table_rec.lob_arr(l_column_indx).retention := 'NONE'; 
              io_table_rec.lob_arr(l_column_indx).minretention := 0;
          END;     
        ELSE
          -- there is no ALL_SEGMENTS
          cort_aux_pkg.read_seg_retention(
            in_segment_name   => l_lob_arr(i).segment_name,
            in_segment_owner  => l_lob_arr(i).owner,
            in_segment_type   => 'LOBSEGMENT',
            out_retention     => io_table_rec.lob_arr(l_column_indx).retention,
            out_minretention  => io_table_rec.lob_arr(l_column_indx).minretention
          );
        END IF;
        
        io_table_rec.lob_arr(l_column_indx).rename_rec := get_rename_rec(
                                                            in_object_name  => io_table_rec.lob_arr(l_column_indx).lob_name,
                                                            in_object_owner => io_table_rec.lob_arr(l_column_indx).owner, 
                                                            in_object_type  => 'LOB'
                                                          );
      END LOOP;
    END IF;


  END read_table_lobs;

  -- read xml columns
  PROCEDURE read_table_xml_cols(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE t_all_xml_cols IS TABLE OF all_xml_tab_cols%ROWTYPE INDEX BY PLS_INTEGER;
    l_all_xml_cols      t_all_xml_cols;
    l_column_indx       PLS_INTEGER;
    l_lob_column_indx   PLS_INTEGER;
    l_column_id         PLS_INTEGER;
  BEGIN
    SELECT *
      BULK COLLECT
      INTO l_all_xml_cols
      FROM all_xml_tab_cols
     WHERE owner = io_table_rec.owner
       AND table_name = io_table_rec.table_name;

    FOR i IN 1..l_all_xml_cols.COUNT LOOP
      l_column_indx := get_column_indx(io_table_rec, l_all_xml_cols(i).column_name);
      l_column_id := io_table_rec.column_arr(l_column_indx).column_id;
      io_table_rec.xml_col_arr(l_column_indx).column_indx    := l_column_indx;
      io_table_rec.xml_col_arr(l_column_indx).owner          := l_all_xml_cols(i).owner;
      io_table_rec.xml_col_arr(l_column_indx).table_name     := l_all_xml_cols(i).table_name;
      io_table_rec.xml_col_arr(l_column_indx).column_name    := l_all_xml_cols(i).column_name;
      io_table_rec.xml_col_arr(l_column_indx).xmlschema      := l_all_xml_cols(i).xmlschema;
      io_table_rec.xml_col_arr(l_column_indx).schema_owner   := l_all_xml_cols(i).schema_owner;
      io_table_rec.xml_col_arr(l_column_indx).element_name   := l_all_xml_cols(i).element_name;
      io_table_rec.xml_col_arr(l_column_indx).storage_type   := l_all_xml_cols(i).storage_type;
      io_table_rec.xml_col_arr(l_column_indx).anyschema      := l_all_xml_cols(i).anyschema;
      io_table_rec.xml_col_arr(l_column_indx).nonschema      := l_all_xml_cols(i).nonschema;
      CASE io_table_rec.xml_col_arr(l_column_indx).storage_type
      WHEN 'BINARY' THEN
        -- next column after XML should be hidden BLOB with the same column_id
        l_lob_column_indx := l_column_indx + 1;
        IF io_table_rec.column_arr.EXISTS(l_lob_column_indx) AND
           io_table_rec.column_arr(l_lob_column_indx).column_id = l_column_id AND
           io_table_rec.column_arr(l_lob_column_indx).data_type = 'BLOB' AND
           io_table_rec.column_arr(l_lob_column_indx).hidden_column = 'YES'
        THEN
          io_table_rec.xml_col_arr(l_column_indx).lob_column_indx := l_lob_column_indx;
          io_table_rec.lob_arr(l_lob_column_indx).xml_column_indx := l_column_indx;
        END IF;
      WHEN 'CLOB' THEN
        -- next column after XML should be hidden CLOB with the same column_id
        l_lob_column_indx := l_column_indx + 1;
        IF io_table_rec.column_arr.EXISTS(l_lob_column_indx) AND
           io_table_rec.column_arr(l_lob_column_indx).column_id = l_column_id AND
           io_table_rec.column_arr(l_lob_column_indx).data_type = 'CLOB' AND
           io_table_rec.column_arr(l_lob_column_indx).hidden_column = 'YES'
        THEN
          io_table_rec.xml_col_arr(l_column_indx).lob_column_indx := l_lob_column_indx;
          io_table_rec.lob_arr(l_lob_column_indx).xml_column_indx := l_column_indx;
        END IF;
      ELSE  
        NULL;  
      END CASE;
    END LOOP;

  END read_table_xml_cols;

  -- read varray columns
  PROCEDURE read_table_varrays(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE t_all_varrays IS TABLE OF all_varrays%ROWTYPE INDEX BY PLS_INTEGER;
    l_all_varrays       t_all_varrays;
    l_column_indx       PLS_INTEGER;
  BEGIN
    SELECT *
      BULK COLLECT
      INTO l_all_varrays
      FROM all_varrays
     WHERE owner = io_table_rec.owner
       AND parent_table_name = io_table_rec.table_name;

    FOR i IN 1..l_all_varrays.COUNT LOOP
      l_column_indx := get_column_indx(io_table_rec, l_all_varrays(i).parent_table_column);
      io_table_rec.varray_arr(l_column_indx).owner                 := l_all_varrays(i).owner;
      io_table_rec.varray_arr(l_column_indx).table_name            := l_all_varrays(i).parent_table_name;
      io_table_rec.varray_arr(l_column_indx).column_name           := l_all_varrays(i).parent_table_column;
      io_table_rec.varray_arr(l_column_indx).column_indx           := l_column_indx;
      io_table_rec.varray_arr(l_column_indx).type_owner            := l_all_varrays(i).type_owner;
      io_table_rec.varray_arr(l_column_indx).type_name             := l_all_varrays(i).type_name;
      io_table_rec.varray_arr(l_column_indx).lob_name              := l_all_varrays(i).lob_name;
      io_table_rec.varray_arr(l_column_indx).storage_spec          := TRIM(l_all_varrays(i).storage_spec);
      io_table_rec.varray_arr(l_column_indx).return_type           := TRIM(l_all_varrays(i).return_type);
      io_table_rec.varray_arr(l_column_indx).element_substitutable := TRIM(l_all_varrays(i).element_substitutable);
      IF io_table_rec.lob_arr.EXISTS(l_column_indx) THEN
        io_table_rec.lob_arr(l_column_indx).varray_column_indx := l_column_indx; 
        io_table_rec.varray_arr(l_column_indx).lob_column_indx := l_column_indx;
      END IF;
    END LOOP;

  END read_table_varrays;

  -- reads table partitions
  PROCEDURE read_partitions(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE gt_tab_partitions_arr IS TABLE OF all_tab_partitions%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE gt_lob_partitions_arr IS TABLE OF all_lob_partitions%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE gt_ind_partitions_arr IS TABLE OF all_ind_partitions%ROWTYPE INDEX BY PLS_INTEGER;
    l_tab_partitions_arr       gt_tab_partitions_arr;
    l_ind_partitions_arr       gt_ind_partitions_arr;
    l_lob_partitions_arr       gt_lob_partitions_arr;
    l_indx                     PLS_INTEGER;
    l_lob_indx                 PLS_INTEGER;
    l_lob_rec                  gt_lob_rec;
    l_block_size               NUMBER;
  BEGIN
    IF io_table_rec.partitioned = 'YES' THEN
      SELECT *
        BULK COLLECT
        INTO l_tab_partitions_arr
        FROM all_tab_partitions
       WHERE table_owner = io_table_rec.owner
         AND table_name = io_table_rec.table_name
       ORDER BY partition_position;

      SELECT *
        BULK COLLECT
        INTO l_lob_partitions_arr
        FROM all_lob_partitions
       WHERE table_owner = io_table_rec.owner
         AND table_name = io_table_rec.table_name
       ORDER BY partition_position, column_name;
    END IF;

    FOR i IN 1..l_tab_partitions_arr.COUNT LOOP
      io_table_rec.partition_indx_arr(l_tab_partitions_arr(i).partition_name) := i;
      io_table_rec.partition_arr(i).indx               := i;
      io_table_rec.partition_arr(i).table_owner        := l_tab_partitions_arr(i).table_owner;
      io_table_rec.partition_arr(i).table_name         := l_tab_partitions_arr(i).table_name;
      io_table_rec.partition_arr(i).partition_level    := 'PARTITION';
      io_table_rec.partition_arr(i).partition_type     := io_table_rec.partitioning_type;
      io_table_rec.partition_arr(i).partition_name     := l_tab_partitions_arr(i).partition_name;
      io_table_rec.partition_arr(i).composite          := l_tab_partitions_arr(i).composite;
      io_table_rec.partition_arr(i).high_value         := l_tab_partitions_arr(i).high_value;
      io_table_rec.partition_arr(i).position           := l_tab_partitions_arr(i).partition_position;
      io_table_rec.partition_arr(i).physical_attr_rec.pct_free := l_tab_partitions_arr(i).pct_free;
      io_table_rec.partition_arr(i).physical_attr_rec.pct_used := l_tab_partitions_arr(i).pct_used;
      io_table_rec.partition_arr(i).physical_attr_rec.ini_trans := l_tab_partitions_arr(i).ini_trans;
      io_table_rec.partition_arr(i).physical_attr_rec.max_trans := l_tab_partitions_arr(i).max_trans;
      io_table_rec.partition_arr(i).physical_attr_rec.storage.initial_extent := l_tab_partitions_arr(i).initial_extent;
--      io_table_rec.partition_arr(i).physical_attr_rec.storage.next_extent := l_tab_partitions_arr(i).next_extent;
      io_table_rec.partition_arr(i).physical_attr_rec.storage.min_extents := l_tab_partitions_arr(i).min_extent;
--      io_table_rec.partition_arr(i).physical_attr_rec.storage.max_extents := l_tab_partitions_arr(i).max_extent;
      io_table_rec.partition_arr(i).physical_attr_rec.storage.pct_increase := l_tab_partitions_arr(i).pct_increase;
      io_table_rec.partition_arr(i).physical_attr_rec.storage.freelists := l_tab_partitions_arr(i).freelists;
      io_table_rec.partition_arr(i).physical_attr_rec.storage.freelist_groups := l_tab_partitions_arr(i).freelist_groups;
      io_table_rec.partition_arr(i).physical_attr_rec.storage.buffer_pool := NULLIF(l_tab_partitions_arr(i).buffer_pool,'DEFAULT');
      io_table_rec.partition_arr(i).compression_rec.compression := l_tab_partitions_arr(i).compression;
      $IF dbms_db_version.version >= 11 AND dbms_db_version.release >= 1 $THEN
      io_table_rec.partition_arr(i).compression_rec.compress_for := l_tab_partitions_arr(i).compress_for;
      $END
      io_table_rec.partition_arr(i).logging := l_tab_partitions_arr(i).logging;
      io_table_rec.partition_arr(i).tablespace_name := l_tab_partitions_arr(i).tablespace_name;
    END LOOP;

    FOR i IN 1..l_lob_partitions_arr.COUNT LOOP
      l_lob_rec.owner              := l_lob_partitions_arr(i).table_owner;
      l_lob_rec.table_name         := l_lob_partitions_arr(i).table_name;
      l_lob_rec.column_name        := l_lob_partitions_arr(i).column_name;
      l_lob_rec.lob_name           := l_lob_partitions_arr(i).lob_name;
      l_lob_rec.partition_name     := l_lob_partitions_arr(i).partition_name;
      l_lob_rec.partition_level    := 'PARTITION';
      l_lob_rec.lob_partition_name := l_lob_partitions_arr(i).lob_partition_name;
      l_lob_rec.lob_indpart_name   := l_lob_partitions_arr(i).lob_indpart_name;
      l_lob_rec.partition_position := l_lob_partitions_arr(i).partition_position;
      l_lob_rec.tablespace_name    := l_lob_partitions_arr(i).tablespace_name;
      -- Oracle bug workaround
      IF l_lob_partitions_arr(i).tablespace_name IS NOT NULL AND 
        io_table_rec.tablespace_block_size_indx.EXISTS(l_lob_partitions_arr(i).tablespace_name) AND 
        l_lob_partitions_arr(i).composite = 'NO'
      THEN  
        l_block_size := io_table_rec.tablespace_block_size_indx(l_lob_partitions_arr(i).tablespace_name);
      ELSE
        l_block_size := 1;
      END IF;   
      l_lob_rec.chunk              := l_lob_partitions_arr(i).chunk * l_block_size;
      --
      l_lob_rec.pctversion         := l_lob_partitions_arr(i).pctversion;
      l_lob_rec.retention          := NVL(l_lob_partitions_arr(i).retention,'NONE');
      l_lob_rec.cache              := l_lob_partitions_arr(i).cache;
      l_lob_rec.logging            := l_lob_partitions_arr(i).logging;
      l_lob_rec.in_row             := l_lob_partitions_arr(i).in_row;
      l_lob_rec.minretention       := NULLIF(NULLIF(l_lob_partitions_arr(i).minretention,'DEFAULT'),'0');

      l_lob_rec.storage.initial_extent  := l_lob_partitions_arr(i).initial_extent;
--      l_lob_rec.storage.next_extent     := l_lob_partitions_arr(i).next_extent;
      l_lob_rec.storage.min_extents     := l_lob_partitions_arr(i).min_extents;
--      l_lob_rec.storage.max_extents     := l_lob_partitions_arr(i).max_extents;
      l_lob_rec.storage.max_size        := l_lob_partitions_arr(i).max_size;
      l_lob_rec.storage.pct_increase    := l_lob_partitions_arr(i).pct_increase;
      l_lob_rec.storage.freelists       := l_lob_partitions_arr(i).freelists;
      l_lob_rec.storage.freelist_groups := l_lob_partitions_arr(i).freelist_groups;
      l_lob_rec.storage.buffer_pool     := NULLIF(l_lob_partitions_arr(i).buffer_pool,'DEFAULT');

      $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
      l_lob_rec.encrypt            := l_lob_partitions_arr(i).encrypt;
      l_lob_rec.compression        := l_lob_partitions_arr(i).compression;
      l_lob_rec.deduplication      := l_lob_partitions_arr(i).deduplication;
      l_lob_rec.securefile         := l_lob_partitions_arr(i).securefile;
      $END
      $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=2 $THEN
      l_lob_rec.flash_cache        := l_lob_partitions_arr(i).flash_cache;
      l_lob_rec.cell_flash_cache   := l_lob_partitions_arr(i).cell_flash_cache;
      $END
      l_lob_rec.column_indx := get_column_indx(io_table_rec, l_lob_rec.column_name);
      l_indx := io_table_rec.partition_indx_arr(l_lob_partitions_arr(i).partition_name);
      l_lob_indx := io_table_rec.partition_arr(l_indx).lob_arr.COUNT+1;
      io_table_rec.partition_arr(l_indx).lob_arr(l_lob_indx) := l_lob_rec;
      io_table_rec.partition_arr(l_indx).lob_indx_arr(l_lob_rec.column_indx) := l_lob_indx;
    END LOOP;

    IF io_table_rec.iot_type IS NOT NULL AND
       io_table_rec.overflow_table_name IS NOT NULL AND
       io_table_rec.partitioned = 'YES'
    THEN
      SELECT *
        BULK COLLECT
        INTO l_ind_partitions_arr
        FROM all_ind_partitions
       WHERE index_owner = io_table_rec.iot_index_owner
         AND index_name = io_table_rec.iot_index_name
       ORDER BY partition_position;

      FOR i IN 1..l_ind_partitions_arr.COUNT LOOP
        io_table_rec.partition_indx_arr(l_ind_partitions_arr(i).partition_name) := i;
        io_table_rec.partition_arr(i).iot_key_compression := l_ind_partitions_arr(i).compression;
        io_table_rec.partition_arr(i).physical_attr_rec.pct_free := l_ind_partitions_arr(i).pct_free;
        io_table_rec.partition_arr(i).physical_attr_rec.ini_trans := l_ind_partitions_arr(i).ini_trans;
        io_table_rec.partition_arr(i).physical_attr_rec.max_trans := l_ind_partitions_arr(i).max_trans;
        io_table_rec.partition_arr(i).physical_attr_rec.storage.initial_extent := l_ind_partitions_arr(i).initial_extent;
        --io_table_rec.partition_arr(i).physical_attr_rec.storage.next_extent := l_ind_partitions_arr(i).next_extent;
        io_table_rec.partition_arr(i).physical_attr_rec.storage.min_extents := l_ind_partitions_arr(i).min_extent;
        --io_table_rec.partition_arr(i).physical_attr_rec.storage.max_extents := l_ind_partitions_arr(i).max_extent;
        io_table_rec.partition_arr(i).physical_attr_rec.storage.pct_increase := l_ind_partitions_arr(i).pct_increase;
        io_table_rec.partition_arr(i).physical_attr_rec.storage.freelists := l_ind_partitions_arr(i).freelists;
        io_table_rec.partition_arr(i).physical_attr_rec.storage.freelist_groups := l_ind_partitions_arr(i).freelist_groups;
        io_table_rec.partition_arr(i).physical_attr_rec.storage.buffer_pool := NULLIF(l_ind_partitions_arr(i).buffer_pool,'DEFAULT');
        io_table_rec.partition_arr(i).logging := l_ind_partitions_arr(i).logging;
        io_table_rec.partition_arr(i).tablespace_name := l_ind_partitions_arr(i).tablespace_name;
      END LOOP;

      SELECT *
        BULK COLLECT
        INTO l_tab_partitions_arr
        FROM all_tab_partitions
       WHERE table_owner = io_table_rec.owner
         AND table_name = io_table_rec.overflow_table_name
       ORDER BY partition_position;

      FOR i IN 1..l_tab_partitions_arr.COUNT LOOP
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.pct_free := l_tab_partitions_arr(i).pct_free;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.pct_used := l_tab_partitions_arr(i).pct_used;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.ini_trans := l_tab_partitions_arr(i).ini_trans;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.max_trans := l_tab_partitions_arr(i).max_trans;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.storage.initial_extent := l_tab_partitions_arr(i).initial_extent;
        --io_table_rec.partition_arr(i).overflow_physical_attr_rec.storage.next_extent := l_tab_partitions_arr(i).next_extent;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.storage.min_extents := l_tab_partitions_arr(i).min_extent;
        --io_table_rec.partition_arr(i).overflow_physical_attr_rec.storage.max_extents := l_tab_partitions_arr(i).max_extent;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.storage.pct_increase := l_tab_partitions_arr(i).pct_increase;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.storage.freelists := l_tab_partitions_arr(i).freelists;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.storage.freelist_groups := l_tab_partitions_arr(i).freelist_groups;
        io_table_rec.partition_arr(i).overflow_physical_attr_rec.storage.buffer_pool := NULLIF(l_tab_partitions_arr(i).buffer_pool,'DEFAULT');
        io_table_rec.partition_arr(i).overflow_logging := l_tab_partitions_arr(i).logging;
        io_table_rec.partition_arr(i).overflow_tablespace := l_tab_partitions_arr(i).tablespace_name;
      END LOOP;
    END IF;
  END read_partitions;

  -- reads table subpartitions
  PROCEDURE read_subpartitions(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE gt_tab_subpartitions_arr IS TABLE OF all_tab_subpartitions%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE gt_lob_subpartitions_arr IS TABLE OF all_lob_subpartitions%ROWTYPE INDEX BY PLS_INTEGER;
    l_tab_subpartitions_arr       gt_tab_subpartitions_arr;
    l_lob_subpartitions_arr       gt_lob_subpartitions_arr;
    l_last_partition_name         VARCHAR2(30);
    l_partition_name              VARCHAR2(30);
    l_partition_indx              PLS_INTEGER;
    l_indx                        PLS_INTEGER;
    l_lob_indx                    PLS_INTEGER;
    l_lob_rec                     gt_lob_rec;
  BEGIN
    IF io_table_rec.subpartitioning_type IS NOT NULL THEN
      SELECT *
        BULK COLLECT
        INTO l_tab_subpartitions_arr
        FROM all_tab_subpartitions
       WHERE table_owner = io_table_rec.owner
         AND table_name = io_table_rec.table_name
       ORDER BY partition_name, subpartition_position;

      SELECT *
        BULK COLLECT
        INTO l_lob_subpartitions_arr
        FROM all_lob_subpartitions
       WHERE table_owner = io_table_rec.owner
         AND table_name = io_table_rec.table_name
       ORDER BY subpartition_position, column_name, lob_partition_name;

    END IF;

    l_last_partition_name := NULL;
    FOR i IN 1..l_tab_subpartitions_arr.COUNT LOOP
      io_table_rec.subpartition_indx_arr(l_tab_subpartitions_arr(i).subpartition_name) := i;
      l_partition_name := l_tab_subpartitions_arr(i).partition_name;
      IF io_table_rec.partition_indx_arr.EXISTS(l_partition_name) THEN
        l_partition_indx := io_table_rec.partition_indx_arr(l_partition_name);
        IF l_last_partition_name = l_partition_name THEN
          io_table_rec.partition_arr(l_partition_indx).subpartition_to_indx := i;
        ELSE
          l_last_partition_name := l_partition_name;
          io_table_rec.partition_arr(l_partition_indx).subpartition_from_indx := i;
          io_table_rec.partition_arr(l_partition_indx).subpartition_to_indx := i;
        END IF;
      END IF;
      io_table_rec.subpartition_arr(i).indx               := i;
      io_table_rec.subpartition_arr(i).table_owner        := l_tab_subpartitions_arr(i).table_owner;
      io_table_rec.subpartition_arr(i).table_name         := l_tab_subpartitions_arr(i).table_name;
      io_table_rec.subpartition_arr(i).partition_level    := 'SUBPARTITION';
      io_table_rec.subpartition_arr(i).partition_type     := io_table_rec.subpartitioning_type;
      io_table_rec.subpartition_arr(i).partition_name     := l_tab_subpartitions_arr(i).subpartition_name;
      io_table_rec.subpartition_arr(i).parent_partition_name := l_tab_subpartitions_arr(i).partition_name;
      io_table_rec.subpartition_arr(i).high_value         := l_tab_subpartitions_arr(i).high_value;
      io_table_rec.subpartition_arr(i).position           := l_tab_subpartitions_arr(i).subpartition_position;
      io_table_rec.subpartition_arr(i).physical_attr_rec.pct_free := l_tab_subpartitions_arr(i).pct_free;
      io_table_rec.subpartition_arr(i).physical_attr_rec.pct_used := l_tab_subpartitions_arr(i).pct_used;
      io_table_rec.subpartition_arr(i).physical_attr_rec.ini_trans := l_tab_subpartitions_arr(i).ini_trans;
      io_table_rec.subpartition_arr(i).physical_attr_rec.max_trans := l_tab_subpartitions_arr(i).max_trans;
      io_table_rec.subpartition_arr(i).physical_attr_rec.storage.initial_extent := l_tab_subpartitions_arr(i).initial_extent;
      --io_table_rec.subpartition_arr(i).physical_attr_rec.storage.next_extent := l_tab_subpartitions_arr(i).next_extent;
      io_table_rec.subpartition_arr(i).physical_attr_rec.storage.min_extents := l_tab_subpartitions_arr(i).min_extent;
      --io_table_rec.subpartition_arr(i).physical_attr_rec.storage.max_extents := l_tab_subpartitions_arr(i).max_extent;
      io_table_rec.subpartition_arr(i).physical_attr_rec.storage.pct_increase := l_tab_subpartitions_arr(i).pct_increase;
      io_table_rec.subpartition_arr(i).physical_attr_rec.storage.freelists := l_tab_subpartitions_arr(i).freelists;
      io_table_rec.subpartition_arr(i).physical_attr_rec.storage.freelist_groups := l_tab_subpartitions_arr(i).freelist_groups;
      io_table_rec.subpartition_arr(i).physical_attr_rec.storage.buffer_pool := NULLIF(l_tab_subpartitions_arr(i).buffer_pool,'DEFAULT');
      io_table_rec.subpartition_arr(i).compression_rec.compression := l_tab_subpartitions_arr(i).compression;
      $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
      io_table_rec.subpartition_arr(i).compression_rec.compress_for := l_tab_subpartitions_arr(i).compress_for;
      $END
      io_table_rec.subpartition_arr(i).logging := l_tab_subpartitions_arr(i).logging;
      io_table_rec.subpartition_arr(i).tablespace_name := l_tab_subpartitions_arr(i).tablespace_name;
    END LOOP;

    FOR i IN 1..l_lob_subpartitions_arr.COUNT LOOP
      l_lob_rec.owner                := l_lob_subpartitions_arr(i).table_owner;
      l_lob_rec.table_name           := l_lob_subpartitions_arr(i).table_name;
      l_lob_rec.column_name          := l_lob_subpartitions_arr(i).column_name;
      l_lob_rec.lob_name             := l_lob_subpartitions_arr(i).lob_name;
      l_lob_rec.parent_lob_part_name := l_lob_subpartitions_arr(i).lob_partition_name;
      l_lob_rec.partition_name       := l_lob_subpartitions_arr(i).subpartition_name;
      l_lob_rec.partition_level      := 'SUBPARTITION';
      l_lob_rec.lob_partition_name   := l_lob_subpartitions_arr(i).lob_subpartition_name;
      l_lob_rec.lob_indpart_name     := l_lob_subpartitions_arr(i).lob_indsubpart_name;
      l_lob_rec.partition_position   := l_lob_subpartitions_arr(i).subpartition_position;
      l_lob_rec.tablespace_name      := l_lob_subpartitions_arr(i).tablespace_name;
      l_lob_rec.chunk                := l_lob_subpartitions_arr(i).chunk;
      l_lob_rec.pctversion           := l_lob_subpartitions_arr(i).pctversion;
      l_lob_rec.retention            := NVL(l_lob_subpartitions_arr(i).retention,'NONE');
      l_lob_rec.cache                := l_lob_subpartitions_arr(i).cache;
      l_lob_rec.logging              := l_lob_subpartitions_arr(i).logging;
      l_lob_rec.in_row               := l_lob_subpartitions_arr(i).in_row;
      l_lob_rec.minretention         := NULLIF(l_lob_subpartitions_arr(i).minretention,'0');

      l_lob_rec.storage.initial_extent  := l_lob_subpartitions_arr(i).initial_extent;
      --l_lob_rec.storage.next_extent     := l_lob_subpartitions_arr(i).next_extent;
      l_lob_rec.storage.min_extents     := l_lob_subpartitions_arr(i).min_extents;
      --l_lob_rec.storage.max_extents     := l_lob_subpartitions_arr(i).max_extents;
      l_lob_rec.storage.max_size        := l_lob_subpartitions_arr(i).max_size;
      l_lob_rec.storage.pct_increase    := l_lob_subpartitions_arr(i).pct_increase;
      l_lob_rec.storage.freelists       := l_lob_subpartitions_arr(i).freelists;
      l_lob_rec.storage.freelist_groups := l_lob_subpartitions_arr(i).freelist_groups;
      l_lob_rec.storage.buffer_pool     := NULLIF(l_lob_subpartitions_arr(i).buffer_pool,'DEFAULT');

      $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
      l_lob_rec.encrypt              := l_lob_subpartitions_arr(i).encrypt;
      l_lob_rec.compression          := l_lob_subpartitions_arr(i).compression;
      l_lob_rec.deduplication        := l_lob_subpartitions_arr(i).deduplication;
      l_lob_rec.securefile           := l_lob_subpartitions_arr(i).securefile;
      $END
      $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=2 $THEN
      l_lob_rec.flash_cache          := l_lob_subpartitions_arr(i).flash_cache;
      l_lob_rec.cell_flash_cache     := l_lob_subpartitions_arr(i).cell_flash_cache;
      $END
      l_lob_rec.column_indx := get_column_indx(io_table_rec, l_lob_rec.column_name);
      l_indx := io_table_rec.subpartition_indx_arr(l_lob_subpartitions_arr(i).subpartition_name);
      l_lob_indx := io_table_rec.subpartition_arr(l_indx).lob_arr.COUNT+1;
      io_table_rec.subpartition_arr(l_indx).lob_arr(l_lob_indx) := l_lob_rec;
      io_table_rec.subpartition_arr(l_indx).lob_indx_arr(l_lob_rec.column_indx) := l_lob_indx;
    END LOOP;

  END read_subpartitions;

  -- read table privileges
  PROCEDURE read_privileges(
    in_owner         IN VARCHAR2,
    in_table_name    IN VARCHAR2,
    io_privilege_arr IN OUT NOCOPY gt_privilege_arr
  )
  AS
    TYPE t_all_tab_privs_arr IS TABLE OF all_tab_privs%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE t_all_col_privs_arr IS TABLE OF all_col_privs%ROWTYPE INDEX BY PLS_INTEGER;
    l_all_tab_privs_arr      t_all_tab_privs_arr;
    l_all_col_privs_arr      t_all_col_privs_arr;
    l_indx                   PLS_INTEGER;
  BEGIN
    io_privilege_arr.DELETE;
    SELECT *
      BULK COLLECT
      INTO l_all_tab_privs_arr
      FROM all_tab_privs
     WHERE table_name = in_table_name
       AND table_schema = in_owner
       AND grantor = user;

    FOR i IN 1..l_all_tab_privs_arr.COUNT LOOP
      io_privilege_arr(i).grantor      := l_all_tab_privs_arr(i).grantor;
      io_privilege_arr(i).grantee      := l_all_tab_privs_arr(i).grantee;
      io_privilege_arr(i).table_schema := l_all_tab_privs_arr(i).table_schema;
      io_privilege_arr(i).table_name   := l_all_tab_privs_arr(i).table_name;
      io_privilege_arr(i).privilege    := l_all_tab_privs_arr(i).privilege;
      io_privilege_arr(i).grantable    := l_all_tab_privs_arr(i).grantable;
      io_privilege_arr(i).hierarchy    := l_all_tab_privs_arr(i).hierarchy;
    END LOOP;

    SELECT *
      BULK COLLECT
      INTO l_all_col_privs_arr
      FROM all_col_privs
     WHERE table_name = in_table_name
       AND table_schema = in_owner;

    FOR i IN 1..l_all_col_privs_arr.COUNT LOOP
      l_indx := io_privilege_arr.COUNT + i;
      io_privilege_arr(l_indx).grantor      := l_all_col_privs_arr(i).grantor;
      io_privilege_arr(l_indx).grantee      := l_all_col_privs_arr(i).grantee;
      io_privilege_arr(l_indx).table_schema := l_all_col_privs_arr(i).table_schema;
      io_privilege_arr(l_indx).table_name   := l_all_col_privs_arr(i).table_name;
      io_privilege_arr(l_indx).column_name  := l_all_col_privs_arr(i).column_name;
      io_privilege_arr(l_indx).privilege    := l_all_col_privs_arr(i).privilege;
      io_privilege_arr(l_indx).grantable    := l_all_col_privs_arr(i).grantable;
    END LOOP;
  END read_privileges;

  PROCEDURE read_table_triggers(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE t_all_triggers_arr         IS TABLE OF all_triggers%ROWTYPE INDEX BY PLS_INTEGER;
    l_all_triggers_arr              t_all_triggers_arr;
    l_full_name                     VARCHAR2(65);
    l_indx                          PLS_INTEGER;
    l_owner_arr                     arrays.gt_str_arr;
    l_trigger_name_arr              arrays.gt_str_arr;
    l_referenced_trigger_owner_arr  arrays.gt_str_arr;
    l_referenced_trigger_name_arr   arrays.gt_str_arr;
    l_ref_indx_arr                  arrays.gt_int_indx;
  BEGIN
    $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
    SELECT t.*
      BULK COLLECT
      INTO l_owner_arr, l_trigger_name_arr, l_referenced_trigger_owner_arr, l_referenced_trigger_name_arr
      FROM (SELECT t.owner, t.trigger_name, t1.referenced_trigger_owner, t1.referenced_trigger_name
              FROM all_triggers t
              LEFT JOIN all_trigger_ordering t1
                ON t1.trigger_name = t.trigger_name
               AND t1.ordering_type = 'FOLLOWS'
             WHERE table_name = io_table_rec.table_name
               AND table_owner = io_table_rec.owner
           ) t
    CONNECT BY referenced_trigger_name = PRIOR trigger_name
           AND referenced_trigger_owner = PRIOR owner
    START WITH referenced_trigger_name IS NULL;

    FOR i IN 1..l_trigger_name_arr.COUNT LOOP
      l_full_name := '"'||l_owner_arr(i)||'"."'||l_trigger_name_arr(i)||'"';
      l_ref_indx_arr(l_full_name) := i;
    END LOOP;
    $END

    SELECT *
      BULK COLLECT
      INTO l_all_triggers_arr
      FROM all_triggers
     WHERE table_name = io_table_rec.table_name
       AND table_owner = io_table_rec.owner;

    FOR i IN 1..l_all_triggers_arr.COUNT LOOP
      l_full_name := '"'||l_all_triggers_arr(i).owner||'"."'||l_all_triggers_arr(i).trigger_name||'"';
      $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
        l_indx := l_ref_indx_arr(l_full_name);
      $ELSE
        l_indx := i;
      $END
      io_table_rec.trigger_indx_arr(l_full_name) := l_indx;
      io_table_rec.trigger_arr(l_indx).owner             := l_all_triggers_arr(i).owner;
      io_table_rec.trigger_arr(l_indx).trigger_name      := l_all_triggers_arr(i).trigger_name;
      io_table_rec.trigger_arr(l_indx).trigger_type      := l_all_triggers_arr(i).trigger_type;
      io_table_rec.trigger_arr(l_indx).triggering_event  := l_all_triggers_arr(i).triggering_event;
      io_table_rec.trigger_arr(l_indx).table_owner       := l_all_triggers_arr(i).table_owner;
      io_table_rec.trigger_arr(l_indx).base_object_type  := l_all_triggers_arr(i).base_object_type;
      io_table_rec.trigger_arr(l_indx).table_name        := l_all_triggers_arr(i).table_name;
      io_table_rec.trigger_arr(l_indx).column_name       := l_all_triggers_arr(i).column_name;
      io_table_rec.trigger_arr(l_indx).referencing_names := l_all_triggers_arr(i).referencing_names;
      io_table_rec.trigger_arr(l_indx).when_clause       := l_all_triggers_arr(i).when_clause;
      io_table_rec.trigger_arr(l_indx).status            := l_all_triggers_arr(i).status;
      io_table_rec.trigger_arr(l_indx).description       := l_all_triggers_arr(i).description;
      io_table_rec.trigger_arr(l_indx).action_type       := l_all_triggers_arr(i).action_type;
      io_table_rec.trigger_arr(l_indx).trigger_body      := l_all_triggers_arr(i).trigger_body;

      io_table_rec.trigger_arr(l_indx).rename_rec := get_rename_rec(
                                                       in_object_name  => l_all_triggers_arr(i).trigger_name,
                                                       in_object_owner => l_all_triggers_arr(i).owner, 
                                                       in_object_type  => 'TRIGGER'
                                                     );

      $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
        IF l_referenced_trigger_name_arr(l_indx) IS NOT NULL THEN
          l_full_name := '"'||l_referenced_trigger_owner_arr(l_indx)||'"."'||l_referenced_trigger_name_arr(l_indx)||'"';
          io_table_rec.trigger_arr(l_indx).referenced_trigger_indx := l_ref_indx_arr(l_full_name);
        END IF;
      $END

    END LOOP;
  END read_table_triggers;
  
  PROCEDURE read_table_policies(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    TYPE t_all_policies_arr         IS TABLE OF all_policies%ROWTYPE INDEX BY PLS_INTEGER;
    l_all_policies_arr              t_all_policies_arr;
    l_column_option_arr             arrays.gt_int_arr;
  BEGIN
    SELECT *
      BULK COLLECT
      INTO l_all_policies_arr
      FROM all_policies
     WHERE object_name = io_table_rec.table_name
       AND object_owner = io_table_rec.owner;

    FOR i IN 1..l_all_policies_arr.COUNT LOOP
--      io_table_rec.policy_indx_arr(l_full_name) := i;
      io_table_rec.policy_arr(i).object_owner   := l_all_policies_arr(i).object_owner;
      io_table_rec.policy_arr(i).object_name    := l_all_policies_arr(i).object_name;
      io_table_rec.policy_arr(i).policy_group   := l_all_policies_arr(i).policy_group;
      io_table_rec.policy_arr(i).policy_name    := l_all_policies_arr(i).policy_name;
      io_table_rec.policy_arr(i).pf_owner       := l_all_policies_arr(i).pf_owner;
      io_table_rec.policy_arr(i).package        := l_all_policies_arr(i).package;
      io_table_rec.policy_arr(i).function       := l_all_policies_arr(i).function;
      io_table_rec.policy_arr(i).sel            := l_all_policies_arr(i).sel;
      io_table_rec.policy_arr(i).ins            := l_all_policies_arr(i).ins;
      io_table_rec.policy_arr(i).upd            := l_all_policies_arr(i).upd;
      io_table_rec.policy_arr(i).del            := l_all_policies_arr(i).del;
      io_table_rec.policy_arr(i).idx            := l_all_policies_arr(i).idx;
      io_table_rec.policy_arr(i).chk_option     := l_all_policies_arr(i).chk_option;
      io_table_rec.policy_arr(i).enable         := l_all_policies_arr(i).enable;
      io_table_rec.policy_arr(i).static_policy  := l_all_policies_arr(i).static_policy;
      io_table_rec.policy_arr(i).policy_type    := l_all_policies_arr(i).policy_type;
      io_table_rec.policy_arr(i).long_predicate := l_all_policies_arr(i).long_predicate;

      SELECT sec_rel_column, column_option
        BULK COLLECT
        INTO io_table_rec.policy_arr(i).sec_rel_col_arr, l_column_option_arr
        FROM all_sec_relevant_cols
       WHERE object_owner = io_table_rec.policy_arr(i).object_owner
         AND object_name = io_table_rec.policy_arr(i).object_name 
         AND policy_group = io_table_rec.policy_arr(i).policy_group 
         AND policy_name = io_table_rec.policy_arr(i).policy_name;
      
      IF l_column_option_arr.COUNT > 0 THEN 
        io_table_rec.policy_arr(i).column_option := l_column_option_arr(1);
      END IF;                          
    END LOOP;   
    
  END read_table_policies;      
                                
  -- read table properties and all attributes/dependant objects
  PROCEDURE read_table_cascade(
    in_table_name      IN VARCHAR2,
    in_owner           IN VARCHAR2,
    out_table_rec      OUT NOCOPY gt_table_rec
  )
  AS
    l_table_rec  gt_table_rec;
  BEGIN
    -- read table attributes
    read_table(
      in_table_name => in_table_name,
      in_owner      => in_owner,
      out_table_rec => l_table_rec
    );
    -- read columns of existed table
    read_table_columns(l_table_rec);
    -- read all indexes for existing table
    read_table_indexes(l_table_rec);
    -- read all constraints for existing table
    read_table_constraints(l_table_rec);
    -- read all log groups for existing table
    read_table_log_groups(l_table_rec);
    -- read all lobs for existing table
    read_table_lobs(l_table_rec);
    -- read all XML columns for existing table
    read_table_xml_cols(l_table_rec);
    -- read all varray columns for existing table
    read_table_varrays(l_table_rec);
    -- read all reference constraints referencing from other tables to existed table
    read_table_references(l_table_rec);
    -- read all join indexes on other tables joined with existing table
    read_table_join_indexes(l_table_rec);
    -- find all columns included into join indexes
    read_table_triggers(l_table_rec);
    -- read partitions
    read_partitions(l_table_rec);
    -- read subpartitions
    read_subpartitions(l_table_rec);
    
    out_table_rec := l_table_rec;
  END read_table_cascade;

  -- read sequence attributes
  PROCEDURE read_sequence(
    in_sequence_name IN VARCHAR2,
    in_owner         IN VARCHAR2,
    out_sequence_rec IN OUT NOCOPY gt_sequence_rec
  )
  AS
    l_all_sequence_rec  all_sequences%ROWTYPE;
  BEGIN
    BEGIN
      SELECT *
        INTO l_all_sequence_rec
        FROM all_sequences
       WHERE sequence_owner = in_owner
         AND sequence_name = in_sequence_name;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RETURN;
    END;

    out_sequence_rec.owner           := l_all_sequence_rec.sequence_owner;
    out_sequence_rec.sequence_name   := l_all_sequence_rec.sequence_name;
    out_sequence_rec.min_value       := l_all_sequence_rec.min_value;
    out_sequence_rec.max_value       := l_all_sequence_rec.max_value;
    out_sequence_rec.increment_by    := l_all_sequence_rec.increment_by;
    out_sequence_rec.cycle_flag      := l_all_sequence_rec.cycle_flag;
    out_sequence_rec.order_flag      := l_all_sequence_rec.order_flag;
    out_sequence_rec.cache_size      := l_all_sequence_rec.cache_size;
    out_sequence_rec.last_number     := l_all_sequence_rec.last_number;   

    
    out_sequence_rec.rename_rec := get_rename_rec(
                                     in_object_name  => out_sequence_rec.sequence_name,
                                     in_object_owner => out_sequence_rec.owner, 
                                     in_object_type  => 'SEQUENCE'
                                   );

  END read_sequence;

  -- generic function return new name based on rename mode
  FUNCTION get_new_name(
    in_rename_rec  IN gt_rename_rec,
    in_rename_mode IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_new_name VARCHAR2(30);
  BEGIN
    CASE in_rename_mode
    WHEN 'TO_TEMP' THEN
      l_new_name := in_rename_rec.temp_name;
    WHEN 'TO_CORT' THEN
      l_new_name := in_rename_rec.cort_name;
    WHEN 'TO_ORIGINAL' THEN
      l_new_name := in_rename_rec.object_name;
    END CASE;
   RETURN l_new_name;  
  END get_new_name; 
  
  -- renames table and table's indexes, named constraints, named log groups, triggers and external references
  PROCEDURE rename_table_cascade(
    in_table_rec     IN gt_table_rec,
    in_rename_mode   IN VARCHAR2,
    io_frwd_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_indx PLS_INTEGER;
  BEGIN
    FOR i IN 1..in_table_rec.index_arr.COUNT LOOP
      IF in_table_rec.index_arr(i).rename_rec.generated = 'N' THEN
        cort_comp_pkg.rename_object(
          in_object_type   => 'INDEX',
          in_owner         => in_table_rec.index_arr(i).owner,
          in_from_name     => in_table_rec.index_arr(i).rename_rec.current_name,
          in_to_name       => get_new_name(in_table_rec.index_arr(i).rename_rec, in_rename_mode),
          io_frwd_stmt_arr => io_frwd_stmt_arr, 
          io_rlbk_stmt_arr => io_rlbk_stmt_arr
        );
      END IF;  
    END LOOP;
    FOR i IN 1..in_table_rec.constraint_arr.COUNT LOOP
      IF in_table_rec.constraint_arr(i).generated = 'USER NAME' THEN
        cort_comp_pkg.rename_object(
          in_object_type   => 'CONSTRAINT',
          in_owner         => in_table_rec.owner,
          in_from_name     => in_table_rec.constraint_arr(i).rename_rec.current_name,
          in_to_name       => get_new_name(in_table_rec.constraint_arr(i).rename_rec, in_rename_mode),
          in_table_name    => in_table_rec.rename_rec.current_name,
          io_frwd_stmt_arr => io_frwd_stmt_arr, 
          io_rlbk_stmt_arr => io_rlbk_stmt_arr
        );
      END IF;
    END LOOP;
    FOR i IN 1..in_table_rec.log_group_arr.COUNT LOOP
      IF in_table_rec.log_group_arr(i).generated = 'USER NAME' THEN
        cort_comp_pkg.rename_object(
          in_object_type   => 'CONSTRAINT',
          in_owner         => in_table_rec.owner,
          in_from_name     => in_table_rec.log_group_arr(i).rename_rec.current_name,
          in_to_name       => get_new_name(in_table_rec.log_group_arr(i).rename_rec, in_rename_mode),
          in_table_name    => in_table_rec.rename_rec.current_name,
          io_frwd_stmt_arr => io_frwd_stmt_arr, 
          io_rlbk_stmt_arr => io_rlbk_stmt_arr
        );
      END IF;
    END LOOP;

    l_indx := in_table_rec.lob_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      IF in_table_rec.lob_arr(l_indx).rename_rec.generated = 'N' THEN
        IF in_table_rec.lob_arr(l_indx).varray_column_indx IS NOT NULL THEN
          cort_comp_pkg.rename_object(
            in_object_type   => 'VARRAY',
            in_owner         => in_table_rec.owner,
            in_from_name     => in_table_rec.varray_arr(in_table_rec.lob_arr(l_indx).varray_column_indx).column_name,
            in_to_name       => get_new_name(in_table_rec.lob_arr(l_indx).rename_rec, in_rename_mode),
            in_table_name    => in_table_rec.rename_rec.current_name,
            io_frwd_stmt_arr => io_frwd_stmt_arr, 
            io_rlbk_stmt_arr => io_rlbk_stmt_arr
          );
        ELSE
          cort_comp_pkg.rename_object(
            in_object_type   => 'LOB',
            in_owner         => in_table_rec.owner,
            in_from_name     => in_table_rec.lob_arr(l_indx).rename_rec.current_name,
            in_to_name       => get_new_name(in_table_rec.lob_arr(l_indx).rename_rec, in_rename_mode),
            in_table_name    => in_table_rec.rename_rec.current_name,
            io_frwd_stmt_arr => io_frwd_stmt_arr, 
            io_rlbk_stmt_arr => io_rlbk_stmt_arr
          );
        END IF;
      END IF;  
      l_indx := in_table_rec.lob_arr.NEXT(l_indx);
    END LOOP;

    IF g_params.keep_refs THEN
      FOR i IN 1..in_table_rec.ref_constraint_arr.COUNT LOOP
        IF in_table_rec.ref_constraint_arr(i).generated = 'USER NAME' THEN
          cort_comp_pkg.rename_object(
            in_object_type   => 'CONSTRAINT',
            in_owner         => in_table_rec.ref_constraint_arr(i).owner,
            in_from_name     => in_table_rec.ref_constraint_arr(i).rename_rec.current_name,
            in_to_name       => get_new_name(in_table_rec.ref_constraint_arr(i).rename_rec, in_rename_mode),
            in_table_name    => in_table_rec.ref_constraint_arr(i).table_name,
            io_frwd_stmt_arr => io_frwd_stmt_arr, 
            io_rlbk_stmt_arr => io_rlbk_stmt_arr
          );
        END IF;
      END LOOP;
    END IF;  
/*    
    IF g_params.keep_triggers THEN
      FOR i IN 1..in_table_rec.trigger_arr.COUNT LOOP
        cort_comp_pkg.rename_object(
          in_object_type   => 'TRIGGER',
          in_owner         => in_table_rec.trigger_arr(i).owner,
          in_from_name     => in_table_rec.trigger_arr(i).rename_rec.current_name,
          in_to_name       => get_new_name(in_table_rec.trigger_arr(i).rename_rec, in_rename_mode),
          in_table_name    => in_table_rec.trigger_arr(i).rename_rec.current_name,
          io_frwd_stmt_arr => io_frwd_stmt_arr, 
          io_rlbk_stmt_arr => io_rlbk_stmt_arr
        );
      END LOOP;
    END IF;  
*/
    cort_comp_pkg.rename_object(
      in_object_type   => 'TABLE',
      in_owner         => in_table_rec.owner,
      in_from_name     => in_table_rec.rename_rec.current_name,
      in_to_name       => get_new_name(in_table_rec.rename_rec, in_rename_mode),
      io_frwd_stmt_arr => io_frwd_stmt_arr, 
      io_rlbk_stmt_arr => io_rlbk_stmt_arr
    );
  END rename_table_cascade;
/*
  -- renames table's triggers and external references
  PROCEDURE rename_triggers_and_refs(
    in_table_rec     IN gt_table_rec,
    in_rename_mode   IN VARCHAR2,
    io_frwd_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
  BEGIN
    FOR i IN 1..in_table_rec.ref_constraint_arr.COUNT LOOP
      cort_comp_pkg.rename_object(
        in_object_type   => 'CONSTRAINT',
        in_owner         => in_table_rec.ref_constraint_arr(i).owner,
        in_from_name     => in_table_rec.ref_constraint_arr(i).rename_rec.current_name,
        in_to_name       => get_new_name(in_table_rec.ref_constraint_arr(i).rename_rec, in_rename_mode),
        in_table_name    => in_table_rec.ref_constraint_arr(i).table_name,
        io_frwd_stmt_arr => io_frwd_stmt_arr, 
        io_rlbk_stmt_arr => io_rlbk_stmt_arr
      );
    END LOOP;
    FOR i IN 1..in_table_rec.trigger_arr.COUNT LOOP
      cort_comp_pkg.rename_object(
        in_object_type   => 'TRIGGER',
        in_owner         => in_table_rec.trigger_arr(i).owner,
        in_from_name     => in_table_rec.trigger_arr(i).rename_rec.current_name,
        in_to_name       => get_new_name(in_table_rec.trigger_arr(i).rename_rec, in_rename_mode),
        in_table_name    => in_table_rec.trigger_arr(i).rename_rec.current_name,
        io_frwd_stmt_arr => io_frwd_stmt_arr, 
        io_rlbk_stmt_arr => io_rlbk_stmt_arr
      );
    END LOOP;
  END rename_triggers_and_refs;
*/
  PROCEDURE mark_join_columns(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    l_column_name     VARCHAR2(30);
    l_full_index_name VARCHAR2(65);

    PROCEDURE mark_join_column(in_column_name IN VARCHAR)
    AS
      l_indx            PLS_INTEGER;
    BEGIN
      IF io_table_rec.column_indx_arr.EXISTS(in_column_name) THEN
        l_indx := io_table_rec.column_indx_arr(in_column_name);
        io_table_rec.column_arr(l_indx).join_index_column := TRUE;
        io_table_rec.column_arr(l_indx).join_index_arr(io_table_rec.column_arr(l_indx).join_index_arr.COUNT+1) := l_full_index_name;
      END IF;
    END mark_join_column;

  BEGIN
    FOR i IN 1..io_table_rec.index_arr.COUNT LOOP
      IF io_table_rec.index_arr(i).index_type = 'BITMAP' AND io_table_rec.index_arr(i).join_index = 'YES' THEN
        l_full_index_name := '"'||io_table_rec.index_arr(i).owner||'"."'||io_table_rec.index_arr(i).index_name||'"';
        FOR j IN 1..io_table_rec.index_arr(i).column_arr.COUNT LOOP
          IF io_table_rec.index_arr(i).column_table_owner_arr(j) = io_table_rec.owner AND
             io_table_rec.index_arr(i).column_table_arr(j) = io_table_rec.table_name
          THEN
            mark_join_column(io_table_rec.index_arr(i).column_arr(j));
          END IF;
        END LOOP;
        FOR j IN 1..io_table_rec.index_arr(i).join_inner_column_arr.COUNT LOOP
          IF io_table_rec.index_arr(i).join_inner_owner_arr(j) = io_table_rec.owner AND
             io_table_rec.index_arr(i).join_inner_table_arr(j) = io_table_rec.table_name
          THEN
            mark_join_column(io_table_rec.index_arr(i).join_inner_column_arr(j));
          END IF;
        END LOOP;
      END IF;
    END LOOP;
    FOR i IN 1..io_table_rec.join_index_arr.COUNT LOOP
      l_full_index_name := '"'||io_table_rec.join_index_arr(i).owner||'"."'||io_table_rec.join_index_arr(i).index_name||'"';
      FOR j IN 1..io_table_rec.join_index_arr(i).column_arr.COUNT LOOP
        IF io_table_rec.join_index_arr(i).column_table_owner_arr(j) = io_table_rec.owner AND
           io_table_rec.join_index_arr(i).column_table_arr(j) = io_table_rec.table_name
        THEN
          mark_join_column(io_table_rec.join_index_arr(i).column_arr(j));
        END IF;
      END LOOP;
      FOR j IN 1..io_table_rec.join_index_arr(i).join_outer_column_arr.COUNT LOOP
        IF io_table_rec.join_index_arr(i).join_outer_owner_arr(j) = io_table_rec.owner AND
           io_table_rec.join_index_arr(i).join_outer_table_arr(j) = io_table_rec.table_name
        THEN
          mark_join_column(io_table_rec.join_index_arr(i).join_outer_column_arr(j));
        END IF;
      END LOOP;
    END LOOP;
  END mark_join_columns;

  -- finds all check constraint that was implicitly created by NOT NULL column constraint and mark their type as 'N'
  PROCEDURE mark_notnull_constraints(
    io_constraint_arr IN OUT NOCOPY gt_constraint_arr,
    io_column_arr     IN OUT NOCOPY gt_column_arr
  )
  AS
    l_column_indx   arrays.gt_int_indx;
    l_indx          PLS_INTEGER;
  BEGIN
    -- bild index
    FOR i IN 1..io_column_arr.COUNT LOOP
      l_column_indx(io_column_arr(i).column_name) := i;
    END LOOP;

    -- find all check constraints
    FOR i IN 1..io_constraint_arr.COUNT LOOP
      IF io_constraint_arr(i).constraint_type = 'C' AND
         io_constraint_arr(i).column_arr.COUNT = 1 AND
         io_constraint_arr(i).search_condition = '"'||io_constraint_arr(i).column_arr(1)||'" IS NOT NULL' AND
         l_column_indx.EXISTS(io_constraint_arr(i).column_arr(1))
      THEN
        l_indx := l_column_indx(io_constraint_arr(i).column_arr(1));
        IF io_column_arr(l_indx).nullable = 'N' THEN
          io_constraint_arr(i).constraint_type := 'N';
          debug(io_constraint_arr(i).constraint_name||' ('||io_constraint_arr(i).column_arr(1)||') is NOT NULL constraint');
          IF io_constraint_arr(i).generated = 'USER NAME' THEN
            io_column_arr(l_indx).notnull_constraint_name := io_constraint_arr(i).constraint_name;
          END IF;
        END IF;
      END IF;

      IF io_constraint_arr(i).constraint_type = 'F' AND
         io_constraint_arr(i).column_arr.COUNT > 1
      THEN
        FOR j IN 1..io_constraint_arr(i).column_arr.COUNT LOOP
          IF io_constraint_arr(i).search_condition = '"'||io_constraint_arr(i).column_arr(j)||'" IS NOT NULL' AND
             l_column_indx.EXISTS(io_constraint_arr(i).column_arr(j))
          THEN   
            l_indx := l_column_indx(io_constraint_arr(i).column_arr(j));
            IF io_column_arr(l_indx).nullable = 'N' THEN
              io_constraint_arr(i).constraint_type := 'N';
              debug(io_constraint_arr(i).constraint_name||' is NOT NULL constraint');
              IF io_constraint_arr(i).generated = 'USER NAME' THEN
                io_column_arr(l_indx).notnull_constraint_name := io_constraint_arr(i).constraint_name;
              END IF;
            END IF;
          END IF;
        END LOOP;
      END IF;
    END LOOP;
  END mark_notnull_constraints;

  -- update join index
  PROCEDURE update_join_index(
    io_index_rec IN OUT NOCOPY gt_index_rec,
    in_table_rec IN            gt_table_rec
  )
  AS
    l_column_name VARCHAR2(30);
    l_column_indx PLS_INTEGER;
  BEGIN
    IF io_index_rec.table_name = in_table_rec.table_name AND
       io_index_rec.table_owner = in_table_rec.owner THEN
      FOR i IN 1..io_index_rec.column_arr.COUNT LOOP
        l_column_name := io_index_rec.column_arr(i);
        IF in_table_rec.column_indx_arr.EXISTS(l_column_name) THEN
          l_column_indx := in_table_rec.column_indx_arr(l_column_name);
          IF in_table_rec.column_arr(l_column_indx).new_column_name IS NOT NULL THEN
            io_index_rec.column_arr(i) := in_table_rec.column_arr(l_column_indx).new_column_name;
          END IF;
        END IF;
      END LOOP;
    END IF;

    FOR i IN 1..io_index_rec.join_inner_column_arr.COUNT LOOP
      IF io_index_rec.join_inner_table_arr(i) = in_table_rec.table_name AND
         io_index_rec.join_inner_owner_arr(i) = in_table_rec.owner THEN
        l_column_name := io_index_rec.join_outer_column_arr(i);
        IF in_table_rec.column_indx_arr.EXISTS(l_column_name) THEN
          l_column_indx := in_table_rec.column_indx_arr(l_column_name);
          IF in_table_rec.column_arr(l_column_indx).new_column_name IS NOT NULL THEN
            io_index_rec.join_inner_column_arr(i) := in_table_rec.column_arr(l_column_indx).new_column_name;
          END IF;
        END IF;
      END IF;
    END LOOP;

    FOR i IN 1..io_index_rec.join_outer_column_arr.COUNT LOOP
      IF io_index_rec.join_outer_table_arr(i) = in_table_rec.table_name AND
         io_index_rec.join_outer_owner_arr(i) = in_table_rec.owner THEN
        l_column_name := io_index_rec.join_outer_column_arr(i);
        IF in_table_rec.column_indx_arr.EXISTS(l_column_name) THEN
          l_column_indx := in_table_rec.column_indx_arr(l_column_name);
          IF in_table_rec.column_arr(l_column_indx).new_column_name IS NOT NULL THEN
            io_index_rec.join_outer_column_arr(i) := in_table_rec.column_arr(l_column_indx).new_column_name;
          END IF;
        END IF;
      END IF;
    END LOOP;

  END update_join_index;

  PROCEDURE alter_table(
    in_table_rec           IN  gt_table_rec,
    io_frwd_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  AS
    l_index_rec            gt_index_rec;
    l_frwd_drop_stmt_arr   arrays.gt_clob_arr;
    l_rlbk_drop_stmt_arr   arrays.gt_clob_arr;
    l_frwd_create_stmt_arr arrays.gt_clob_arr;
    l_rlbk_create_stmt_arr arrays.gt_clob_arr;
    l_frwd_alter_stmt_arr  arrays.gt_clob_arr;
    l_rlbk_alter_stmt_arr  arrays.gt_clob_arr;
  BEGIN
    FOR i IN 1..in_table_rec.index_arr.COUNT LOOP
      IF in_table_rec.index_arr(i).drop_flag AND in_table_rec.index_arr(i).recreate_flag THEN
        l_index_rec := in_table_rec.index_arr(i);
        cort_comp_pkg.drop_index(
          in_index_rec           => l_index_rec,
          io_frwd_alter_stmt_arr => l_frwd_drop_stmt_arr,
          io_rlbk_alter_stmt_arr => l_rlbk_drop_stmt_arr
        );
        update_join_index(
          io_index_rec => l_index_rec,
          in_table_rec => in_table_rec
        );
        cort_comp_pkg.create_index(
          in_index_rec           => l_index_rec,
          io_frwd_alter_stmt_arr => l_frwd_create_stmt_arr,
          io_rlbk_alter_stmt_arr => l_rlbk_create_stmt_arr
        );
      END IF;
    END LOOP;
    FOR i IN 1..in_table_rec.join_index_arr.COUNT LOOP
      IF in_table_rec.join_index_arr(i).recreate_flag THEN
        l_index_rec := in_table_rec.join_index_arr(i);
        cort_comp_pkg.drop_index(
          in_index_rec           => l_index_rec,
          io_frwd_alter_stmt_arr => l_frwd_drop_stmt_arr,
          io_rlbk_alter_stmt_arr => l_rlbk_drop_stmt_arr
        );
        update_join_index(
          io_index_rec => l_index_rec,
          in_table_rec => in_table_rec
        );
        cort_comp_pkg.create_index(
          in_index_rec           => l_index_rec,
          io_frwd_alter_stmt_arr => l_frwd_create_stmt_arr,
          io_rlbk_alter_stmt_arr => l_rlbk_create_stmt_arr
        );
      END IF;
    END LOOP;
    l_frwd_alter_stmt_arr := l_frwd_drop_stmt_arr;
    l_rlbk_alter_stmt_arr := l_rlbk_drop_stmt_arr;
    FOR i IN 1..io_frwd_alter_stmt_arr.COUNT LOOP
      l_frwd_alter_stmt_arr(l_frwd_alter_stmt_arr.COUNT+1) := io_frwd_alter_stmt_arr(i);
      l_rlbk_alter_stmt_arr(l_rlbk_alter_stmt_arr.COUNT+1) := io_rlbk_alter_stmt_arr(i);
    END LOOP;
    FOR i IN 1..l_frwd_create_stmt_arr.COUNT LOOP
      l_frwd_alter_stmt_arr(l_frwd_alter_stmt_arr.COUNT+1) := l_frwd_create_stmt_arr(i);
      l_rlbk_alter_stmt_arr(l_rlbk_alter_stmt_arr.COUNT+1) := l_rlbk_create_stmt_arr(i);
    END LOOP;
    io_frwd_alter_stmt_arr := l_frwd_alter_stmt_arr;
    io_rlbk_alter_stmt_arr := l_rlbk_alter_stmt_arr;
    apply_changes(
      in_frwd_alter_stmt_arr => l_frwd_alter_stmt_arr,
      in_rlbk_alter_stmt_arr => l_rlbk_alter_stmt_arr,
      in_test                => g_params.test 
    );
  END alter_table;
  
  -- update triggers definition - include table name in double quotes - workaround for Oracle bug
  PROCEDURE update_triggers(
    in_table_rec IN gt_table_rec
  )
  AS
    l_frwd_alter_stmt_arr      arrays.gt_clob_arr;
    l_rlbk_alter_stmt_arr      arrays.gt_clob_arr;
  BEGIN
    FOR i IN 1..in_table_rec.trigger_arr.COUNT LOOP
      cort_comp_pkg.update_trigger(
        in_table_rec           => in_table_rec,
        in_trigger_rec         => in_table_rec.trigger_arr(i),
        io_frwd_alter_stmt_arr => l_frwd_alter_stmt_arr,
        io_rlbk_alter_stmt_arr => l_rlbk_alter_stmt_arr
      );
    END LOOP;
    apply_changes(
      in_frwd_alter_stmt_arr => l_frwd_alter_stmt_arr,
      in_rlbk_alter_stmt_arr => l_rlbk_alter_stmt_arr
    );
  END update_triggers;

  -- copy table's and columns' comments
  PROCEDURE copy_comments(
    in_source_table_rec IN gt_table_rec,
    in_target_table_rec IN gt_table_rec,
    io_frwd_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_tab_comment     all_tab_comments.comments%TYPE;
    l_col_arr         arrays.gt_str_arr;
    l_col_comment_arr arrays.gt_lstr_arr;
    l_ddl             VARCHAR2(4200);
    l_col_name        VARCHAR2(30);
    l_indx            PLS_INTEGER;
  BEGIN
    BEGIN
      SELECT comments
        INTO l_tab_comment
        FROM all_tab_comments
       WHERE owner = in_source_table_rec.owner
         AND table_name = in_source_table_rec.table_name
         AND table_type = 'TABLE';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;
    IF l_tab_comment IS NOT NULL THEN
      l_ddl := 'COMMENT ON TABLE "'||in_target_table_rec.owner||'"."'||in_target_table_rec.table_name||'" IS Q''{'||l_tab_comment||'}''';
      cort_comp_pkg.add_stmt(
        io_frwd_stmt_arr => io_frwd_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_stmt_arr,
        in_frwd_stmt     => l_ddl,
        in_rlbk_stmt     => NULL
      );  
    END IF;
    SELECT column_name, comments
      BULK COLLECT
      INTO l_col_arr, l_col_comment_arr
      FROM all_col_comments
     WHERE owner = in_source_table_rec.owner
       AND table_name = in_source_table_rec.table_name
       AND comments IS NOT NULL;
     FOR i IN 1..l_col_comment_arr.COUNT LOOP
       l_col_name := l_col_arr(i);
       IF in_source_table_rec.column_indx_arr.EXISTS(l_col_name) THEN
         l_indx := in_source_table_rec.column_indx_arr(l_col_name);
         l_col_name := NVL(in_source_table_rec.column_arr(l_indx).new_column_name, in_source_table_rec.column_arr(l_indx).column_name);
         IF in_target_table_rec.column_indx_arr.EXISTS(l_col_name) THEN
           l_ddl := 'COMMENT ON COLUMN "'||in_target_table_rec.owner||'"."'||in_target_table_rec.table_name||'"."'||l_col_name||'" IS Q''{'||l_col_comment_arr(i)||'}''';
           cort_comp_pkg.add_stmt(
             io_frwd_stmt_arr => io_frwd_stmt_arr,
             io_rlbk_stmt_arr => io_rlbk_stmt_arr,
             in_frwd_stmt     => l_ddl,
             in_rlbk_stmt     => NULL
           );  
         END IF;
       END IF;
     END LOOP;
  END copy_comments;

  -- copy table's privileges
  PROCEDURE copy_privileges(
    io_source_table_rec IN OUT NOCOPY gt_table_rec,
    io_target_table_rec IN OUT NOCOPY gt_table_rec,
    io_frwd_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
  BEGIN
    read_privileges(
      in_owner         => io_source_table_rec.owner,
      in_table_name    => io_source_table_rec.table_name,
      io_privilege_arr => io_source_table_rec.privilege_arr
    );
    cort_comp_pkg.copy_privileges(
      in_source_table_rec      => io_source_table_rec,
      io_target_table_rec      => io_target_table_rec,
      io_frwd_alter_stmt_arr   => io_frwd_stmt_arr,
      io_rlbk_alter_stmt_arr   => io_rlbk_stmt_arr
    );
  END copy_privileges;

  -- copy sequence's privileges
  PROCEDURE copy_privileges(
    io_source_sequence_rec IN OUT NOCOPY gt_sequence_rec,
    io_target_sequence_rec IN OUT NOCOPY gt_sequence_rec,
    io_frwd_stmt_arr       IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr       IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
  BEGIN
    read_privileges(
      in_owner         => io_source_sequence_rec.owner,
      in_table_name    => io_source_sequence_rec.sequence_name,
      io_privilege_arr => io_source_sequence_rec.privilege_arr
    );
    io_target_sequence_rec.privilege_arr := io_source_sequence_rec.privilege_arr;
    
    FOR i IN 1..io_target_sequence_rec.privilege_arr.COUNT LOOP
      io_target_sequence_rec.privilege_arr(i).table_name := io_target_sequence_rec.sequence_name;
    END LOOP;
    
    cort_comp_pkg.get_privileges_stmt(
      in_privilege_arr         => io_target_sequence_rec.privilege_arr,
      io_frwd_alter_stmt_arr   => io_frwd_stmt_arr, 
      io_rlbk_alter_stmt_arr   => io_rlbk_stmt_arr
    );
    
  END copy_privileges;


  -- Disable/Enable all FK before/after data load
  PROCEDURE change_status_for_all_fk(
    in_table_rec     IN gt_table_rec,
    in_action        IN VARCHAR2,  -- DISABLE/ENABLE
    io_frwd_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS
    l_validate VARCHAR2(30);
    l_sql      VARCHAR2(1000);
  BEGIN
    IF in_action NOT IN ('ENABLE','DISABLE') THEN
      RETURN;
    END IF;

    FOR i IN 1..in_table_rec.constraint_arr.COUNT LOOP
      IF in_table_rec.constraint_arr(i).constraint_type = 'R' AND
         in_table_rec.constraint_arr(i).status = 'ENABLED'
      THEN
        IF in_action = 'ENABLE' THEN
          IF NOT g_params.validate_refs THEN
            l_validate := 'NOVALIDATE';
          ELSE
            l_validate := NULL;
          END IF;
        END IF;
        l_sql := 'ALTER TABLE "'||in_table_rec.owner||'"."'||in_table_rec.table_name||'" '||
                 in_action||' '||l_validate||' CONSTRAINT "'||in_table_rec.constraint_arr(i).constraint_name||'"';
        cort_comp_pkg.add_stmt(
          io_frwd_stmt_arr => io_frwd_stmt_arr,
          io_rlbk_stmt_arr => io_rlbk_stmt_arr,
          in_frwd_stmt     => l_sql,
          in_rlbk_stmt     => NULL
        );  
      END IF;
    END LOOP;
  END change_status_for_all_fk;            
                 
  -- copying data from old tale to new one
  PROCEDURE copy_data(
    in_source_table_rec IN gt_table_rec,
    in_target_table_rec IN gt_table_rec,
    io_frwd_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr
  )
  AS                        
    l_columns_list  CLOB;
    l_values_list   CLOB;
    l_sql           CLOB;
    l_parallel      VARCHAR2(30);
  BEGIN
    cort_comp_pkg.get_column_values_list(
      in_source_table_rec => in_source_table_rec,
      in_target_table_rec => in_target_table_rec, 
      out_columns_list    => l_columns_list, 
      out_values_list     => l_values_list
    );
    
    IF g_params.parallel > 1 THEN
      l_parallel := 'PARALLEL ('||g_params.parallel||')';
      l_sql := 'ALTER SESSION ENABLE PARALLEL DML';
      cort_comp_pkg.add_stmt(
        io_frwd_stmt_arr => io_frwd_stmt_arr,
        io_rlbk_stmt_arr => io_rlbk_stmt_arr,
        in_frwd_stmt     => l_sql,
        in_rlbk_stmt     => NULL
      );
    ELSE
      l_parallel := NULL;    
    END IF;

    --check for non-empty strings
    IF l_columns_list IS NOT NULL AND 
       l_values_list IS NOT NULL 
    THEN
      l_sql := 'INSERT /*+ APPEND '||l_parallel||' */ INTO "'||in_target_table_rec.rename_rec.current_name||'" ('||l_columns_list||') '||CHR(10)||
               'SELECT /*+ '||l_parallel||' */ '||l_values_list||' FROM "'||in_source_table_rec.owner||'"."'||in_source_table_rec.rename_rec.current_name||'" '||g_params.alias;
    ELSE
      RETURN;
    END IF;  
    
    -- disable all foreign keys
    change_status_for_all_fk(
      in_table_rec     => in_target_table_rec,
      in_action        => 'DISABLE',
      io_frwd_stmt_arr => io_frwd_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_stmt_arr
    );

    cort_comp_pkg.add_stmt(
      io_frwd_stmt_arr => io_frwd_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_stmt_arr,
      in_frwd_stmt     => l_sql,
      in_rlbk_stmt     => NULL
    );  

    cort_comp_pkg.add_stmt(
      io_frwd_stmt_arr => io_frwd_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_stmt_arr,
      in_frwd_stmt      => 'COMMIT',
      in_rlbk_stmt      => NULL
    );  

    -- enable all foreign keys
    change_status_for_all_fk(
      in_table_rec     => in_target_table_rec,
      in_action        => 'ENABLE',
      io_frwd_stmt_arr => io_frwd_stmt_arr,
      io_rlbk_stmt_arr => io_rlbk_stmt_arr
    );
    
  END copy_data;

  -- copy all table's triggers
  PROCEDURE copy_triggers(
    io_source_table_rec IN OUT NOCOPY gt_table_rec,
    io_target_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    l_frwd_alter_stmt_arr      arrays.gt_clob_arr;
    l_rlbk_alter_stmt_arr      arrays.gt_clob_arr;
  BEGIN
    cort_comp_pkg.copy_triggers(
      in_source_table_rec      => io_source_table_rec,
      io_target_table_rec      => io_target_table_rec,
      io_frwd_alter_stmt_arr   => l_frwd_alter_stmt_arr,
      io_rlbk_alter_stmt_arr   => l_rlbk_alter_stmt_arr
    );

    apply_changes(
      in_frwd_alter_stmt_arr => l_frwd_alter_stmt_arr,
      in_rlbk_alter_stmt_arr => l_rlbk_alter_stmt_arr,
      in_test                => g_params.test
    );

  END copy_triggers;

  -- copy all table's policies
  PROCEDURE copy_policies(
    in_source_table_rec IN gt_table_rec,
    in_target_table_rec IN gt_table_rec
  )
  AS
    l_frwd_alter_stmt_arr      arrays.gt_clob_arr;
    l_rlbk_alter_stmt_arr      arrays.gt_clob_arr;
  BEGIN
    cort_comp_pkg.copy_policies(
      in_source_table_rec      => in_source_table_rec,
      in_target_table_rec      => in_target_table_rec,
      io_frwd_alter_stmt_arr   => l_frwd_alter_stmt_arr,
      io_rlbk_alter_stmt_arr   => l_rlbk_alter_stmt_arr
    );

    apply_changes(
      in_frwd_alter_stmt_arr => l_frwd_alter_stmt_arr,
      in_rlbk_alter_stmt_arr => l_rlbk_alter_stmt_arr,
      in_test                => g_params.test
    );

  END copy_policies;

  PROCEDURE export_stats(
    in_table_rec IN gt_table_rec
  )
  AS
  BEGIN
    debug('Export stats from "'||in_table_rec.owner||'"."'||in_table_rec.rename_rec.current_name||'"');
    dbms_stats.export_table_stats(
      ownname => '"'||in_table_rec.owner||'"',
      tabname => '"'||in_table_rec.rename_rec.current_name||'"',
      stattab => '"'||gc_cort_stat_table||'"',
      statown => '"'||cort_aux_pkg.gc_cort_schema||'"'
    );
  END export_stats;

  PROCEDURE import_stats(
    in_table_rec  IN gt_table_rec
  )
  AS
  BEGIN
    debug('Import stats into "'||in_table_rec.owner||'"."'||in_table_rec.rename_rec.current_name||'"');
    dbms_stats.import_table_stats(
      ownname => '"'||in_table_rec.owner||'"',
      tabname => '"'||in_table_rec.rename_rec.current_name||'"',
      stattab => '"'||gc_cort_stat_table||'"',
      statown => '"'||cort_aux_pkg.gc_cort_schema||'"'
    );
  END import_stats;
  
  PROCEDURE copy_stats(
    in_source_table_rec IN gt_table_rec,
    in_target_table_rec IN gt_table_rec
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    export_stats(in_source_table_rec);
    cort_aux_pkg.copy_stats(
      in_source_table_name => in_source_table_rec.rename_rec.current_name,
      in_target_table_name => in_target_table_rec.rename_rec.current_name
    );  
    import_stats(in_target_table_rec);
    COMMIT;  
  END copy_stats;

  -- move data using exchange partition mechanism
  PROCEDURE move_data(
    in_source_table_rec IN gt_table_rec,
    in_target_table_rec IN gt_table_rec,
    io_frwd_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr,    
    io_rlbk_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr    
  )
  AS                             
    l_swap_table_name        VARCHAR2(30);
    l_swap_table_rec         gt_table_rec;
    l_sql                    CLOB;
    l_source_part_name       VARCHAR2(30);
    l_target_part_name       VARCHAR2(30);
    l_frwd_stmt_arr          arrays.gt_clob_arr;
    l_rlbk_stmt_arr          arrays.gt_clob_arr;
  BEGIN                          
    l_swap_table_name := get_object_temp_name(
                           in_object_id   => in_source_table_rec.rename_rec.object_id,
                           in_owner       => in_source_table_rec.owner,
                           in_prefix      => gc_cort_swap_prefix
                         );     
    l_swap_table_rec.table_name := l_swap_table_name;
    l_swap_table_rec.owner := in_source_table_rec.owner;
    l_swap_table_rec.rename_rec.object_name := l_swap_table_name;
    l_swap_table_rec.rename_rec.object_owner := in_source_table_rec.owner;
    l_swap_table_rec.rename_rec.current_name := l_swap_table_name;
    l_swap_table_rec.part_key_column_arr(1) := cort_comp_pkg.get_column_for_partitioning(in_source_table_rec);
    
    IF in_source_table_rec.partitioned = 'YES' AND
       (in_source_table_rec.subpartitioning_type <> 'NONE' OR NOT cort_comp_pkg.is_subpartitioning_available(in_target_table_rec))
    THEN
      -- repeat for every partition
      FOR i IN 1..in_target_table_rec.partition_arr.COUNT LOOP
        --to-do: check that partition exists in both tables 
        IF in_target_table_rec.partition_arr(i).matching_indx IS NOT NULL THEN
          l_target_part_name := in_target_table_rec.partition_arr(i).partition_name;
          l_source_part_name := in_source_table_rec.partition_arr(in_target_table_rec.partition_arr(i).matching_indx).partition_name;
          -- create swap table for given partition
          cort_comp_pkg.create_swap_table_sql(
            in_table_rec      => in_target_table_rec,
            in_swap_table_rec => l_swap_table_rec,
            in_partition_rec  => in_target_table_rec.partition_arr(i),
            io_frwd_stmt_arr  => l_frwd_stmt_arr,
            io_rlbk_stmt_arr  => l_rlbk_stmt_arr
          );
          -- forward changes:
          FOR i IN 1..l_frwd_stmt_arr.COUNT LOOP
            io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_frwd_stmt_arr(i);
          END LOOP;
          FOR i IN 1..l_rlbk_stmt_arr.COUNT LOOP
            io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_rlbk_stmt_arr(i);
          END LOOP;

          -- Exchange partition from source table with swap table 
          cort_comp_pkg.exchange_partition(
            in_source_table_rec => in_source_table_rec,
            in_target_table_rec => l_swap_table_rec,
            in_partition_name   => l_source_part_name,
            io_frwd_stmt_arr    => io_frwd_stmt_arr,
            io_rlbk_stmt_arr    => io_rlbk_stmt_arr
          );
          -- Exchange partition from target table with swap table 
          cort_comp_pkg.exchange_partition(
            in_source_table_rec => in_target_table_rec,
            in_target_table_rec => l_swap_table_rec,
            in_partition_name   => l_target_part_name,
            io_frwd_stmt_arr    => io_frwd_stmt_arr,
            io_rlbk_stmt_arr    => io_rlbk_stmt_arr
          );

          -- drop swap table for given partition (switch forward and rollback statements). Change becomes absolutely symmetric
          FOR i IN 1..l_rlbk_stmt_arr.COUNT LOOP
            io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_rlbk_stmt_arr(i);
          END LOOP;
          FOR i IN 1..l_frwd_stmt_arr.COUNT LOOP
            io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_frwd_stmt_arr(i);
          END LOOP;
          
        END IF;  
      END LOOP;
    ELSE
      l_target_part_name := 'P_DEF';
      -- do it only once
      cort_comp_pkg.create_swap_table_sql(
        in_table_rec      => in_target_table_rec,
        in_swap_table_rec => l_swap_table_rec,
        in_partition_rec  => NULL,
        io_frwd_stmt_arr  => l_frwd_stmt_arr,
        io_rlbk_stmt_arr  => l_rlbk_stmt_arr
      );
      -- forward changes:
      FOR i IN 1..l_frwd_stmt_arr.COUNT LOOP
        io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_frwd_stmt_arr(i);
      END LOOP;
      FOR i IN 1..l_rlbk_stmt_arr.COUNT LOOP
        io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_rlbk_stmt_arr(i);
      END LOOP;
      
      -- Exchange partition from source table with swap table 
      cort_comp_pkg.exchange_partition(
        in_source_table_rec => l_swap_table_rec,
        in_target_table_rec => in_source_table_rec,
        in_partition_name   => l_target_part_name,
        io_frwd_stmt_arr    => io_frwd_stmt_arr,
        io_rlbk_stmt_arr    => io_rlbk_stmt_arr
      );
      -- Exchange partition from source table with swap table 
      cort_comp_pkg.exchange_partition(
        in_source_table_rec => l_swap_table_rec,
        in_target_table_rec => in_target_table_rec,
        in_partition_name   => l_target_part_name,
        io_frwd_stmt_arr    => io_frwd_stmt_arr,
        io_rlbk_stmt_arr    => io_rlbk_stmt_arr
      );
      
      -- drop swap table for given partition (switch forward and rollback statements). Change becomes absolutely symmetric
      FOR i IN 1..l_rlbk_stmt_arr.COUNT LOOP
        io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_rlbk_stmt_arr(i);
      END LOOP;
      FOR i IN 1..l_frwd_stmt_arr.COUNT LOOP
        io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_frwd_stmt_arr(i);
      END LOOP;
      
    END IF;
  END move_data;


  -- rename tables, copy data, move references, copy comments, copy grants, reassign synonyms
  PROCEDURE recreate_table(
    io_source_table_rec IN OUT NOCOPY gt_table_rec,
    io_target_table_rec IN OUT NOCOPY gt_table_rec,
    io_frwd_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr,
    io_rlbk_stmt_arr    IN OUT NOCOPY arrays.gt_clob_arr,
    in_recreate_mode    IN PLS_INTEGER,
    in_sid              IN VARCHAR2
  )
  AS
    l_cnt              PLS_INTEGER;
    l_timer            PLS_INTEGER; 
    l_lock_mode        PLS_INTEGER;
    l_frwd_stmt_arr    arrays.gt_clob_arr;
    l_rlbk_stmt_arr    arrays.gt_clob_arr;
  BEGIN
    -- following changes do not have rollback plan because applied to temp table
    BEGIN
      IF g_params.keep_data AND 
         NOT io_source_table_rec.is_table_empty AND
         in_recreate_mode = cort_comp_pkg.gc_result_recreate 
      THEN
        start_timer(l_timer);
        copy_data(
          in_source_table_rec => io_source_table_rec,
          in_target_table_rec => io_target_table_rec,
          io_frwd_stmt_arr    => l_frwd_stmt_arr,     
          io_rlbk_stmt_arr    => l_rlbk_stmt_arr
        );                               
        stop_timer(l_timer, 'Copy data');
      END IF;
      -- following changes are added to the queue 
      IF g_params.keep_data AND 
         NOT io_source_table_rec.is_table_empty AND
         in_recreate_mode = cort_comp_pkg.gc_result_exchange 
      THEN
        start_timer(l_timer);
        -- add create swap table exchange partition and drop swap table statements into the queue
        move_data(
          in_source_table_rec => io_source_table_rec,
          in_target_table_rec => io_target_table_rec,
          io_frwd_stmt_arr    => l_frwd_stmt_arr,     
          io_rlbk_stmt_arr    => l_rlbk_stmt_arr
        );
        stop_timer(l_timer, 'Move data');
      END IF;

      apply_changes(
        in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
        in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr,
        in_test                => g_params.test
      );

      -- execute immediate
      IF g_params.keep_stats THEN
        start_timer(l_timer);
        copy_stats(
          in_source_table_rec => io_source_table_rec,
          in_target_table_rec => io_target_table_rec
        );
        stop_timer(l_timer, 'Copy stats');
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        debug('Error. Rolling back...');
        g_error_stack := g_error_stack||dbms_utility.format_error_backtrace||CHR(10);
        IF NOT g_params.keep_temp_table THEN
          exec_drop_table(    
            in_table_name => io_target_table_rec.owner,
            in_owner      => io_target_table_rec.rename_rec.current_name
          );
        END IF;  
        RAISE;
    END;
    
    FOR i IN 1..l_frwd_stmt_arr.COUNT LOOP
      io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_frwd_stmt_arr(i); 
    END LOOP;
    l_frwd_stmt_arr.DELETE;
    io_rlbk_stmt_arr.DELETE;

    BEGIN  
      IF g_params.keep_indexes THEN
        start_timer(l_timer);
        cort_comp_pkg.copy_indexes(
          in_source_table_rec      => io_source_table_rec,
          io_target_table_rec      => io_target_table_rec,
          io_frwd_alter_stmt_arr   => l_frwd_stmt_arr,
          io_rlbk_alter_stmt_arr   => l_rlbk_stmt_arr
        );
        stop_timer(l_timer, 'Copy indexes');
      END IF;
      IF g_params.keep_comments THEN
        start_timer(l_timer);
        copy_comments(
          in_source_table_rec => io_source_table_rec,
          in_target_table_rec => io_target_table_rec,
          io_frwd_stmt_arr    => l_frwd_stmt_arr,
          io_rlbk_stmt_arr    => l_rlbk_stmt_arr
        );
        stop_timer(l_timer, 'Copy comments');
      END IF;
      IF g_params.keep_privs THEN
        start_timer(l_timer);
        copy_privileges(
          io_source_table_rec => io_source_table_rec,
          io_target_table_rec => io_target_table_rec,
          io_frwd_stmt_arr    => l_frwd_stmt_arr,
          io_rlbk_stmt_arr    => l_rlbk_stmt_arr
        );
        stop_timer(l_timer, 'Copy privileges');
      END IF;
      IF g_params.keep_privs THEN
        start_timer(l_timer);
        cort_comp_pkg.copy_policies(
          in_source_table_rec      => io_source_table_rec,
          in_target_table_rec      => io_target_table_rec,
          io_frwd_alter_stmt_arr   => l_frwd_stmt_arr,
          io_rlbk_alter_stmt_arr   => l_rlbk_stmt_arr
        );
        stop_timer(l_timer, 'Copy policies');
      END IF;
      IF g_params.keep_refs THEN
        start_timer(l_timer);
        cort_comp_pkg.copy_references(
          in_source_table_rec      => io_source_table_rec,
          io_target_table_rec      => io_target_table_rec,
          io_frwd_alter_stmt_arr   => l_frwd_stmt_arr,
          io_rlbk_alter_stmt_arr   => l_rlbk_stmt_arr
        );
        stop_timer(l_timer, 'Copy references');
      END IF;
      cort_comp_pkg.drop_triggers(
        in_table_rec             => io_source_table_rec,
        io_frwd_alter_stmt_arr   => l_frwd_stmt_arr,
        io_rlbk_alter_stmt_arr   => l_rlbk_stmt_arr
      );
      IF g_params.keep_triggers THEN
        start_timer(l_timer);
        cort_comp_pkg.copy_triggers(
          in_source_table_rec      => io_source_table_rec,
          io_target_table_rec      => io_target_table_rec,
          io_frwd_alter_stmt_arr   => l_frwd_stmt_arr,
          io_rlbk_alter_stmt_arr   => l_rlbk_stmt_arr
        );
        stop_timer(l_timer, 'Copy triggers');
      END IF;  

      -- move depending objects ignoring errors
      apply_changes_ignore_errors(
        io_stmt_arr => l_frwd_stmt_arr,
        in_test     => g_params.test
      );
      
      -- execute immediate
      IF g_params.keep_stats THEN
        start_timer(l_timer);
        copy_stats(
          in_source_table_rec => io_source_table_rec,
          in_target_table_rec => io_target_table_rec
        );
        stop_timer(l_timer, 'Copy stats');
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        debug('Error. Rolling back...');
        g_error_stack := g_error_stack||dbms_utility.format_error_backtrace||CHR(10);
        IF NOT g_params.keep_temp_table THEN
          exec_drop_table(    
            in_table_name => io_target_table_rec.owner,
            in_owner      => io_target_table_rec.rename_rec.current_name
          );
        END IF;  
        RAISE;
    END;
    
    FOR i IN 1..l_frwd_stmt_arr.COUNT LOOP
      IF l_frwd_stmt_arr.EXISTS(i) THEN
        io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_frwd_stmt_arr(i);
      END IF;   
    END LOOP;
    l_frwd_stmt_arr.DELETE;
    l_rlbk_stmt_arr.DELETE;
    
    -- if in build
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') IS NOT NULL THEN
      cort_comp_pkg.drop_triggers(
        in_table_rec             => io_source_table_rec,
        io_frwd_alter_stmt_arr   => l_frwd_stmt_arr,
        io_rlbk_alter_stmt_arr   => l_rlbk_stmt_arr
      );
      -- register state for all existing triggers on current table
      FOR i IN 1..io_source_table_rec.trigger_arr.COUNT LOOP
        cort_aux_pkg.register_change(
          in_object_owner  => io_source_table_rec.trigger_arr(i).owner,
          in_object_name   => io_source_table_rec.trigger_arr(i).trigger_name,
          in_object_type   => 'TRIGGER',
          in_sid           => in_sid,
          in_sql           => NULL,
          in_change_type   => cort_comp_pkg.gc_result_recreate,
          in_revert_name   => NULL,
          in_frwd_stmt_arr => l_frwd_stmt_arr,
          in_rlbk_stmt_arr => l_rlbk_stmt_arr
        );
      END LOOP;
    END IF;

    l_frwd_stmt_arr.DELETE;
    l_rlbk_stmt_arr.DELETE;

    -- Final 2 steps - renaming
    -- Clean up forward changes queue but keep rollback queue to rollback all changes from previous step 
    
    -- rename existing (old) table and all depending objects and log groups to cort_name
    rename_table_cascade(
      in_table_rec     => io_source_table_rec, 
      in_rename_mode   => 'TO_CORT',
      io_frwd_stmt_arr => l_frwd_stmt_arr,
      io_rlbk_stmt_arr => l_rlbk_stmt_arr
    );

    FOR i IN 1..l_frwd_stmt_arr.COUNT LOOP
      io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_frwd_stmt_arr(i); 
      io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := l_rlbk_stmt_arr(i);
    END LOOP;
    -- remember number of elements;
    l_cnt := l_frwd_stmt_arr.COUNT;

    -- rename new table and all depending objects to actual names
    rename_table_cascade(
      in_table_rec     => io_target_table_rec, 
      in_rename_mode   => 'TO_ORIGINAL',
      io_frwd_stmt_arr => l_frwd_stmt_arr,
      io_rlbk_stmt_arr => l_rlbk_stmt_arr
    );

    FOR i IN l_cnt+1..l_frwd_stmt_arr.COUNT LOOP
      io_frwd_stmt_arr(io_frwd_stmt_arr.COUNT+1) := l_frwd_stmt_arr(i); 
    END LOOP;
    io_rlbk_stmt_arr(io_rlbk_stmt_arr.COUNT+1) := get_drop_table_ddl(
                                                    in_table_name => io_target_table_rec.rename_rec.object_name,
                                                    in_owner      => io_target_table_rec.rename_rec.object_owner
                                                  );

    -- for CREATE AS SELECT FROM itself
    IF in_recreate_mode = cort_comp_pkg.gc_result_create_as_select THEN
      -- check if selecting from recreating table
      -- if yes source session will hold shared lock on this table and will not allow to rename the table.
      -- so last 2 steps need to be done after release trigger's lock.
      IF NOT g_params.test AND cort_parse_pkg.as_select_from(io_source_table_rec.table_name) THEN
        -- Just output DDL commands without execution 
        apply_changes(
          in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
          in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr,
          in_test                => TRUE
        );
        stop_timer(g_overall_timer, 'Overall');
        
        -- Terminate the waiting loop
        cort_job_pkg.success_job(
          in_sid => in_sid
        );
        -- Continue execution in the backgrouond
      END IF;
    END IF; 

    -- execute all changes in the queue
    BEGIN
      apply_changes(
        in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
        in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr,
        in_test                => g_params.test
      );
    EXCEPTION
      WHEN OTHERS THEN
        debug('Error. Rolling back...');
        IF NOT g_params.keep_temp_table THEN
          exec_drop_table(    
            in_table_name => io_target_table_rec.owner,
            in_owner      => io_target_table_rec.rename_rec.current_name
          );
        END IF;  
        g_error_stack := g_error_stack||dbms_utility.format_error_backtrace||CHR(10);
        RAISE;
    END;

  END recreate_table;
  
  -- Parses SQL syntax. Do not call for DDL!!! It executes DDL!
  FUNCTION is_sql_valid(in_sql IN CLOB)
  RETURN BOOLEAN
  AS
    l_sql_arr dbms_sql.varchar2a;
    l_cursor  NUMBER;
    l_result  BOOLEAN;
  BEGIN
    cort_aux_pkg.clob_to_varchar2a(in_sql, l_sql_arr);
    l_cursor := dbms_sql.open_cursor;
    BEGIN
      dbms_sql.parse(
        c             => l_cursor, 
        statement     => l_sql_arr,
        lb            => 1,
        ub            => l_sql_arr.COUNT, 
        lfflg         => FALSE, 
        language_flag => dbms_sql.native
      );
      l_result := TRUE;
    EXCEPTION
      WHEN OTHERS THEN
        l_result := FALSE;
        IF dbms_sql.is_open(l_cursor) THEN
          dbms_sql.close_cursor(l_cursor);
        END IF;  
    END;
    IF dbms_sql.is_open(l_cursor) THEN
      dbms_sql.close_cursor(l_cursor);
    END IF;
    RETURN l_result;  
  END is_sql_valid;
  
  -- Check for all columns cort values
  PROCEDURE check_cort_values(
    io_new_table_rec IN OUT NOCOPY gt_table_rec,
    in_old_table_rec IN gt_table_rec
  )
  AS
    l_sql    CLOB;
  BEGIN
    l_sql := NULL;
    FOR i IN 1..io_new_table_rec.column_arr.COUNT LOOP
      IF io_new_table_rec.column_arr(i).cort_values.COUNT > 0 THEN
        io_new_table_rec.column_arr(i).cort_value_indx := NULL;
        FOR j IN 1..io_new_table_rec.column_arr(i).cort_values.COUNT LOOP
          IF NVL(io_new_table_rec.column_arr(i).cort_values(j).release, '{NULL}') = NVL(g_params.release, '{NULL}') THEN
            l_sql := 'SELECT '||io_new_table_rec.column_arr(i).cort_values(j).expression||' FROM "'||in_old_table_rec.owner||'"."'||in_old_table_rec.rename_rec.current_name||'" a';
            IF is_sql_valid(l_sql) THEN
              io_new_table_rec.column_arr(i).cort_value_indx := j;
            ELSE  
              debug('invalid cort value : '||io_new_table_rec.column_arr(i).cort_values(j).expression);
              io_new_table_rec.column_arr(i).cort_value_indx := NULL;
            END IF;
            EXIT WHEN io_new_table_rec.column_arr(i).cort_value_indx IS NOT NULL;
          END IF;  
        END LOOP;
      END IF;
    END LOOP;
  END check_cort_values;

  
  -- find renaming columns and set new_column_name property for them  
  PROCEDURE find_renaming_columns(
    io_source_table_rec IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    in_target_table_rec IN cort_exec_pkg.gt_table_rec
  )
  AS
    l_cort_value  VARCHAR2(32767);
    l_alias       VARCHAR2(100);
    l_name1       VARCHAR2(4000);
    l_name2       VARCHAR2(4000);
    l_name3       VARCHAR2(4000);
    l_name4       VARCHAR2(4000);
    l_col_name    VARCHAR2(30);
    l_pos         PLS_INTEGER;
  BEGIN
    -- find renaming columns
    FOR i IN 1..in_target_table_rec.column_arr.COUNT LOOP
      IF in_target_table_rec.column_arr(i).cort_value_indx IS NOT NULL THEN
        l_cort_value := TRIM(in_target_table_rec.column_arr(i).cort_values(in_target_table_rec.column_arr(i).cort_value_indx).expression);

        l_name1 := NULL; 
        l_name2 := NULL;
        l_name3 := NULL;
        l_name4 := NULL;
        l_pos   := NULL;
        
        BEGIN
          dbms_utility.name_tokenize(l_cort_value, l_name1, l_name2, l_name3, l_name4, l_pos);
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
        
        IF l_pos = LENGTH(l_cort_value) THEN
          
          IF l_name1 = io_source_table_rec.owner AND
             l_name2 = io_source_table_rec.table_name 
          THEN
            l_col_name := l_name3;
          ELSIF l_name1 = io_source_table_rec.table_name OR 
                l_name1 = g_params.alias
          THEN
            l_col_name := l_name2;
          ELSE
            l_col_name := l_name1;
          END IF;

          IF io_source_table_rec.column_indx_arr.EXISTS(l_col_name) THEN
            l_pos := io_source_table_rec.column_indx_arr(l_col_name);
            io_source_table_rec.column_arr(l_pos).new_column_name := in_target_table_rec.column_arr(i).column_name;
            -- update reference by name
            io_source_table_rec.column_indx_arr.DELETE(l_col_name);
            io_source_table_rec.column_indx_arr(io_source_table_rec.column_arr(l_pos).new_column_name) := l_pos;
            debug('Column '||io_source_table_rec.column_arr(l_pos).column_name||': new column name = '||io_source_table_rec.column_arr(l_pos).new_column_name);
          END IF;
          
        END IF;    
      END IF;      
    END LOOP;
  END find_renaming_columns;  

  -- replace names for renaming column in expressions for virtual columns, index records, constraint records, partition records? 
  PROCEDURE update_refs_on_renamed_columns(
    io_table_rec IN OUT NOCOPY gt_table_rec
  )
  AS
    l_renamed     arrays.gt_str_indx;
    l_col_name    VARCHAR2(30);
    
    PROCEDURE update_column_arr(
      io_column_arr    IN OUT NOCOPY arrays.gt_str_arr,
      in_replace_names IN arrays.gt_str_indx
    )
    AS
      l_col_name    VARCHAR2(30);
    BEGIN
      FOR i IN 1..io_column_arr.COUNT LOOP 
        l_col_name := io_column_arr(i);
        IF in_replace_names.EXISTS(l_col_name) THEN
          io_column_arr(i) := in_replace_names(l_col_name);
        END IF;
      END LOOP;
    END update_column_arr;
    
  BEGIN
    FOR i IN 1..io_table_rec.column_arr.COUNT LOOP
      IF io_table_rec.column_arr(i).virtual_column = 'NO' AND
         io_table_rec.column_arr(i).hidden_column = 'NO' AND
         io_table_rec.column_arr(i).new_column_name IS NOT NULL 
      THEN
        l_renamed(io_table_rec.column_arr(i).column_name) := io_table_rec.column_arr(i).new_column_name;
      END IF;   
    END LOOP;
    
    update_column_arr(
      io_column_arr    => io_table_rec.iot_pk_column_arr,
      in_replace_names => l_renamed
    );
    update_column_arr(
      io_column_arr    => io_table_rec.part_key_column_arr,
      in_replace_names => l_renamed
    );
    update_column_arr(
      io_column_arr    => io_table_rec.subpart_key_column_arr,
      in_replace_names => l_renamed
    );
    
    -- update virtual column expressions
    FOR i IN 1..io_table_rec.column_arr.COUNT LOOP
      IF io_table_rec.column_arr(i).virtual_column = 'YES' THEN
        cort_parse_pkg.update_expression(
          io_expression    => io_table_rec.column_arr(i).data_default, 
          in_replace_names => l_renamed
        );
      END IF;   
    END LOOP;

    -- update constraint columns
    FOR i IN 1..io_table_rec.constraint_arr.COUNT LOOP
      update_column_arr(
        io_column_arr    => io_table_rec.constraint_arr(i).column_arr,
        in_replace_names => l_renamed
      );

      -- update ref columns for self-referencing FK constraints
      IF io_table_rec.constraint_arr(i).constraint_type = 'R' AND
         io_table_rec.constraint_arr(i).r_owner = io_table_rec.owner AND
         io_table_rec.constraint_arr(i).r_table_name = io_table_rec.table_name 
      THEN
        update_column_arr(
          io_column_arr    => io_table_rec.constraint_arr(i).r_column_arr,
          in_replace_names => l_renamed
        );
      END IF;   
    END LOOP;
    
    -- update reference constraint columns
    FOR i IN 1..io_table_rec.ref_constraint_arr.COUNT LOOP
      update_column_arr(
        io_column_arr    => io_table_rec.ref_constraint_arr(i).r_column_arr,
        in_replace_names => l_renamed
      );
    END LOOP;

    -- update index columns
    FOR i IN 1..io_table_rec.index_arr.COUNT LOOP
      update_column_arr(
        io_column_arr    => io_table_rec.index_arr(i).column_arr,
        in_replace_names => l_renamed
      );
      update_column_arr(
        io_column_arr    => io_table_rec.index_arr(i).part_key_column_arr,
        in_replace_names => l_renamed
      );
      update_column_arr(
        io_column_arr    => io_table_rec.index_arr(i).subpart_key_column_arr,
        in_replace_names => l_renamed
      );
      FOR j IN 1..io_table_rec.index_arr(i).column_expr_arr.COUNT LOOP
        IF io_table_rec.index_arr(i).column_expr_arr(j) IS NOT NULL THEN 
          cort_parse_pkg.update_expression(
            io_expression    => io_table_rec.index_arr(i).column_expr_arr(j), 
            in_replace_names => l_renamed
          );
        END IF;
      END LOOP;   
    END LOOP;

    -- update join-index columns
    FOR i IN 1..io_table_rec.join_index_arr.COUNT LOOP
      FOR j IN 1..io_table_rec.join_index_arr(i).join_inner_column_arr.COUNT LOOP
        IF io_table_rec.join_index_arr(i).join_inner_owner_arr(j) = io_table_rec.owner AND
           io_table_rec.join_index_arr(i).join_inner_table_arr(j) = io_table_rec.table_name 
        THEN     
          l_col_name := io_table_rec.index_arr(i).join_inner_column_arr(j);
          IF l_renamed.EXISTS(l_col_name) THEN
            io_table_rec.index_arr(i).join_inner_column_arr(j) := l_renamed(l_col_name); 
          END IF;
        END IF;
      END LOOP;   
      FOR j IN 1..io_table_rec.join_index_arr(i).join_outer_column_arr.COUNT LOOP
        IF io_table_rec.join_index_arr(i).join_outer_owner_arr(j) = io_table_rec.owner AND
           io_table_rec.join_index_arr(i).join_outer_table_arr(j) = io_table_rec.table_name 
        THEN     
          l_col_name := io_table_rec.index_arr(i).join_outer_column_arr(j);
          IF l_renamed.EXISTS(l_col_name) THEN
            io_table_rec.index_arr(i).join_outer_column_arr(j) := l_renamed(l_col_name); 
          END IF;
        END IF;
      END LOOP;   
    END LOOP;

    FOR i IN 1..io_table_rec.log_group_arr.COUNT LOOP
      FOR j IN 1..io_table_rec.log_group_arr(i).column_arr.COUNT LOOP
        l_col_name := io_table_rec.log_group_arr(i).column_arr(j);
        IF l_renamed.EXISTS(l_col_name) THEN
          io_table_rec.log_group_arr(i).column_arr(j) := l_renamed(l_col_name); 
        END IF;
      END LOOP;   
    END LOOP;  

  END update_refs_on_renamed_columns;

  -- compare tables
  FUNCTION comp_tables(
    io_source_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_target_table_rec     IN OUT NOCOPY cort_exec_pkg.gt_table_rec,
    io_frwd_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr, -- forward alter statements
    io_rlbk_alter_stmt_arr  IN OUT NOCOPY arrays.gt_clob_arr  -- rollback alter statements
  )
  RETURN PLS_INTEGER
  AS
    l_result               PLS_INTEGER;
    l_comp_result          PLS_INTEGER;
    l_stop_vaue            PLS_INTEGER;
  BEGIN
    l_result := cort_comp_pkg.gc_result_nochange;
    IF g_params.force_recreate THEN
      l_stop_vaue := cort_comp_pkg.gc_result_alter;
    ELSE
      l_stop_vaue := cort_comp_pkg.gc_result_exchange;
    END IF;
    
    -- If teher is at least one force value then always recreate table
    FOR i IN 1..io_target_table_rec.column_arr.COUNT LOOP
      IF io_target_table_rec.column_arr(i).cort_value_indx IS NOT NULL AND
         io_target_table_rec.column_arr(i).cort_values.EXISTS(io_target_table_rec.column_arr(i).cort_value_indx) 
      THEN
        IF io_target_table_rec.column_arr(i).cort_values(io_target_table_rec.column_arr(i).cort_value_indx).force_value THEN
          debug('There is force cort-value - recreate');
          l_result := cort_comp_pkg.gc_result_recreate;
          EXIT;
        END IF;  
      END IF;
    END LOOP;
    
    debug('Compare start - '||l_result);
    -- compare table attributes
    IF l_result < l_stop_vaue THEN
      -- compare main table attributes
      l_result := cort_comp_pkg.comp_tables(
                    in_source_table_rec    => io_source_table_rec,
                    in_target_table_rec    => io_target_table_rec,
                    io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                    io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                  );
      debug('Compare table attributes - '||l_result);
    END IF;

    -- if table attributes are the same then compare columns
    IF l_result < l_stop_vaue THEN
      -- compare columns
      l_comp_result := cort_comp_pkg.comp_table_columns(
                         io_source_table_rec    => io_source_table_rec,
                         io_target_table_rec    => io_target_table_rec,
                         io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                         io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                       );
      debug('Compare columns - '||l_comp_result);
      l_result := GREATEST(l_result, l_comp_result);
       
      -- update column names - replace name for smart renamed columns 
      update_refs_on_renamed_columns(
        io_table_rec    => io_source_table_rec
      );
      -- compares column name dependent table attributes 
      l_comp_result := cort_comp_pkg.comp_table_col_attrs(
                         in_source_table_rec => io_source_table_rec,
                         in_target_table_rec => io_target_table_rec
                       );
      debug('Compare col attributes - '||l_comp_result);
      l_result := GREATEST(l_result, l_comp_result);
    END IF;

    -- constraints
    IF l_result < l_stop_vaue THEN

      l_comp_result := cort_comp_pkg.comp_constraints(
                         io_source_table_rec      => io_source_table_rec,
                         io_target_table_rec      => io_target_table_rec,
                         io_frwd_alter_stmt_arr   => io_frwd_alter_stmt_arr,
                         io_rlbk_alter_stmt_arr   => io_rlbk_alter_stmt_arr
                       );
      debug('Compare constraints - '||l_comp_result);
      l_result := GREATEST(l_result, l_comp_result);
    END IF;
    -- logging groups
    IF l_result < l_stop_vaue THEN

      l_comp_result := cort_comp_pkg.comp_log_groups(
                         io_source_table_rec      => io_source_table_rec,
                         io_target_table_rec      => io_target_table_rec,
                         io_frwd_alter_stmt_arr   => io_frwd_alter_stmt_arr,
                         io_rlbk_alter_stmt_arr   => io_rlbk_alter_stmt_arr
                       );
      debug('Compare logging groups - '||l_comp_result);
      l_result := GREATEST(l_result, l_comp_result);
    END IF;
    -- lobs
    IF l_result < l_stop_vaue THEN

      l_comp_result := cort_comp_pkg.comp_lobs(
                         in_source_table_rec    => io_source_table_rec,
                         in_target_table_rec    => io_target_table_rec,
                         io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                         io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                       );
      debug('Compare lobs - '||l_comp_result);
      l_result := GREATEST(l_result, l_comp_result);
    END IF;
    -- xml columns
    IF l_result < l_stop_vaue THEN

      l_comp_result := cort_comp_pkg.comp_xml_columns(
                         in_source_table_rec    => io_source_table_rec,
                         in_target_table_rec    => io_target_table_rec,
                         io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                         io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                       );
      debug('Compare xml cols - '||l_comp_result);
      l_result := GREATEST(l_result, l_comp_result);
    END IF;
    -- xml columns
    IF l_result < l_stop_vaue THEN

      l_comp_result := cort_comp_pkg.comp_varray_columns(
                         in_source_table_rec    => io_source_table_rec,
                         in_target_table_rec    => io_target_table_rec,
                         io_frwd_alter_stmt_arr => io_frwd_alter_stmt_arr,
                         io_rlbk_alter_stmt_arr => io_rlbk_alter_stmt_arr
                       );
      debug('Compare varray cols - '||l_comp_result);
      l_result := GREATEST(l_result, l_comp_result);
    END IF;

    -- partitions
    IF l_result < l_stop_vaue AND
       io_source_table_rec.partitioned = 'YES' AND
       io_target_table_rec.partitioned = 'YES' AND
       NOT g_params.keep_partitions
    THEN
      l_comp_result := cort_comp_pkg.comp_partitions(
                         io_source_table_rec     => io_source_table_rec,
                         io_target_table_rec     => io_target_table_rec,
                         io_source_partition_arr => io_source_table_rec.partition_arr,
                         io_target_partition_arr => io_target_table_rec.partition_arr,
                         in_partition_level      => 'PARTITION',
                         io_frwd_alter_stmt_arr  => io_frwd_alter_stmt_arr,
                         io_rlbk_alter_stmt_arr  => io_rlbk_alter_stmt_arr
                       );
      debug('Compare partitions - '||l_comp_result);
      l_result := GREATEST(l_result, l_comp_result);

    END IF;

    IF l_result = cort_comp_pkg.gc_result_alter AND
       g_params.force_recreate
    THEN
      l_result := cort_comp_pkg.gc_result_recreate;
    END IF;

    debug('Compare result - '||l_result);
    RETURN l_result;
  END comp_tables;
  
  -- lock table in exclusive mode. Return TRUE if lock acquired. Return FALSE if table not found. Otherwise raise unhandled exception
  FUNCTION lock_table(
    in_table_name  IN VARCHAR2,
    in_owner       IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
    l_sql                  VARCHAR2(1000);
    l_result               BOOLEAN;
    e_table_not_found      EXCEPTION;
    e_resource_busy        EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_table_not_found, -942);
    PRAGMA EXCEPTION_INIT(e_resource_busy, -54);
  BEGIN
    -- obtain table lock
    l_sql := 'LOCK TABLE "'||in_owner||'"."'||in_table_name||'" IN EXCLUSIVE MODE NOWAIT';
    BEGIN
      execute_immediate(l_sql, NULL);
      l_result := TRUE;
    EXCEPTION
      WHEN e_table_not_found THEN
        l_result := FALSE;
      WHEN e_resource_busy THEN
        l_result := FALSE;
    END;
    RETURN l_result;  
  END lock_table;

  -- lock table in exclusive mode. Return TRUE if lock acquired. Return FALSE if table not found. Otherwise raise unhandled exception
  FUNCTION check_locked_table(
    in_table_name  IN VARCHAR2,
    in_owner       IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
    l_sql                  VARCHAR2(1000);
    l_result               BOOLEAN;
    e_table_not_found      EXCEPTION;
    e_resource_busy        EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_table_not_found, -942);
    PRAGMA EXCEPTION_INIT(e_resource_busy, -54);
  BEGIN
    -- obtain table lock
    l_sql := 'LOCK TABLE "'||in_owner||'"."'||in_table_name||'" IN EXCLUSIVE MODE NOWAIT';
    BEGIN
      execute_immediate(l_sql, NULL);
      l_result := TRUE;
    EXCEPTION
      WHEN e_table_not_found THEN
        l_result := FALSE;
      WHEN e_resource_busy THEN
        l_result := TRUE;
    END;
    RETURN l_result;  
  END check_locked_table;

  -- Public : create or replace table
  PROCEDURE create_or_replace_table(
    in_sid            IN VARCHAR2,
    in_schema_name    IN VARCHAR2,
    in_table_name     IN VARCHAR2,
    in_owner          IN VARCHAR2,
    in_sql            IN CLOB  
  )
  AS
    l_new_sql              CLOB;
    l_new_modified_sql     CLOB;
    l_test_sql             CLOB;
    l_test_modified_sql    CLOB;
    l_partitions_sql       CLOB;

    l_old_table_rec        gt_table_rec;
    l_new_table_rec        gt_table_rec;
    l_subpart_arr          gt_partition_arr;
    

    l_frwd_rename_stmt_arr arrays.gt_clob_arr;
    l_rlbk_rename_stmt_arr arrays.gt_clob_arr;
    l_frwd_stmt_arr        arrays.gt_clob_arr;
    l_rlbk_stmt_arr        arrays.gt_clob_arr;

    l_result               PLS_INTEGER;
    l_comp_result          PLS_INTEGER;
    l_schema               VARCHAR2(30);

    l_timer                PLS_INTEGER;
    l_xml                  XMLTYPE;
    
    l_create_as_select     BOOLEAN;
    l_partitioning_type    VARCHAR2(30);
    l_subpartitioning_type VARCHAR2(30);
    l_modified_part_flag   BOOLEAN;
    l_dummy_params         cort_params_pkg.gt_params_rec;
    l_current_schema       VARCHAR2(30);
    l_revert_name          VARCHAR2(30);
  BEGIN
    l_modified_part_flag := FALSE;
    l_current_schema := cort_parse_pkg.get_sql_schema_name;

    -- disable CORT trigger for current session
    cort_session_pkg.disable;

    -- try to obtain exclusive lock
    IF lock_table(
         in_table_name  => in_table_name, 
         in_owner       => in_owner 
       ) 
    THEN
      -- read table attributes
      read_table_cascade(
        in_table_name => in_table_name, 
        in_owner      => in_owner, 
        out_table_rec => l_old_table_rec
      );

      -- find all columns included into join indexes
      mark_join_columns(l_old_table_rec);
      
      -- convert table rec into XML
      cort_xml_pkg.write_to_xml(  
        in_value => l_old_table_rec,
        out_xml  => l_xml
      );
     
      -- log source table rec
      cort_log_pkg.log(
        in_log_type => 'OLD_TABLE_REC',
        in_text     => l_xml.getClobVal()
      );
      
      l_new_sql := in_sql;

      -- modify original SQL - replace original names with temp ones. 
      cort_parse_pkg.replace_names(
        in_table_rec => l_old_table_rec,
        io_sql       => l_new_sql 
      );
      
      -- parse modified sql
      cort_parse_pkg.initial_parse_sql(
        in_sql           => l_new_sql,
        in_operation     => 'CREATE',
        in_object_type   => 'TABLE',
        in_object_name   => l_old_table_rec.rename_rec.temp_name,
        in_object_owner  => in_owner,       
        io_params_rec    => l_dummy_params
      );

      -- find "AS SELECT" start position
      cort_parse_pkg.parse_as_select(
        out_as_select => l_create_as_select
      );
      
      $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=2 $THEN
        execute_immediate('ALTER SESSION SET DEFERRED_SEGMENT_CREATION=FALSE', NULL);
      $END

      IF l_current_schema IS NULL AND in_schema_name <> in_owner THEN
        execute_immediate('ALTER SESSION SET CURRENT_SCHEMA = "'||in_owner||'"', TRUE);
      END IF;  
     

      -- if "CREATE AS SELECT"  and test mode enabled 
      IF l_create_as_select AND g_params.test THEN
        -- save original SQL for display 
        l_test_sql := l_new_sql;
        
        -- modify sql: make subquery return 0 rows
        cort_parse_pkg.modify_as_select(
          io_sql => l_new_sql
        );
      END IF;

      IF l_old_table_rec.partitioned = 'YES' AND 
         g_params.keep_partitions 
      THEN
        -- parse partitioning types, positions
        cort_parse_pkg.parse_partitioning(
          out_partitioning_type    => l_partitioning_type,
          out_subpartitioning_type => l_subpartitioning_type
        );
        
        IF l_old_table_rec.partitioning_type = l_partitioning_type THEN

          IF l_old_table_rec.subpartitioning_type <> 'NONE' AND 
             g_params.keep_subpartitions AND
             l_old_table_rec.subpartitioning_type = l_subpartitioning_type  
          THEN 
            l_subpart_arr := l_old_table_rec.subpartition_arr;
          END IF;
          
          -- make a copy of original SQL (with replaced names)
          l_new_modified_sql := l_new_sql;
          
          -- generate SQL for partitions definitions from source table
          l_partitions_sql := cort_comp_pkg.get_partitions_sql(
                                in_partition_arr    => l_old_table_rec.partition_arr,
                                in_subpartition_arr => l_subpart_arr
                              );
          
          -- modify original SQL - replace partitions
          cort_parse_pkg.replace_partitions_sql(
            io_sql           => l_new_modified_sql,
            in_partition_sql => l_partitions_sql 
          );
          
          l_modified_part_flag := TRUE;
          
          IF l_create_as_select AND 
             g_params.test 
          THEN
            -- make a copy of original test SQL (with replaced names)
            l_test_modified_sql := l_test_sql;

            -- modify original test SQL - replace partitions
            cort_parse_pkg.replace_partitions_sql(
              io_sql           => l_test_modified_sql,
              in_partition_sql => l_partitions_sql
            );
          END IF; 

          -- Try to execute modified SQL
          BEGIN
            execute_immediate(
              in_sql  => l_new_modified_sql, 
              in_echo => FALSE
            );
            l_new_sql := l_new_modified_sql; 
            l_test_sql := l_test_modified_sql;
          EXCEPTION
            WHEN OTHERS THEN
              -- If modified SQL failed
              debug('SQL with replaced partitions is not valid: '||l_new_sql);
              -- restore l_modified_part_flag to execute original SQL on next step 
              l_modified_part_flag := FALSE;
          END;

        END IF;  
        
      END IF;  
      
      -- if SQL was not replaced with partitions or modified SQL failed
      IF NOT l_modified_part_flag THEN
        -- execute original sql with replaces names
        execute_immediate(
          in_sql  => l_new_sql, 
          in_echo => FALSE
        );
      END IF;    

      BEGIN
        -- if AS SELECT and test mode 
        IF l_create_as_select AND g_params.test THEN
          -- replace executed SQL with SQL for display in execution stack array 
          IF g_ddl_arr.COUNT > 0 THEN 
            g_ddl_arr(g_ddl_arr.COUNT) := l_test_sql;
          END IF;  
        END IF;

        -- read new table attributes
        read_table_cascade(
          in_table_name => l_old_table_rec.rename_rec.temp_name, 
          in_owner      => in_owner, 
          out_table_rec => l_new_table_rec
        );
          
        -- assign column cort_values
        cort_parse_pkg.parse_columns(
          io_table_rec  => l_new_table_rec
        );

        -- find valid cort values
        check_cort_values(
          io_new_table_rec => l_new_table_rec,
          in_old_table_rec => l_old_table_rec
        );
          
        -- convert target table recinto XML
        cort_xml_pkg.write_to_xml(  
          in_value => l_new_table_rec,
          out_xml  => l_xml
        );

        -- log target table rec
        cort_log_pkg.log(
          in_log_type => 'NEW_TABLE_REC',
          in_text     => l_xml.getClobVal()
        );

        -- mark all check constraints implicitly created from NOT NULL column constraint
        mark_notnull_constraints(
          io_constraint_arr => l_old_table_rec.constraint_arr,
          io_column_arr     => l_old_table_rec.column_arr
        );
        --
        mark_notnull_constraints(
          io_constraint_arr => l_new_table_rec.constraint_arr,
          io_column_arr     => l_new_table_rec.column_arr
        );

        -- find columns to rename
        find_renaming_columns(
          io_source_table_rec    => l_old_table_rec,
          in_target_table_rec    => l_new_table_rec
        );

        -- update column names - replace name for renamed columns with cort values 
        update_refs_on_renamed_columns(
          io_table_rec    => l_old_table_rec
        );

        IF l_create_as_select THEN
          l_result := cort_comp_pkg.gc_result_create_as_select;
        ELSE
          start_timer(l_timer);
          -- compare tables
          l_result := comp_tables(
                        io_source_table_rec    => l_old_table_rec,
                        io_target_table_rec    => l_new_table_rec,
                        io_frwd_alter_stmt_arr => l_frwd_stmt_arr,
                        io_rlbk_alter_stmt_arr => l_rlbk_stmt_arr
                      );
          stop_timer(l_timer, 'Compare');
        END IF;  

      EXCEPTION
        WHEN OTHERS THEN
          debug('Error. Rolling back...');
          g_error_stack := g_error_stack||dbms_utility.format_error_backtrace||CHR(10);
          -- drop temp table
          exec_drop_table(
            in_table_name => l_new_table_rec.rename_rec.current_name, 
            in_owner      => l_new_table_rec.owner,
            in_echo       => NULL
          );
          RAISE;
      END;

      BEGIN
        CASE
        WHEN l_result = cort_comp_pkg.gc_result_nochange THEN
          -- drop temp table
          IF NOT g_params.keep_temp_table THEN
            exec_drop_table(
              in_table_name => l_new_table_rec.rename_rec.current_name, 
              in_owner      => l_new_table_rec.owner,
              in_echo       => NULL
            );
          END IF;  

          debug('Do nothing ...');

        WHEN l_result IN (cort_comp_pkg.gc_result_alter, cort_comp_pkg.gc_result_alter_move) THEN
          start_timer(l_timer);
          -- drop temp table
          IF NOT g_params.keep_temp_table THEN
            exec_drop_table(
              in_table_name => l_new_table_rec.rename_rec.current_name, 
              in_owner      => l_new_table_rec.owner,
              in_echo       => NULL
            );
          END IF;  

          debug('Do alters ...');
          
          -- altering existing table
          alter_table(
            in_table_rec           => l_old_table_rec,
            io_frwd_alter_stmt_arr => l_frwd_stmt_arr,
            io_rlbk_alter_stmt_arr => l_rlbk_stmt_arr
          );
          
          l_revert_name := NULL;
          
          stop_timer(l_timer, 'Alter');

        WHEN l_result IN (cort_comp_pkg.gc_result_exchange, cort_comp_pkg.gc_result_recreate, cort_comp_pkg.gc_result_create_as_select) THEN
          start_timer(l_timer);

          debug('Do recreate ...');

          -- drop previous revert table
          IF l_result IN (cort_comp_pkg.gc_result_recreate, cort_comp_pkg.gc_result_create_as_select) AND
             NOT g_params.test 
          THEN
            l_revert_name := cort_aux_pkg.get_last_revert_name(
                                in_object_owner => in_owner,
                                in_object_type  => 'TABLE',
                                in_object_name  => in_table_name
                              );
            exec_drop_table(
              in_table_name => l_revert_name, 
              in_owner      => in_owner,
              in_echo       => TRUE
            );
          END IF;

          print_ddl;
          
          IF l_result = cort_comp_pkg.gc_result_exchange THEN
            -- altering existing table
            alter_table(
              in_table_rec           => l_old_table_rec,
              io_frwd_alter_stmt_arr => l_frwd_stmt_arr,
              io_rlbk_alter_stmt_arr => l_rlbk_stmt_arr
            );
          ELSE
            l_frwd_stmt_arr.DELETE;
            l_rlbk_stmt_arr.DELETE;
          END IF;

          l_frwd_stmt_arr(l_frwd_stmt_arr.COUNT+1) := l_new_sql;
          l_rlbk_stmt_arr(l_rlbk_stmt_arr.COUNT+1) := get_drop_table_ddl(
                                                        in_table_name => l_old_table_rec.rename_rec.temp_name,
                                                        in_owner      => l_old_table_rec.rename_rec.object_owner
                                                      );

          -- Move data and depending objects and rename temp and original tables
          recreate_table(
            io_source_table_rec => l_old_table_rec,
            io_target_table_rec => l_new_table_rec,
            io_frwd_stmt_arr    => l_frwd_stmt_arr,
            io_rlbk_stmt_arr    => l_rlbk_stmt_arr,
            in_sid              => in_sid,
            in_recreate_mode    => l_result
          );
          
          l_revert_name := l_old_table_rec.rename_rec.cort_name;
          
          -- register change
          IF g_params.test THEN
            -- drop temp table created in TEST mode 
            IF NOT g_params.keep_temp_table THEN
              exec_drop_table(
                in_table_name => l_new_table_rec.rename_rec.current_name, 
                in_owner      => l_new_table_rec.owner,
                in_echo       => NULL
              );
            END IF;
          END IF;
          
          stop_timer(l_timer, 'Recreate');
        END CASE;

        IF NOT g_params.test THEN
          cort_aux_pkg.register_change(
            in_object_owner  => in_owner,
            in_object_type   => 'TABLE',
            in_object_name   => in_table_name,
            in_sid           => in_sid,
            in_sql           => in_sql,
            in_change_type   => l_result,
            in_revert_name   => l_revert_name,
            in_frwd_stmt_arr => l_frwd_stmt_arr, 
            in_rlbk_stmt_arr => l_rlbk_stmt_arr
          );
        END IF;

      EXCEPTION
        WHEN OTHERS THEN 
          g_error_stack := g_error_stack||dbms_utility.format_error_backtrace||CHR(10);
          -- drop temp table 
          IF NOT g_params.keep_temp_table THEN
            exec_drop_table(
              in_table_name => l_new_table_rec.rename_rec.current_name, 
              in_owner      => l_new_table_rec.owner,
              in_echo       => NULL
            );
          END IF;
          RAISE;  
      END;
    
    ELSE
      -- Table does not exist.
      -- Just execute SQL as is
      l_frwd_stmt_arr(1) := in_sql;
      l_rlbk_stmt_arr(1) := get_drop_table_ddl(
                              in_table_name => in_table_name,
                              in_owner      => in_owner
                            );

      -- table doesn't exist. Create it as is.
      apply_changes(
        in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
        in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr, 
        in_test                => g_params.test, 
        in_echo                => TRUE
      );
      
      IF NOT g_params.test THEN
        -- delete any previous records for given object
        cleanup_history(
          in_object_owner  => in_owner,
          in_object_type   => 'TABLE',
          in_object_name   => in_table_name
        );
        -- register table
        cort_aux_pkg.register_change(
          in_object_owner  => in_owner,
          in_object_type   => 'TABLE',
          in_object_name   => in_table_name,
          in_sid           => in_sid,
          in_sql           => in_sql,
          in_change_type   => cort_comp_pkg.gc_result_create,
          in_revert_name   => NULL,
          in_frwd_stmt_arr => l_frwd_stmt_arr, 
          in_rlbk_stmt_arr => l_rlbk_stmt_arr
        );
      END IF;
    END IF;
    
    cort_session_pkg.enable;

    COMMIT; -- release table lock 
  END create_or_replace_table;

  -- create temp table as copy of given table to create indexes
  PROCEDURE create_index_temp_table(
    in_sid            IN VARCHAR2,
    in_table_name     IN VARCHAR2,
    in_owner          IN VARCHAR2
  )
  AS
    l_old_table_rec     gt_table_rec;
    l_new_table_rec     gt_table_rec;
    l_old_table_xml     XMLTYPE;
    l_new_table_xml     XMLTYPE;
    l_create_table_sql  arrays.gt_clob_arr;
    l_drop_table_sql    arrays.gt_clob_arr;
    l_rec               cort_index_temp_tables%ROWTYPE;
  BEGIN
    -- try to obtain exclusive lock
    IF lock_table(
         in_table_name  => in_table_name, 
         in_owner       => in_owner 
       ) 
    THEN
      -- read table attributes
      read_table_cascade(
        in_table_name => in_table_name, 
        in_owner      => in_owner, 
        out_table_rec => l_old_table_rec
      );
      -- create temp table with the same structure
      cort_comp_pkg.create_clone_table_sql(
        in_table_rec       => l_old_table_rec,
        io_frwd_stmt_arr   => l_create_table_sql,
        io_rlbk_stmt_arr   => l_drop_table_sql
      );

      $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=2 $THEN
        execute_immediate('ALTER SESSION SET DEFERRED_SEGMENT_CREATION=FALSE', NULL);
      $END

      apply_changes(
        in_frwd_alter_stmt_arr => l_create_table_sql,
        in_rlbk_alter_stmt_arr => l_drop_table_sql,
        in_echo                => g_params.debug
      );
      -- read temp table 
      read_table(
        in_table_name => l_old_table_rec.rename_rec.temp_name, 
        in_owner      => in_owner, 
        out_table_rec => l_new_table_rec
      );

      -- Convert old_table_rec into XML
      cort_xml_pkg.write_to_xml(  
        in_value => l_old_table_rec,
        out_xml  => l_old_table_xml
      );
      -- Convert new_table_rec into XML
      cort_xml_pkg.write_to_xml(  
        in_value => l_new_table_rec,
        out_xml  => l_new_table_xml
      );
      
      l_rec := cort_aux_pkg.get_index_temp_table_rec(
                 in_table_name  => in_table_name,
                 in_table_owner => in_owner
               );
      IF l_rec.temp_table_name IS NOT NULL THEN
        exec_drop_table(
          in_table_name => l_rec.temp_table_name,
          in_owner      => in_owner,
          in_echo       => g_params.debug
        );
      END IF;         

      -- register index temp table 
      cort_aux_pkg.register_index_temp_table(
        in_sid             => in_sid,
        in_table_name      => in_table_name,
        in_table_owner     => in_owner,
        in_temp_table_name => l_old_table_rec.rename_rec.temp_name,
        in_old_table_xml   => l_old_table_xml,
        in_new_table_xml   => l_new_table_xml
      );

      -- release table lock  
      COMMIT;
    ELSE
      raise_error('Could not obtain exclusive lock on table "'||in_owner||'"."'||in_table_name||'"');
    END IF;
  END create_index_temp_table;

  -- create temp copy of index on temp table
  PROCEDURE create_or_replace_index(
    in_sid            IN VARCHAR2,
    in_schema_name    IN VARCHAR2,
    in_index_name     IN VARCHAR2,
    in_owner          IN VARCHAR2,
    in_sql            IN CLOB
  )
  AS
    l_object_type         VARCHAR2(30);
    l_table_owner         VARCHAR2(30);
    l_table_name          VARCHAR2(30);
    l_cnt                 PLS_INTEGER;
    l_old_table_rec       gt_table_rec;
    l_new_table_rec       gt_table_rec;
    l_index_rec           gt_index_rec;
    l_temp_name           VARCHAR2(30);
    l_index_full_name     VARCHAR2(65);
    l_new_sql             CLOB;
    l_rec                 cort_index_temp_tables%ROWTYPE;
    l_xml                 XMLTYPE;
  BEGIN
    cort_parse_pkg.get_index_main_object(
      out_object_type  => l_object_type,
      out_object_owner => l_table_owner,
      out_object_name  => l_table_name
    );    
    
    -- if tablse schema name is not specified then use current schema
    l_table_owner := NVL(l_table_owner,in_schema_name);

    -- If index is not the first in cort-transaction and
    -- it is defined on different table then raise an exception  
    IF l_table_owner <> cort_trg_pkg.get_context('TABLE_OWNER') OR
       l_table_name  <> cort_trg_pkg.get_context('TABLE_NAME')
    THEN
      raise_error(-20001, 'Index must be defined on table "'||cort_trg_pkg.get_context('TABLE_OWNER')||'"."'||cort_trg_pkg.get_context('TABLE_NAME')||'"');
    END IF;       
    
    -- cort schema must have access to all indexes
    BEGIN
      SELECT 1
        INTO l_cnt
        FROM all_constraints
       WHERE owner = l_table_owner
         AND table_name = l_table_name
         AND index_owner = in_owner
         AND index_name = in_index_name
         AND constraint_type IN ('P','U');
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_cnt := 0;
    END;     

    IF l_cnt = 1 THEN
      raise_error(-20001, 'Index "'||in_owner||'"."'||in_index_name||'" is used for enforcement of unique/primary key');
    END IF;
    
    l_rec := cort_aux_pkg.get_index_temp_table_rec(
               in_table_name  => l_table_name,
               in_table_owner => l_table_owner
             );   
             
    cort_xml_pkg.read_from_xml(
      in_value => l_rec.old_table_rec,
      out_rec  => l_old_table_rec
    );
    cort_xml_pkg.read_from_xml(
      in_value => l_rec.new_table_rec,
      out_rec  => l_new_table_rec
    );
    
    -- Table should to be locked by parent session. 
    IF check_locked_table(
         in_table_name  => l_old_table_rec.table_name, 
         in_owner       => l_old_table_rec.owner 
       ) AND
    -- Lock temp table where we are creating temp index on   
       lock_table(
         in_table_name  => l_new_table_rec.table_name, 
         in_owner       => l_new_table_rec.owner 
       ) 
    THEN

      l_temp_name := get_object_temp_name(
                       in_object_type => 'INDEX', 
                       in_owner       => in_owner,
                       in_prefix      => gc_cort_temp_prefix
                     );

      l_index_rec.owner := in_owner;
      l_index_rec.index_name := in_index_name;
      l_index_rec.rename_rec.temp_name := l_temp_name;
      
      l_new_sql := in_sql;
      
      -- modify original SQL - replace original names with temp ones. 
      cort_parse_pkg.replace_names(
        in_table_rec       => l_old_table_rec,
        in_index_rec       => l_index_rec,
        io_sql             => l_new_sql 
      );

      $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=2 $THEN
        execute_immediate('ALTER SESSION SET DEFERRED_SEGMENT_CREATION=FALSE', NULL);
      $END

      IF in_schema_name IS NOT NULL THEN
        execute_immediate('ALTER SESSION SET CURRENT_SCHEMA = "'||in_schema_name||'"', NULL);
      END IF;  

      -- create temp index on temp table
      execute_immediate(l_new_sql, g_params.debug);
      
      -- read index metadata into record
      read_index(
        in_index_name => l_temp_name,
        in_owner      => in_owner,
        out_index_rec => l_index_rec
      );
      
      l_index_rec.rename_rec.object_owner := in_index_name;
      l_index_rec.rename_rec.current_name := l_temp_name;
      l_index_rec.table_name := l_table_name;
              
      l_index_full_name := '"'||l_index_rec.owner||'"."'||l_index_rec.index_name||'"';
      l_new_table_rec.index_indx_arr(l_index_full_name) := l_new_table_rec.index_arr.COUNT + 1;
      l_new_table_rec.index_arr(l_new_table_rec.index_arr.COUNT + 1) := l_index_rec;
      
      -- convert target table recinto XML
      cort_xml_pkg.write_to_xml(  
        in_value => l_new_table_rec,
        out_xml  => l_xml
      );
      
      -- write new table metradata into cort_index_temp_tables table 
      cort_aux_pkg.update_index_temp_table(
        in_table_name        => l_table_name,
        in_table_owner       => l_table_owner,
        in_new_table_rec     => l_xml
      );
    
      -- release table lock  
      COMMIT;
    ELSE
      raise_error( 'Could not obtain exclusive lock on table "'||l_old_table_rec.owner||'"."'||l_old_table_rec.table_name||'"');
    END IF;
  END create_or_replace_index;

  -- process all indexes SQL defined on given table
  PROCEDURE create_or_replace_indexes(
    in_sid            IN VARCHAR2,
    in_table_name     IN VARCHAR2,
    in_owner          IN VARCHAR2
  )
  AS
    l_old_table_rec     gt_table_rec;
    l_new_table_rec     gt_table_rec;
    l_new_index_rec     gt_index_rec;
    l_temp_name_arr     arrays.gt_str_arr;
    l_indx              PLS_INTEGER;
    l_comp_result       PLS_INTEGER;
    l_create_table_sql  arrays.gt_clob_arr;
    l_drop_table_sql    arrays.gt_clob_arr;
    l_frwd_stmt_arr     arrays.gt_clob_arr;
    l_rlbk_stmt_arr     arrays.gt_clob_arr;
    l_index_full_name   VARCHAR2(65);
    l_rec               cort_index_temp_tables%ROWTYPE;
  BEGIN
    -- try to obtain exclusive lock
    IF lock_table(
         in_table_name  => in_table_name, 
         in_owner       => in_owner 
       ) 
    THEN
      l_rec := cort_aux_pkg.get_index_temp_table_rec(
                 in_table_name  => in_table_name,
                 in_table_owner => in_owner
               );

      cort_xml_pkg.read_from_xml(
        in_value => l_rec.old_table_rec,
        out_rec  => l_old_table_rec
      );
      cort_xml_pkg.read_from_xml(
        in_value => l_rec.new_table_rec,
        out_rec  => l_new_table_rec
      );

      -- Start compare indexes
      cort_comp_pkg.comp_indexes(
        in_source_table_rec  => l_old_table_rec,
        in_target_table_rec  => l_new_table_rec,
        io_frwd_stmt_arr     => l_frwd_stmt_arr,
        io_rlbk_stmt_arr     => l_rlbk_stmt_arr
      );
        
      -- apply changes for all indexes 
      apply_changes(
        in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
        in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr,
        in_test                => g_params.test 
      );

      IF NOT g_params.test THEN
        -- register table indexes change as single change to rollback all changes at once.
        cort_aux_pkg.register_change(
          in_object_owner  => in_owner,
          in_object_type   => 'INDEXES',
          in_object_name   => in_table_name,
          in_sid           => in_sid,
          in_sql           => NULL,
          in_change_type   => cort_comp_pkg.gc_result_recreate,
          in_revert_name   => NULL,
          in_frwd_stmt_arr => l_frwd_stmt_arr, 
          in_rlbk_stmt_arr => l_rlbk_stmt_arr
        );
      END IF;

      IF l_rec.temp_table_name IS NOT NULL THEN
        cort_exec_pkg.exec_drop_table(
          in_table_name => l_rec.temp_table_name,
          in_owner      => in_owner,
          in_echo       => g_params.debug
        );
      END IF;
      
      cort_aux_pkg.unregister_index_temp_table(
        in_table_name      => in_table_name,
        in_table_owner     => in_owner
      );           

      -- release table lock  
      COMMIT;
    ELSE
      raise_error( 'Could not obtain exclusive lock on table "'||in_owner||'"."'||in_table_name||'"');
    END IF;
  END create_or_replace_indexes;

  -- Create or replace sequence
  PROCEDURE create_or_replace_sequence(
    in_sid            IN VARCHAR2,
    in_schema_name    IN VARCHAR2,
    in_sequence_name  IN VARCHAR2,
    in_owner          IN VARCHAR2,
    in_sql            IN CLOB  
  )
  AS
    l_new_sql              CLOB;
    l_current_schema       VARCHAR2(30);

    l_old_sequence_rec     gt_sequence_rec;
    l_new_sequence_rec     gt_sequence_rec;

    l_frwd_stmt_arr        arrays.gt_clob_arr;
    l_rlbk_stmt_arr        arrays.gt_clob_arr;

    l_result               PLS_INTEGER;
  BEGIN
    -- disable CORT trigger for current session
    cort_session_pkg.disable;

    -- read sequence attributes
    read_sequence(
      in_sequence_name => in_sequence_name, 
      in_owner         => in_owner, 
      out_sequence_rec => l_old_sequence_rec
    );
    IF l_old_sequence_rec.sequence_name IS NOT NULL THEN

      l_current_schema := cort_parse_pkg.get_sql_schema_name;

      read_privileges(
        in_owner         => l_old_sequence_rec.owner,
        in_table_name    => l_old_sequence_rec.sequence_name,
        io_privilege_arr => l_old_sequence_rec.privilege_arr
      );
        
      l_new_sql := in_sql;

      -- modify original SQL - replace original names with temp ones. 
      cort_parse_pkg.replace_names(
        in_sequence_rec => l_old_sequence_rec,
        io_sql          => l_new_sql 
      );
      
      IF l_current_schema IS NULL AND in_schema_name <> in_owner THEN
        execute_immediate('ALTER SESSION SET CURRENT_SCHEMA = "'||in_owner||'"', TRUE);
      END IF;  

      -- execute original sql with replaces names
      execute_immediate(l_new_sql, FALSE);
      
      l_result := cort_comp_pkg.gc_result_nochange;
      
      BEGIN
        -- read new sequence attributes
        read_sequence(
          in_sequence_name => l_old_sequence_rec.rename_rec.temp_name, 
          in_owner         => in_owner, 
          out_sequence_rec => l_new_sequence_rec
        );

        -- compare sequences
        l_result := cort_comp_pkg.comp_sequences(
                      in_source_sequence_rec => l_old_sequence_rec,
                      in_target_sequence_rec => l_new_sequence_rec,
                      io_frwd_alter_stmt_arr => l_frwd_stmt_arr,
                      io_rlbk_alter_stmt_arr => l_rlbk_stmt_arr
                    );
      EXCEPTION
        WHEN OTHERS THEN
          debug('Error. Rolling back...');
          g_error_stack := g_error_stack||dbms_utility.format_error_backtrace||CHR(10);
          -- drop temp sequence
          exec_drop_object(
            in_object_type  => 'SEQUENCE',
            in_object_name  => l_new_sequence_rec.rename_rec.current_name, 
            in_object_owner => l_new_sequence_rec.owner,
            in_echo         => NULL
          );
          RAISE;
      END;
      
      -- drop temp sequence
      exec_drop_object(
        in_object_type  => 'SEQUENCE',
        in_object_name  => l_new_sequence_rec.rename_rec.current_name, 
        in_object_owner => l_new_sequence_rec.owner,
        in_echo         => NULL
      );
      
      CASE
      WHEN l_result = cort_comp_pkg.gc_result_nochange THEN
        debug('Do nothing ...');
      WHEN l_result = cort_comp_pkg.gc_result_alter THEN
        -- drop temp sequence
        debug('Do alters ...');
        -- altering existing sequence

        apply_changes(
          in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
          in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr,
          in_test                => g_params.test 
        );
          
      WHEN l_result = cort_comp_pkg.gc_result_recreate THEN
        debug('Do recreate ...');

        l_frwd_stmt_arr.DELETE;
        l_rlbk_stmt_arr.DELETE;
        
        cort_comp_pkg.get_privileges_stmt(
          in_privilege_arr         => l_old_sequence_rec.privilege_arr,
          io_frwd_alter_stmt_arr   => l_rlbk_stmt_arr, 
          io_rlbk_alter_stmt_arr   => l_frwd_stmt_arr  -- GRANT privs for rollback
        );

        FOR i IN 1..l_frwd_stmt_arr.COUNT LOOP
          l_frwd_stmt_arr(i) := NULL;
        END LOOP;
        
        l_frwd_stmt_arr(l_frwd_stmt_arr.COUNT+1) := get_drop_object_ddl(
                                                      in_object_type  => 'SEQUENCE',
                                                      in_object_owner => l_old_sequence_rec.owner,
                                                      in_object_name  => l_old_sequence_rec.sequence_name
                                                    );
        l_rlbk_stmt_arr(l_rlbk_stmt_arr.COUNT+1) := cort_comp_pkg.get_sequence_sql(l_old_sequence_rec);

        l_frwd_stmt_arr(l_frwd_stmt_arr.COUNT+1) := in_sql;
        l_rlbk_stmt_arr(l_rlbk_stmt_arr.COUNT+1) := get_drop_object_ddl(
                                                      in_object_type  => 'SEQUENCE',
                                                      in_object_owner => l_old_sequence_rec.owner,
                                                      in_object_name  => l_old_sequence_rec.sequence_name
                                                    );

        IF g_params.keep_privs THEN
          cort_comp_pkg.get_privileges_stmt(
            in_privilege_arr         => l_old_sequence_rec.privilege_arr,
            io_frwd_alter_stmt_arr   => l_frwd_stmt_arr, 
            io_rlbk_alter_stmt_arr   => l_rlbk_stmt_arr
          );
        END IF;

        -- execute all changes in the queue
        BEGIN
          apply_changes(
            in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
            in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr,
            in_test                => g_params.test
          );
        EXCEPTION
          WHEN OTHERS THEN
            debug('Error. Rolling back...');
            g_error_stack := g_error_stack||dbms_utility.format_error_backtrace||CHR(10);
            RAISE;
        END;

      END CASE;

      IF NOT g_params.test THEN
        cort_aux_pkg.register_change(
          in_object_owner  => in_owner,
          in_object_type   => 'SEQUENCE',
          in_object_name   => in_sequence_name,
          in_sid           => in_sid,
          in_sql           => in_sql,
          in_change_type   => l_result,
          in_revert_name   => NULL,
          in_frwd_stmt_arr => l_frwd_stmt_arr, 
          in_rlbk_stmt_arr => l_rlbk_stmt_arr
        );
      END IF;

    ELSE
      -- Sequence does not exist.
      -- Just execute SQL as is
      l_frwd_stmt_arr(1) := in_sql;
      l_rlbk_stmt_arr(1) := get_drop_sequence_ddl(
                              in_sequence_name => in_sequence_name,
                              in_owner         => in_owner
                            );

      -- Sequence doesn't exist. Create it as is.
      apply_changes(
        in_frwd_alter_stmt_arr => l_frwd_stmt_arr,
        in_rlbk_alter_stmt_arr => l_rlbk_stmt_arr, 
        in_test                => g_params.test, 
        in_echo                => TRUE
      );
      
      IF NOT g_params.test THEN
        -- delete any previous records for given object
        cleanup_history(
          in_object_owner  => in_owner,
          in_object_type   => 'SEQUENCE',
          in_object_name   => in_sequence_name
        );
        -- register sequence
        cort_aux_pkg.register_change(
          in_object_owner  => in_owner,
          in_object_type   => 'SEQUENCE',
          in_object_name   => in_sequence_name,
          in_sid           => in_sid,
          in_sql           => in_sql,
          in_change_type   => cort_comp_pkg.gc_result_create,
          in_revert_name   => NULL,
          in_frwd_stmt_arr => l_frwd_stmt_arr, 
          in_rlbk_stmt_arr => l_rlbk_stmt_arr
        );
      END IF;
    END IF;
    cort_session_pkg.enable;
    
  END create_or_replace_sequence;

  -- Public: create or replace object
  PROCEDURE create_or_replace(
    in_sid           IN VARCHAR2,
    in_job_rec       IN cort_jobs%ROWTYPE,
    in_params_rec    IN cort_params_pkg.gt_params_rec
  ) 
  AS
    l_xml           XMLTYPE;
  BEGIN
    g_ddl_arr.DELETE;

    start_timer(g_overall_timer);
    g_params := in_params_rec;
    g_event_id := NULL;
    g_error_stack := NULL;
    
    -- parse original sql and cort-hints and obtain object params
    cort_parse_pkg.initial_parse_sql(
      in_sql           => in_job_rec.sql_text,
      in_operation     => 'CREATE',
      in_object_type   => in_job_rec.object_type,
      in_object_name   => in_job_rec.object_name,
      in_object_owner  => in_job_rec.object_owner,       
      io_params_rec    => g_params
    );

    -- ignore cort hints for all objects except tables and sequences
    IF in_job_rec.object_type NOT IN ('TABLE','SEQUENCE') THEN
      g_params := in_params_rec;
    END IF;
    
    -- Convert object params into XML
    cort_xml_pkg.write_to_xml(  
      in_value => g_params,
      out_xml  => l_xml
    );

    cort_log_pkg.log(
      in_log_type => 'INITIAL_PARSE_PARAMS',
      in_text     => l_xml.getClobVal()
    );
  
    IF g_params.test THEN
      output('== TEST OUTPUT ==');
    END IF;

    CASE in_job_rec.object_type
      WHEN 'TABLE' THEN
        create_or_replace_table(
          in_sid           => in_sid,
          in_schema_name   => in_job_rec.job_owner,
          in_table_name    => in_job_rec.object_name,
          in_owner         => in_job_rec.object_owner,
          in_sql           => in_job_rec.sql_text
        );
      WHEN 'INDEX' THEN
        create_or_replace_index(
          in_sid           => in_sid,
          in_schema_name   => in_job_rec.schema_name,
          in_index_name    => in_job_rec.object_name,
          in_owner         => in_job_rec.object_owner,
          in_sql           => in_job_rec.sql_text
        );
      WHEN 'INDEXES' THEN
        create_or_replace_indexes(
          in_sid           => in_sid,
          in_table_name    => in_job_rec.object_name,
          in_owner         => in_job_rec.object_owner
        ); 
      WHEN 'SEQUENCE' THEN
        create_or_replace_sequence(
          in_sid           => in_sid,
          in_schema_name   => in_job_rec.job_owner,
          in_sequence_name => in_job_rec.object_name,
          in_owner         => in_job_rec.object_owner,
          in_sql           => in_job_rec.sql_text
        ); 
      ELSE
        raise_error(-20001, 'Unsupported object type');
    END CASE;

    IF g_params.test THEN
      output('== END OF TEST OUTPUT ==');
    END IF;

    stop_timer(g_overall_timer, 'Overall');
  END create_or_replace;

  -- Add metadata of creating recreatable object
  PROCEDURE before_create_or_replace(
    in_sid           IN VARCHAR2,
    in_job_rec       IN cort_jobs%ROWTYPE,
    in_params_rec    IN cort_params_pkg.gt_params_rec
  )
  AS
    l_sql              CLOB;
    l_object_type      VARCHAR2(30);
    l_meta_object_type VARCHAR2(30);
    l_frwd_stmt_arr    arrays.gt_clob_arr;
    l_rlbk_stmt_arr    arrays.gt_clob_arr;
    l_exists           BOOLEAN;
    l_change_type      PLS_INTEGER;
  BEGIN
    g_ddl_arr.DELETE;

    start_timer(g_overall_timer);
    g_params := in_params_rec;
    g_event_id := NULL;
    g_error_stack := NULL;

    CASE 
    WHEN in_job_rec.object_type IN ('TABLE','SEQUENCE') THEN
      l_sql := 'DROP '||in_job_rec.object_type||' "'||in_job_rec.object_owner||'"."'||in_job_rec.object_name||'"';
      l_exists := FALSE;
    WHEN in_job_rec.object_type IN ('VIEW','PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY','TRIGGER','TYPE','TYPE BODY','LIBRARY','SYNONYM','JAVA','CONTEXT') THEN
      IF in_job_rec.object_type = 'JAVA' THEN 
        l_object_type := 'JAVA SOURCE';
      END IF;  

      CASE 
      WHEN l_object_type = 'PACKAGE' THEN 
        l_meta_object_type := 'PACKAGE_SPEC';
      WHEN l_object_type = 'TYPE' THEN 
        l_meta_object_type := 'TYPE_SPEC';
      ELSE
        l_meta_object_type := REPLACE(l_object_type, ' ', '_');
      END CASE;
      
      l_exists := object_exists(
                    in_object_type  => l_object_type, 
                    in_object_name  => in_job_rec.object_name, 
                    in_object_owner => NVL(in_job_rec.object_owner,'SYS')
                  );
      IF l_exists THEN
        BEGIN
          l_sql := dbms_metadata.get_ddl(l_meta_object_type, in_job_rec.object_name, in_job_rec.object_owner);
        EXCEPTION
          WHEN OTHERS THEN
            g_error_stack := g_error_stack||dbms_utility.format_error_backtrace||CHR(10);
            RAISE;
        END;
      ELSE
        IF l_object_type = 'SYNONYM' THEN
          IF in_job_rec.object_owner = 'PUBLIC' THEN
            l_sql := 'DROP PUBLIC SYNONYM "'||in_job_rec.object_name||'"';
          ELSE
            l_sql := 'DROP SYNONYM "'||in_job_rec.object_owner||'"."'||in_job_rec.object_name||'"';
          END IF;  
        ELSE
          IF in_job_rec.object_owner IS NOT NULL THEN
            l_sql := 'DROP '||l_object_type||' "'||in_job_rec.object_owner||'"."'||in_job_rec.object_name||'"';
          ELSE
            l_sql := 'DROP '||l_object_type||' "'||in_job_rec.object_name||'"';
          END IF;  
        END IF;  
        l_exists := FALSE;
      END IF;    
    ELSE 
      NULL;     
    END CASE;  
    l_frwd_stmt_arr(1) := in_job_rec.sql_text;
    l_rlbk_stmt_arr(1) := l_sql;
    
    IF NOT l_exists THEN     
      -- delete all previous data for given object (if any)
      cort_aux_pkg.cleanup_history(
        in_object_owner  => in_job_rec.object_owner,
        in_object_type   => in_job_rec.object_type,
        in_object_name   => in_job_rec.object_name
      );
      l_change_type := cort_comp_pkg.gc_result_create;
    ELSE
      l_change_type := cort_comp_pkg.gc_result_replace;
    END IF;
        
    cort_aux_pkg.register_change(
      in_object_owner  => in_job_rec.object_owner,
      in_object_name   => in_job_rec.object_name,
      in_object_type   => in_job_rec.object_type,
      in_sid           => in_sid,
      in_sql           => in_job_rec.sql_text,
      in_change_type   => l_change_type,
      in_revert_name   => NULL,
      in_frwd_stmt_arr => l_frwd_stmt_arr,
      in_rlbk_stmt_arr => l_rlbk_stmt_arr
    );
        
  END before_create_or_replace;

  -- Rollback the latest change for given object
  PROCEDURE revert_change(
    in_metadata_rec   IN cort_objects%ROWTYPE,
    in_params_rec     IN cort_params_pkg.gt_params_rec
  )
  AS
    l_rlbk_stmt_arr arrays.gt_clob_arr;
    l_xml           XMLTYPE;
  BEGIN
    g_params := in_params_rec;
    g_event_id := NULL;
    g_error_stack := NULL;

    IF in_metadata_rec.object_name IS NOT NULL THEN
    
      cort_xml_pkg.read_from_xml(  
        in_value => in_metadata_rec.revert_ddl,
        out_arr  => l_rlbk_stmt_arr
      );

      IF g_params.test THEN 
        output('reverting of '||in_metadata_rec.object_type||' "'||in_metadata_rec.object_owner||'"."'||in_metadata_rec.object_name||'"');
        output('== TEST OUTPUT ==');
      END IF;
        
      IF l_rlbk_stmt_arr.COUNT > 0 THEN 

        IF in_metadata_rec.revert_name IS NOT NULL AND
           NOT object_exists(
                 in_object_type  => in_metadata_rec.object_type, 
                 in_object_owner => in_metadata_rec.object_owner,
                 in_object_name  => in_metadata_rec.revert_name
               )
        THEN
          raise_error('Unable to revert of the last change for object "'||in_metadata_rec.object_owner||'"."'||in_metadata_rec.object_name||'" because backup object is not available');
        END IF;    

        apply_changes_ignore_errors(
          io_stmt_arr => l_rlbk_stmt_arr,
          in_test     => g_params.test
        );

      ELSE
        output('There is no revert information');
      END IF;

      IF NOT g_params.test THEN
        cort_aux_pkg.unregister_change(
          in_object_owner => in_metadata_rec.object_owner,
          in_object_type  => in_metadata_rec.object_type,
          in_object_name  => in_metadata_rec.object_name,
          in_exec_time    => in_metadata_rec.exec_time
        );
      END IF;

      IF g_params.test THEN 
        output('== END OF TEST OUTPUT ==');
      END IF;

    ELSE
      output('Object not found ');
    END IF;
             
  END revert_change;

END cort_exec_pkg;
/