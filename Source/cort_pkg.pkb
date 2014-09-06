CREATE OR REPLACE PACKAGE BODY cort_pkg
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
  Description: API for end-user - wrappers around main procedures/functions.  
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added build API
  ----------------------------------------------------------------------------------------------------------------------  
*/

  /* Private */
  PROCEDURE check_param_is_not_null(
    in_param IN VARCHAR2,
    in_value IN VARCHAR2
  )
  AS
  BEGIN
    IF in_value IS NULL THEN
      cort_exec_pkg.raise_error( 'Parameter '||in_param||' must have a value');
    END IF; 
  END check_param_is_not_null;

  PROCEDURE check_param_max_length(
    in_param   IN VARCHAR2,
    in_value   IN VARCHAR2,
    in_max_len IN NUMBER
  )
  AS
  BEGIN
    IF LENGTH(in_value) > in_max_len THEN
      cort_exec_pkg.raise_error( 'Parameter '||in_param||' is too long (longer '||in_max_len||' symbols)');
    END IF; 
  END check_param_max_length;

  PROCEDURE check_param_min_length(
    in_param   IN VARCHAR2,
    in_value   IN VARCHAR2,
    in_min_len IN NUMBER
  )
  AS
  BEGIN
    IF LENGTH(in_value) < in_min_len THEN
      cort_exec_pkg.raise_error( 'Parameter '||in_param||' must be longer '||in_min_len||' symbols');
    END IF; 
  END check_param_min_length;

  /* Public */

  -- getters for CORT default params
  FUNCTION get_param_default_value(
    in_param_name   IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN cort_params_pkg.get_param_default_value(in_param_name);
  END get_param_default_value;
  
  -- setters for modifying CORT default params permanently
  PROCEDURE set_param_default_value(
    in_param_name   IN VARCHAR2,
    in_param_value  IN NUMBER
  )
  AS
  BEGIN
    cort_params_pkg.set_param_default_value(in_param_name, NVL(TO_CHAR(in_param_value),'NULL'));
  END set_param_default_value;

  PROCEDURE set_param_default_value(
    in_param_name   IN VARCHAR2,
    in_param_value  IN VARCHAR2
  )
  AS
  BEGIN
    cort_params_pkg.set_param_default_value(in_param_name, CASE WHEN in_param_value IS NULL THEN 'NULL' ELSE ''''||in_param_value||'''' END);
  END set_param_default_value;
  
  PROCEDURE set_param_default_value(
    in_param_name   IN VARCHAR2,
    in_param_value  IN BOOLEAN
  )
  AS
  BEGIN
    cort_params_pkg.set_param_default_value(in_param_name, cort_params_pkg.bool_to_str(in_param_value));
  END set_param_default_value;

  -- Procedure is called from job
  PROCEDURE execute_action(
    in_sid IN VARCHAR2
  )
  AS
    l_rec        cort_jobs%ROWTYPE;
    l_params     cort_params_pkg.gt_params_rec;
  BEGIN
    dbms_output.enable(buffer_size => 1000000);

    l_rec := cort_job_pkg.get_job(
               in_sid => in_sid
             );
    dbms_application_info.set_client_info('CORT_BUILD='||l_rec.build);
    
    cort_xml_pkg.read_from_xml(l_rec.session_params, l_params); 
  
    BEGIN
      cort_trg_pkg.set_context(l_rec.action||'_'||l_rec.object_type,l_rec.object_name);
      cort_session_pkg.disable;        

      CASE l_rec.action 
      WHEN 'CREATE_OR_REPLACE' THEN
        cort_exec_pkg.create_or_replace(
          in_sid        => in_sid,
          in_job_rec    => l_rec,
          in_params_rec => l_params
        );
      WHEN 'EXPLAIN_PLAN_FOR' THEN
        cort_exec_pkg.create_or_replace(
          in_sid        => in_sid,
          in_job_rec    => l_rec,
          in_params_rec => l_params
        );
      WHEN 'REGISTER' THEN  
        cort_exec_pkg.before_create_or_replace(
          in_sid        => in_sid,
          in_job_rec    => l_rec,
          in_params_rec => l_params
        );
      END CASE;  

      cort_trg_pkg.set_context(l_rec.action||'_'||l_rec.object_type,NULL);
      cort_session_pkg.enable;        

      cort_job_pkg.success_job(
        in_sid => in_sid
      );
    EXCEPTION
      WHEN OTHERS THEN
        cort_trg_pkg.set_context(l_rec.action||'_'||l_rec.object_type,NULL);
        cort_session_pkg.enable;        
        cort_job_pkg.fail_job(
          in_sid             => in_sid,
          in_error_code      => sqlcode,
          in_error_message   => sqlerrm,
          in_error_backtrace => dbms_utility.format_error_backtrace,
          in_cort_stack      => cort_exec_pkg.get_error_stack
        );
        dbms_application_info.set_client_info(NULL);
        RAISE;
    END;
  END execute_action;

  -- permanently enable CORT
  PROCEDURE enable
  AS
  BEGIN
    cort_aux_pkg.enable_cort;
  END enable;
  
  -- permanently disable CORT
  PROCEDURE disable
  AS
  BEGIN
    cort_aux_pkg.disable_cort;
  END disable;

  -- get CORT status (ENABLED/DISABLED/DISABLED FOR SESSION)
  FUNCTION get_status
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN cort_aux_pkg.get_cort_status;
  END get_status;

  -- Rollback the latest change for given object (Private declaration)
  PROCEDURE revert_object(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_params_rec   IN cort_params_pkg.gt_params_rec 
  )
  AS
    l_metadata_rec cort_objects%ROWTYPE;
  BEGIN
    dbms_output.enable(buffer_size => 1000000);

    cort_log_pkg.init_log(
      in_sid          => dbms_session.unique_session_id,
      in_action       => 'REVERT',
      in_job_time     => SYSTIMESTAMP,
      in_object_type  => in_object_type,
      in_object_owner => in_object_owner,
      in_object_name  => in_object_name
    );
    
    cort_log_pkg.log(
      in_log_type  => 'REVERT',
      in_text      => 'Reverting of '||in_object_type||' "'||in_object_owner||'"."'||in_object_name||'"'
    );
  
    l_metadata_rec := cort_aux_pkg.get_last_change(
                        in_object_type  => in_object_type,
                        in_object_name  => in_object_name,
                        in_object_owner => in_object_owner
                      );
    
    cort_exec_pkg.revert_change(
      in_metadata_rec => l_metadata_rec,
      in_params_rec   => in_params_rec
    );
  END revert_object;

  -- Revert the latest change for given object (overloaded - Public declaration)
  PROCEDURE revert_object(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_echo         IN BOOLEAN  DEFAULT NULL,  -- NULL - take from session param 
    in_test         IN BOOLEAN  DEFAULT NULL   -- NULL - take from session param
  )
  AS   
    l_params   cort_params_pkg.gt_params_rec;
  BEGIN
    l_params := cort_session_pkg.get_params;
    IF in_echo IS NOT NULL THEN
      l_params.echo := in_echo;
    END IF;
    IF in_test IS NOT NULL THEN
      l_params.test := in_test;
    END IF;
    revert_object(
      in_object_type  => in_object_type,
      in_object_name  => in_object_name,
      in_object_owner => in_object_owner,
      in_params_rec   => l_params 
    );
  END revert_object;
  
  -- Wrapper for tables 
  PROCEDURE revert_table(
    in_table_name   IN VARCHAR2,
    in_table_owner  IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_echo         IN BOOLEAN  DEFAULT NULL,  -- NULL - take from session param 
    in_test         IN BOOLEAN  DEFAULT NULL   -- NULL - take from session param
  )
  AS
  BEGIN
    revert_object(
      in_object_type  => 'TABLE',
      in_object_name  => in_table_name,
      in_object_owner => in_table_owner,
      in_echo         => in_echo, 
      in_test         => in_test
    );
  END revert_table;

  -- drop object if it exists using current session params (in test mode just log and spool)
  PROCEDURE drop_object(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  )
  AS
    l_params   cort_params_pkg.gt_params_rec;
  BEGIN
    l_params := cort_session_pkg.get_params;
    cort_exec_pkg.g_params := l_params;
    cort_log_pkg.init_log(
      in_sid          => dbms_session.unique_session_id,
      in_action       => 'DROP',
      in_job_time     => SYSTIMESTAMP,
      in_object_type  => in_object_type,
      in_object_name  => in_object_name,
      in_object_owner => in_object_owner
    );
    cort_exec_pkg.exec_drop_object(
      in_object_type  => in_object_type,
      in_object_name  => in_object_name,
      in_object_owner => in_object_owner,
      in_echo         => TRUE,
      in_test         => l_params.test
    );
  END drop_object;
  
  -- Wrapper for tables 
  PROCEDURE drop_table(
    in_table_name   IN VARCHAR2,
    in_table_owner  IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  )
  AS
  BEGIN
    drop_object(
      in_object_type  => 'TABLE',
      in_object_name  => in_table_name,
      in_object_owner => in_table_owner
    );
  END drop_table;

  -- drop revert object if it exists
  PROCEDURE drop_revert_object(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  )
  AS
    l_revert_name VARCHAR2(30);
  BEGIN
    l_revert_name := cort_aux_pkg.get_last_revert_name(
                        in_object_type  => in_object_type,
                        in_object_name  => in_object_name,
                        in_object_owner => in_object_owner
                      );
    cort_exec_pkg.debug('revert object for '||in_object_type||' "'||in_object_owner||'"."'||in_object_name||'" is "'||l_revert_name||'"');
    IF l_revert_name IS NOT NULL THEN
      drop_object(
        in_object_type  => in_object_type,
        in_object_name  => l_revert_name,
        in_object_owner => in_object_owner
      );
    END IF;         
  END drop_revert_object;
  
  -- Warpper for tables 
  PROCEDURE drop_revert_table(
    in_table_name  IN VARCHAR2,
    in_table_owner IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  )
  AS
  BEGIN
    drop_revert_object(
      in_object_type  => 'TABLE',
      in_object_name  => in_table_name,
      in_object_owner => in_table_owner
    );         
  END drop_revert_table;
  
  -- disable all foreign keys on given table
  PROCEDURE disable_all_references(
    in_table_name IN VARCHAR2,
    in_owner      IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  )
  AS
    l_owner_arr            arrays.gt_str_arr; 
    l_table_name_arr       arrays.gt_str_arr; 
    l_constraint_name_arr  arrays.gt_str_arr;
    l_sql                  VARCHAR2(32767);
  BEGIN
    SELECT fk.owner, fk.table_name, fk.constraint_name
      BULK COLLECT
      INTO l_owner_arr, l_table_name_arr, l_constraint_name_arr 
      FROM all_constraints pk
     INNER JOIN all_constraints fk
        ON fk.r_constraint_name = pk.constraint_name
       AND fk.r_owner = pk.owner
       AND fk.status = 'ENABLED'
     WHERE pk.table_name = in_table_name
       AND pk.owner = in_owner
       AND pk.constraint_type in ('P','U');
       
    cort_exec_pkg.g_params := cort_session_pkg.get_params;
    FOR i IN 1..l_constraint_name_arr.COUNT LOOP
      cort_log_pkg.init_log(
        in_sid          => dbms_session.unique_session_id,
        in_action       => 'DISABLE_REFERENCES',
        in_job_time     => SYSTIMESTAMP,
        in_object_type  => 'TABLE',
        in_object_owner => in_owner,
        in_object_name  => in_table_name
      );
      l_sql := 'ALTER TABLE "'||l_owner_arr(i)||'"."'||l_table_name_arr(i)||'" DISABLE CONSTRAINT "'||l_constraint_name_arr(i)||'"';
      cort_exec_pkg.execute_immediate(
        in_sql  => l_sql,
        in_echo => TRUE,
        in_test => cort_exec_pkg.g_params.test
      );
    END LOOP;        
  END disable_all_references;  
  
  -- enable all foreign keys on given table
  PROCEDURE enable_all_references(
    in_table_name IN VARCHAR2,
    in_owner      IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_validate   IN BOOLEAN DEFAULT TRUE
  )
  AS
    l_owner_arr            arrays.gt_str_arr; 
    l_table_name_arr       arrays.gt_str_arr; 
    l_constraint_name_arr  arrays.gt_str_arr;
    l_sql                  VARCHAR2(32767);
    l_validate             VARCHAR2(50);
  BEGIN
    SELECT fk.owner, fk.table_name, fk.constraint_name
      BULK COLLECT
      INTO l_owner_arr, l_table_name_arr, l_constraint_name_arr 
      FROM all_constraints pk
     INNER JOIN all_constraints fk
        ON fk.r_constraint_name = pk.constraint_name
       AND fk.r_owner = pk.owner
       AND fk.status = 'DISABLED'
     WHERE pk.table_name = in_table_name
       AND pk.owner = in_owner
       AND pk.constraint_type in ('P','U');
       
    cort_exec_pkg.g_params := cort_session_pkg.get_params;
    FOR i IN 1..l_constraint_name_arr.COUNT LOOP
      cort_log_pkg.init_log(
        in_sid          => dbms_session.unique_session_id,
        in_action       => 'ENABLE_REFERENCES',
        in_job_time     => SYSTIMESTAMP,
        in_object_type  => 'TABLE',
        in_object_owner => in_owner,
        in_object_name  => in_table_name
      );
      IF in_validate THEN
        l_validate := 'VALIDATE';
      ELSE
        l_validate := 'NOVALIDATE';
      END IF;
      l_sql := 'ALTER TABLE "'||l_owner_arr(i)||'"."'||l_table_name_arr(i)||'" ENABLE '||l_validate||' CONSTRAINT "'||l_constraint_name_arr(i)||'"';
      cort_exec_pkg.execute_immediate(
        in_sql  => l_sql,
        in_echo => TRUE,
        in_test => cort_exec_pkg.g_params.test
      );
    END LOOP;        
  END enable_all_references;  

  --enable CORT-session for indexes
  PROCEDURE begin_index_definition(
    in_table_name IN VARCHAR2,
    in_owner      IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  )
  AS
    l_action       VARCHAR2(30) := 'CREATE_INDEX_TEMP';
    l_table_name   VARCHAR2(30);
    l_table_owner  VARCHAR2(30);
    l_sid          VARCHAR2(30);
  BEGIN
    dbms_output.enable(buffer_size => 1000000);

    l_table_name := cort_trg_pkg.get_context('TABLE_NAME');
    l_table_owner := cort_trg_pkg.get_context('TABLE_OWNER');
 
    IF l_table_name IS NULL AND 
       l_table_owner IS NULL 
    THEN 
      IF NOT cort_exec_pkg.object_exists(
               in_object_type  => 'TABLE',
               in_object_owner => in_owner,
               in_object_name  => in_table_name
             )
      THEN       
        cort_exec_pkg.raise_error( 'Table "'||in_owner||'"."'||in_table_name||'" does not exist');
      END IF;       

      l_sid := dbms_session.unique_session_id;

      cort_log_pkg.init_log(
        in_sid          => l_sid,
        in_action       => l_action,
        in_job_time     => SYSTIMESTAMP,
        in_object_type  => 'TABLE',
        in_object_owner => in_owner,
        in_object_name  => in_table_name
      );

      cort_exec_pkg.g_params := cort_session_pkg.get_params;

      cort_trg_pkg.set_context('CURRENT_SCHEMA', SYS_CONTEXT('USERENV','CURRENT_SCHEMA'));

      cort_exec_pkg.create_index_temp_table(
        in_sid            => l_sid,
        in_table_name     => in_table_name,
        in_owner          => in_owner
      );
        
      cort_trg_pkg.set_context('TABLE_NAME', in_table_name);
      cort_trg_pkg.set_context('TABLE_OWNER', in_owner);
      
      -- switch on CREATE OR REPLACE trigger for indexes in current session 
      cort_trg_pkg.set_context('CREATE_OR_REPLACE_INDEXES', 'ACTIVE');
    ELSE
      IF in_table_name <> l_table_name OR 
         in_owner  <> l_table_owner
      THEN
        cort_exec_pkg.raise_error( 'Indexes defition for table "'||l_table_owner||'"."'||l_table_name||'" is not completed');
      END IF;    
    END IF;  
  END begin_index_definition;
   
  --disable CORT-session for indexes and apply all changes
  PROCEDURE end_index_definition
  AS
    l_action       VARCHAR2(30) := 'CREATE_OR_REPLACE';
    l_table_name   VARCHAR2(30);
    l_table_owner  VARCHAR2(30);
    l_sid          VARCHAR2(30);
    l_rec          cort_jobs%ROWTYPE;
    l_params       cort_params_pkg.gt_params_rec;
  BEGIN
    -- switch off CREATE OR REPLACE trigger for indexes in current session 
    cort_trg_pkg.set_context('CREATE_OR_REPLACE_INDEXES', NULL);
    dbms_output.enable(buffer_size => 1000000);
    
    l_table_name := cort_trg_pkg.get_context('TABLE_NAME');
    l_table_owner := cort_trg_pkg.get_context('TABLE_OWNER');
 
    IF l_table_name IS NOT NULL AND 
       l_table_owner IS NOT NULL 
    THEN 
      l_sid := dbms_session.unique_session_id;
  
      cort_log_pkg.init_log(
        in_sid          => l_sid,
        in_action       => l_action,
        in_job_time     => SYSTIMESTAMP,
        in_object_type  => 'INDEXES',
        in_object_owner => l_table_owner,
        in_object_name  => l_table_name
      );

      l_params := cort_session_pkg.get_params;
      
      l_rec.schema_name := cort_trg_pkg.get_context('CURRENT_SCHEMA');
      l_rec.object_type := 'INDEXES';
      l_rec.object_name := l_table_name;
      l_rec.object_owner := l_table_owner;

     
      cort_exec_pkg.create_or_replace(
        in_sid        => l_sid,
        in_job_rec    => l_rec,
        in_params_rec => l_params
      );

      cort_trg_pkg.set_context('TABLE_NAME', NULL);
      cort_trg_pkg.set_context('TABLE_OWNER', NULL);
      cort_trg_pkg.set_context('CURRENT_SCHEMA', NULL);
    ELSE
      cort_exec_pkg.raise_error( 'Indexes defition is not started');
    END IF;  
  END end_index_definition;

  --cancel CORT-session for indexes without applying any changes
  PROCEDURE cancel_index_definition
  AS
    l_sid          VARCHAR2(30);
    l_action       VARCHAR2(30) := 'CANCEL_CREATE_OR_REPLACE';
    l_table_name   VARCHAR2(30);
    l_table_owner  VARCHAR2(30);
    l_rec          cort_index_temp_tables%ROWTYPE;
  BEGIN
    l_sid := dbms_session.unique_session_id;
    l_table_name := cort_trg_pkg.get_context('TABLE_NAME');
    l_table_owner := cort_trg_pkg.get_context('TABLE_OWNER');

    cort_trg_pkg.set_context('CREATE_OR_REPLACE_INDEXES', NULL);
    cort_trg_pkg.set_context('TABLE_NAME', NULL);
    cort_trg_pkg.set_context('TABLE_OWNER', NULL);
    cort_trg_pkg.set_context('CURRENT_SCHEMA', NULL);

    cort_log_pkg.init_log(
      in_sid          => l_sid,
      in_action       => l_action,
      in_job_time     => SYSTIMESTAMP,
      in_object_type  => 'INDEXES',
      in_object_owner => l_table_owner,
      in_object_name  => l_table_name
    );

    IF l_rec.temp_table_name IS NOT NULL THEN
      cort_exec_pkg.exec_drop_table(
        in_table_name => l_table_name,
        in_owner      => l_table_owner,
        in_echo       => NULL
      );
    END IF;
      
    cort_aux_pkg.unregister_index_temp_table(
      in_table_name      => l_table_name,
      in_table_owner     => l_table_owner
    );           
  END cancel_index_definition;
  
  PROCEDURE set_build_mode(in_enable IN BOOLEAN)
  AS
  BEGIN
    -- DISABLE restgoring references
    cort_session_pkg.set_param(
      in_param_name   => 'keep_refs',
      in_param_value  => NOT in_enable
    );
    -- DISABLE restgoring privileges
    cort_session_pkg.set_param(
      in_param_name   => 'keep_privs',
      in_param_value  => NOT in_enable
    );
    -- DISABLE restgoring indexes
    cort_session_pkg.set_param(
      in_param_name   => 'keep_indexes',
      in_param_value  => NOT in_enable
    );
    -- DISABLE restgoring keep_triggers
    cort_session_pkg.set_param(
      in_param_name   => 'keep_triggers',
      in_param_value  => NOT in_enable
    );
    -- DISABLE restgoring policies
    cort_session_pkg.set_param(
      in_param_name   => 'keep_policies',
      in_param_value  => NOT in_enable
    );
    -- DISABLE restgoring keep_comments
    cort_session_pkg.set_param(
      in_param_name   => 'keep_comments',
      in_param_value  => NOT in_enable
    );
  END set_build_mode;
  
  PROCEDURE drop_build_temp_objects(in_build IN VARCHAR2)
  AS
  BEGIN
    FOR x IN (SELECT * 
                FROM cort_objects
               WHERE build = in_build
                 AND revert_name IS NOT NULL
                 AND change_type <> cort_comp_pkg.gc_result_nochange
                 AND object_type = 'TABLE'
             )
    LOOP
      BEGIN
        drop_object(
          in_object_type  => x.object_type,
          in_object_owner => x.object_owner,
          in_object_name  => x.revert_name
        );
      EXCEPTION
        WHEN OTHERS THEN
          -- ignore errors and continue the loop
          -- error already captured and spooled 
          NULL;
      END;  
    END LOOP;         
  END drop_build_temp_objects;

  -- drop objects included into previous build but not installed whithin given build
  PROCEDURE drop_excluded_objects(
    in_application IN VARCHAR2
  )
  AS
    l_rec            cort_builds%ROWTYPE;
    l_prev_build     VARCHAR2(20);
  BEGIN
    l_rec := cort_aux_pkg.find_build(
               in_application => in_application
             );

    IF l_rec.status IN ('ACTIVE', 'STALE') THEN
      IF l_rec.status = 'ACTIVE' AND 
         cort_aux_pkg.is_build_active(l_rec.build)  
      THEN
        cort_exec_pkg.raise_error( 'This build is running in another session');
      END IF;

      l_prev_build := cort_aux_pkg.get_prev_build(
                        in_build       => l_rec.build, 
                        in_application => in_application
                      );  
      
      FOR x in (SELECT object_type, object_owner, object_name
                  FROM cort_objects o1
                 WHERE build = l_prev_build
                   AND NOT EXISTS(SELECT * 
                                    FROM cort_objects o2
                                   WHERE build = l_rec.build
                                     AND o2.object_type = o1.object_type
                                     AND o2.object_owner = o1.object_owner
                                     AND o2.object_name = o1.object_name
                                 )
                   AND object_type <> 'INDEXES'              
                 ORDER BY CASE WHEN object_type in ('TABLE','INDEX','SEQUENCE') THEN 2 ELSE 1 END                
               ) 
      LOOP
        BEGIN
          drop_object(
            in_object_type  => x.object_type,
            in_object_owner => x.object_owner,
            in_object_name  => x.object_name
          );
        EXCEPTION
          WHEN OTHERS THEN
            -- ignore errors and continue the loop
            -- error already captured and spooled 
            NULL;
        END;  
      END LOOP; 
    END IF;                
                                 
  END drop_excluded_objects;

  PROCEDURE start_build(
    in_application IN VARCHAR2,
    in_release     IN VARCHAR2  DEFAULT NULL
  )
  AS
    l_rec cort_builds%ROWTYPE;
  BEGIN
    check_param_is_not_null('in_application', in_application);
    check_param_max_length('in_application', in_application, 20);
    
    l_rec := cort_aux_pkg.find_build(
               in_application => in_application
             );
    IF l_rec.build IS NULL THEN
      l_rec.build := cort_aux_pkg.gen_build_id;
      l_rec.application := in_application;
      l_rec.release := in_release;
      l_rec.status := 'ACTIVE';
      l_rec.start_time := SYSDATE; 

      cort_aux_pkg.create_build(l_rec);

      cort_aux_pkg.start_build_session(l_rec.build);
      
      cort_session_pkg.set_param(
        in_param_name   => 'application',
        in_param_value  => l_rec.application
      );
      cort_session_pkg.set_param(
        in_param_name   => 'release',
        in_param_value  => l_rec.release
      );
     
      set_build_mode(TRUE);
    ELSE
      IF l_rec.status = 'ACTIVE' THEN
        IF cort_aux_pkg.is_build_active(l_rec.build) OR
           SYS_CONTEXT('USERENV', 'CLIENT_INFO') = 'CORT_BUILD='||l_rec.build
        THEN
          cort_exec_pkg.raise_error( 'Build for application '||in_application||' is already running. Use attach_to_build procedure to attach new session to existing build');
        ELSE
          l_rec.status := 'STALE';
          cort_aux_pkg.update_build(l_rec);
        END IF;
      END IF;
      
      IF l_rec.status = 'STALE' THEN
        cort_exec_pkg.raise_error( 'Build for application '||in_application||' is in "stale" status. Please revert build or resume build');
      END IF;
    END IF;         
  END start_build;

  -- Check is any of session (exception current one) linked to given build active  
  FUNCTION is_build_active(
    in_application IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
    l_rec cort_builds%ROWTYPE;
  BEGIN
    l_rec := cort_aux_pkg.find_build(
               in_application => in_application
             );
    IF l_rec.build IS NOT NULL THEN
      RETURN cort_aux_pkg.is_build_active(in_build => l_rec.build);
    ELSE
      RETURN FALSE;
    END IF;  
  END is_build_active;

  PROCEDURE attach_to_build(
    in_application IN VARCHAR2
  )
  AS
    l_rec cort_builds%ROWTYPE;
  BEGIN
    l_rec := cort_aux_pkg.find_build(
               in_application => in_application
             );

    IF l_rec.status = 'ACTIVE' THEN
      IF cort_aux_pkg.is_build_active(l_rec.build) THEN
        IF NVL(SYS_CONTEXT('USERENV', 'CLIENT_INFO'), 'NULL') <> 'CORT_BUILD='||l_rec.build THEN 
          -- attache session to the existing build
          cort_aux_pkg.start_build_session(l_rec.build);
      
          cort_session_pkg.set_param(
            in_param_name   => 'application',
            in_param_value  => l_rec.application
          );
          cort_session_pkg.set_param(
            in_param_name   => 'release',
            in_param_value  => l_rec.release
          );
         
          set_build_mode(TRUE);
        END IF;  
      ELSE
        l_rec.status := 'STALE';
        cort_aux_pkg.update_build(l_rec);
      END IF;
    END IF;
    
    IF l_rec.status <> 'ACTIVE' THEN
      cort_exec_pkg.raise_error( 'Build for application '||in_application||' is not running');
    END IF;      
  END attach_to_build;        

  -- detach current session from the build
  PROCEDURE detach_from_build(
    in_application IN VARCHAR2
  )
  AS
    l_rec cort_builds%ROWTYPE;
  BEGIN
    l_rec := cort_aux_pkg.find_build(
               in_application => in_application
             );

    IF l_rec.build IS NOT NULL THEN
      -- attache session to the existing build
      cort_aux_pkg.stop_build_session(l_rec.build);
      set_build_mode(FALSE);
    END IF;
  END detach_from_build;        

  PROCEDURE resume_build(
    in_application IN VARCHAR2
  )
  AS
    l_rec cort_builds%ROWTYPE;
  BEGIN
    l_rec := cort_aux_pkg.find_build(
               in_application => in_application
             );

    IF l_rec.status = 'ACTIVE' THEN
      IF NOT cort_aux_pkg.is_build_active(l_rec.build) AND 
         NVL(SYS_CONTEXT('USERENV', 'CLIENT_INFO'), 'NULL') <> 'CORT_BUILD='||l_rec.build 
      THEN 
        l_rec.status := 'STALE';
      ELSE
        cort_exec_pkg.raise_error( 'Build for application '||in_application||' is already running. Use attach_to_build procedure to attach new session to existing build');
      END IF;  
    END IF; 

    IF l_rec.status = 'STALE' THEN
      l_rec.status := 'ACTIVE';
      cort_aux_pkg.update_build(l_rec);
    
      cort_aux_pkg.start_build_session(l_rec.build);
      
      -- attache session to the existing build
      cort_session_pkg.set_param(
        in_param_name   => 'application',
        in_param_value  => l_rec.application
      );
      cort_session_pkg.set_param(
        in_param_name   => 'release',
        in_param_value  => l_rec.release
      );
         
      set_build_mode(TRUE);
    END IF;  
    
    IF l_rec.status <> 'ACTIVE' THEN
      cort_exec_pkg.raise_error( 'Build for application '||in_application||' is not running');
    END IF;      
  END resume_build;        

  PROCEDURE end_build(
    in_application IN VARCHAR2
  )
  AS
    l_rec cort_builds%ROWTYPE;
  BEGIN
    l_rec := cort_aux_pkg.find_build(
               in_application => in_application
             );
             
    IF l_rec.status IN ('ACTIVE', 'STALE') THEN
      IF l_rec.status = 'ACTIVE' AND 
         cort_aux_pkg.is_build_active(l_rec.build)  
      THEN
        cort_exec_pkg.raise_error( 'This build is running in another session');
      END IF;
                   
      -- build temp objects   
      drop_build_temp_objects(l_rec.build);
          
      l_rec.status := 'COMPLETED';
      l_rec.end_time := SYSDATE;

      cort_aux_pkg.stop_build_session(l_rec.build);

      cort_aux_pkg.update_build(l_rec);

      set_build_mode(FALSE);
    END IF;         
  END end_build;

  PROCEDURE revert_build(
    in_application IN VARCHAR2,
    in_test        IN BOOLEAN DEFAULT NULL
  )
  AS
    TYPE t_rec_arr IS TABLE OF cort_objects%ROWTYPE INDEX BY PLS_INTEGER;
    l_arr    t_rec_arr; 
    l_rec    cort_builds%ROWTYPE;
    l_params cort_params_pkg.gt_params_rec;
    l_sid    VARCHAR2(30);
    
    PROCEDURE revert_objects
    AS
    BEGIN
      FOR i IN 1..l_arr.COUNT LOOP
        cort_log_pkg.init_log(
          in_sid          => l_sid,
          in_action       => 'REVERT',
          in_job_time     => SYSTIMESTAMP,
          in_object_type  => l_arr(i).object_type,
          in_object_owner => l_arr(i).object_owner,
          in_object_name  => l_arr(i).object_name
        );
        cort_exec_pkg.revert_change(
          in_metadata_rec => l_arr(i),
          in_params_rec   => l_params
        );
      END LOOP;         
    END revert_objects;
  BEGIN 
    l_params := cort_session_pkg.get_params;
    IF in_test IS NOT NULL THEN
      l_params.test := in_test;
    END IF;
    
    l_sid := dbms_session.unique_session_id;
    l_rec := cort_aux_pkg.find_build(
               in_application => in_application
             );
    IF l_rec.build IS NOT NULL AND
       l_rec.status IN ('ACTIVE','STALE') 
    THEN
      -- revert schema changes first - apply in reverse order
      SELECT * 
        BULK COLLECT
        INTO l_arr
        FROM cort_objects
       WHERE build = l_rec.build
         AND object_type IN ('TABLE','INDEX','INDEXES','SEQUENCE')
       ORDER by exec_time DESC;
      
      revert_objects;
      
      l_arr.DELETE;
      
      -- revert other objects - apply in normal order
      SELECT * 
        BULK COLLECT
        INTO l_arr
        FROM cort_objects
       WHERE build = l_rec.build
         AND object_type NOT IN ('TABLE','INDEX','INDEXES','SEQUENCE')
       ORDER by exec_time ASC;
      
      revert_objects;       

      IF NOT l_params.test THEN
        l_rec.status := 'REVERTED';
        l_rec.end_time := SYSDATE;
        cort_aux_pkg.stop_build_session(l_rec.build);

        cort_aux_pkg.update_build(l_rec);

        set_build_mode(FALSE);
      END IF;  
    END IF;         
  END revert_build;

END cort_pkg;
/