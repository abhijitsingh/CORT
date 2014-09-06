CREATE OR REPLACE PACKAGE BODY cort_job_pkg 
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
  Description: job execution API
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added procedure for running job in same session. Improved waiting cycle.
  ----------------------------------------------------------------------------------------------------------------------  
*/

  g_registering_time  TIMESTAMP(6);
  

  -- Run infinit loop until job is done
  FUNCTION wait_for_job_end(
    in_sid IN VARCHAR2
  ) 
  RETURN cort_jobs%ROWTYPE
  AS
    l_rec              cort_jobs%ROWTYPE;
    l_job_details_rec  all_scheduler_job_run_details%rowtype;
    
    FUNCTION get_job_rec(in_sid IN VARCHAR2)
    RETURN cort_jobs%ROWTYPE
    AS
      l_rec              cort_jobs%ROWTYPE;
    BEGIN
      BEGIN
        SELECT *
          INTO l_rec
          FROM cort_jobs
         WHERE sid = in_sid;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          cort_exec_pkg.debug('Record with sid = '||in_sid||' not found in cort_jobs');
      END;   
      RETURN l_rec;
    END get_job_rec;
    
  BEGIN
    l_rec := get_job_rec(in_sid);
    IF l_rec.status IS NULL THEN
      RETURN l_rec;
    END IF;   

    -- infinity loop
    LOOP
      -- check that running job exists
      BEGIN
        SELECT *
          INTO l_job_details_rec  
          FROM all_scheduler_job_run_details
         WHERE owner = l_rec.job_owner
           AND job_name = gc_cort_job_name||in_sid
           AND actual_start_date > g_registering_time
           AND rownum = 1
         ORDER BY log_date DESC;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          NULL;
      END;
         
      CASE 
      WHEN l_job_details_rec.status = 'RUNNING' THEN 
        l_rec := get_job_rec(in_sid);  
        IF l_rec.status <> 'RUNNING' THEN
          EXIT;
        END IF;  
      WHEN l_job_details_rec.status = 'FAILED' THEN
        l_rec := get_job_rec(in_sid);  
        l_rec.error_code := NVL(l_rec.error_code,l_job_details_rec.error#);
        l_rec.error_backtrace := NVL(l_rec.error_backtrace,l_job_details_rec.additional_info);
        l_rec.status := 'FAILED';
        cort_exec_pkg.debug('Job "'||l_rec.job_owner||'"."'||gc_cort_job_name||in_sid||'" failed');
        EXIT;
      WHEN l_job_details_rec.status = 'SUCCEEDED' THEN 
        l_rec := get_job_rec(in_sid);  
        cort_exec_pkg.debug('job status = '||l_rec.status);
        EXIT;
      WHEN l_job_details_rec.status IS NULL THEN
        l_rec := get_job_rec(in_sid);  
        IF l_rec.status <> 'RUNNING' THEN
          EXIT;
        ELSE
          IF l_rec.job_sid IS NOT NULL AND 
             NOT dbms_session.is_session_alive(l_rec.job_sid) 
          THEN 
            cort_exec_pkg.debug('Job session '||l_rec.job_sid||' is not alive');
            l_rec.status := 'FAILED'; 
            EXIT;
          END IF;
        END IF;  
      END CASE;
    END LOOP;
    
    RETURN l_rec;
  END wait_for_job_end;

  -- Public 
  -- Return running job record and update set job id to it
  FUNCTION get_job(
    in_sid IN VARCHAR2
  ) 
  RETURN cort_jobs%ROWTYPE
  AS
  PRAGMA autonomous_transaction;
    l_rec cort_jobs%ROWTYPE;
  BEGIN
    SELECT *
      INTO l_rec
      FROM cort_jobs
     WHERE sid = in_sid
       AND status = 'RUNNING'
       FOR UPDATE WAIT 1;
    
    IF l_rec.sid IS NOT NULL THEN
      UPDATE cort_jobs
         SET job_sid = dbms_session.unique_session_id 
       WHERE sid = in_sid
         AND status = 'RUNNING';

       cort_log_pkg.init_log(
         in_sid          => in_sid,
         in_action       => l_rec.action,
         in_job_time     => l_rec.job_time,
         in_object_type  => l_rec.object_type,
         in_object_owner => l_rec.object_owner,
         in_object_name  => l_rec.object_name
       );

      cort_log_pkg.log_job(
        in_status          => 'RUNNING',
        in_job_owner       => l_rec.job_owner,
        in_job_name        => l_rec.job_name,
        in_job_sid         => dbms_session.unique_session_id,
        in_schema_name     => l_rec.schema_name,
        in_sql_text        => l_rec.sql_text
      );
    END IF;      
      
    COMMIT;
    
    RETURN l_rec;
  END get_job;
  
  -- add record for given job. Return TRUE if record added successfully. FALSE - if there is running job for current session 
  PROCEDURE register_job(
    in_sid           IN VARCHAR2,
    in_action        IN VARCHAR2,
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sql           IN CLOB,
    in_job_owner     IN VARCHAR2,
    in_job_name      IN VARCHAR2
  )
  AS
  PRAGMA autonomous_transaction;
    l_cnt        PLS_INTEGER;  
    l_rec        cort_jobs%ROWTYPE;
    l_new_rec    cort_jobs%ROWTYPE;
  BEGIN
    l_cnt := 0;  

    -- Get and lock record for given session   
    BEGIN
      SELECT *
        INTO l_rec
        FROM cort_jobs
       WHERE sid = in_sid
         FOR UPDATE WAIT 1;
      l_cnt := 1;  
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_cnt := 0;
    END;
    
    -- Check that there is no RUNNING job for current session
    IF l_cnt = 1 THEN
   
      IF l_rec.status <> 'RUNNING' THEN
        -- delete all record for current session 
        DELETE FROM cort_jobs
         WHERE sid = in_sid;
      ELSE
        SELECT COUNT(*)
          INTO l_cnt 
          FROM all_scheduler_running_jobs
         WHERE job_name = l_rec.job_name
           AND owner = l_rec.job_owner;
        IF l_cnt > 0 THEN
          -- release locks
          ROLLBACK;
          -- job is still running
          cort_exec_pkg.raise_error('There is a job attached to this session running in the background');
        ELSE
          DELETE FROM cort_jobs
           WHERE sid = in_sid;
        END IF;    
      END IF;
    END IF;
      
    -- Check that given object is not changing by another session  
    BEGIN
      SELECT *
        INTO l_rec
        FROM cort_jobs
       WHERE object_name = in_object_name
         AND object_owner = in_object_owner
         AND DECODE(status,'RUNNING','0',sid) = '0'
         FOR UPDATE WAIT 1;
      l_cnt := 1;  
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_cnt := 0;
    END;
      
    IF l_cnt = 1 THEN
      -- Check that found sessions are stil alive 
      IF dbms_session.is_session_alive(l_rec.sid) OR
         (l_rec.job_sid IS NOT NULL AND
          dbms_session.is_session_alive(l_rec.job_sid)) 
      THEN
        -- release locks
        ROLLBACK;
        -- sessions are alive
        cort_exec_pkg.raise_error( 'Object "'||in_object_owner||'"."'||in_object_name||'" is changing by another process');
      ELSE
        -- delete records for dead sessions
        DELETE FROM cort_jobs
         WHERE sid = l_rec.sid;
      END IF;
    END IF;

    l_new_rec.sid := in_sid;
    l_new_rec.action := in_action;
    l_new_rec.status := 'RUNNING';
    l_new_rec.job_time := g_registering_time;
    l_new_rec.job_owner := in_job_owner; 
    l_new_rec.job_name := in_job_name;
    l_new_rec.object_type := in_object_type;
    l_new_rec.object_owner := in_object_owner;
    l_new_rec.object_name := in_object_name;
    l_new_rec.sql_text := in_sql;
    l_new_rec.schema_name := in_job_owner;
    l_new_rec.build := SUBSTR(SYS_CONTEXT('USERENV','CLIENT_INFO'), 12);
    cort_xml_pkg.write_to_xml(cort_exec_pkg.g_params, l_new_rec.session_params);

    -- Try to register new job
    INSERT INTO cort_jobs
    VALUES l_new_rec;
    
    cort_log_pkg.init_log(
      in_sid          => in_sid,
      in_action       => in_action,
      in_job_time     => l_new_rec.job_time,
      in_object_type  => in_object_type,
      in_object_owner => in_object_owner,
      in_object_name  => in_object_name
    );
             
    cort_log_pkg.log_job(
      in_status          => 'REGISTERING',
      in_job_owner       => l_new_rec.job_owner,
      in_job_name        => l_new_rec.job_name,
      in_job_sid         => NULL,
      in_sql_text        => in_sql,
      in_schema_name     => l_new_rec.schema_name
    );    
    
    COMMIT;

  END register_job; 
  
  -- Finish job
  PROCEDURE success_job(
    in_sid IN VARCHAR2
  )
  AS
  PRAGMA autonomous_transaction;
    l_rec        cort_jobs%ROWTYPE;
    l_lines      dbms_output.chararr;
    l_num_lines  INTEGER := 2147483647;
    l_output     CLOB;
  BEGIN
    dbms_output.get_lines(l_lines, l_num_lines);
    cort_aux_pkg.lines_to_clob(
      in_str_arr  => l_lines, 
      out_clob    => l_output
    );     
    FOR I IN 1..l_num_lines LOOP
      dbms_output.put_line(l_lines(i));
    END LOOP;
    
    BEGIN
      SELECT * 
        INTO l_rec
        FROM cort_jobs
       WHERE sid = in_sid;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;  

    cort_log_pkg.log_job(
      in_status          => 'COMPLETED',
      in_job_owner       => l_rec.job_owner,
      in_job_name        => l_rec.job_name,
      in_job_sid         => l_rec.job_sid,
      in_sql_text        => l_rec.sql_text,
      in_schema_name     => l_rec.schema_name,
      in_output          => l_output
    );    
         
    UPDATE cort_jobs
       SET status = 'COMPLETED',
           output = l_output
     WHERE sid = in_sid;
       
    COMMIT;
  END success_job; 
  
  -- Finish job with error
  PROCEDURE fail_job(
    in_sid             IN VARCHAR2,
    in_error_code      IN NUMBER,
    in_error_message   IN VARCHAR2,
    in_error_backtrace IN VARCHAR2,
    in_cort_stack      IN VARCHAR2
  )
  AS
  PRAGMA autonomous_transaction;
    l_rec        cort_jobs%ROWTYPE;
    l_lines      dbms_output.chararr;
    l_num_lines  INTEGER := 2147483647;
    l_output     CLOB;
  BEGIN
    dbms_output.get_lines(l_lines,l_num_lines);
    cort_aux_pkg.lines_to_clob(
      in_str_arr  => l_lines, 
      out_clob    => l_output
    );          
    FOR i IN 1..l_num_lines LOOP
      dbms_output.put_line(l_lines(i));
    END LOOP;
    
    BEGIN
      SELECT * 
        INTO l_rec
        FROM cort_jobs
       WHERE sid = in_sid
         AND status = 'RUNNING';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;  

    cort_log_pkg.log_job(
      in_status          => 'FAILED',
      in_job_owner       => l_rec.job_owner,
      in_job_name        => l_rec.job_name,
      in_job_sid         => l_rec.job_sid,
      in_sql_text        => l_rec.sql_text,
      in_schema_name     => l_rec.schema_name,
      in_output          => l_output,
      in_error_code      => in_error_code,
      in_error_message   => in_error_message,
      in_error_backtrace => in_error_backtrace,
      in_cort_stack      => in_cort_stack
    );    

    UPDATE cort_jobs
       SET status          = 'FAILED',
           output          = l_output,
           error_code      = in_error_code,
           error_message   = in_error_message,
           error_backtrace = in_error_backtrace,
           cort_stack      = in_cort_stack
     WHERE sid = in_sid
       AND status = 'RUNNING';
       
    COMMIT;
    
  END fail_job; 

  -- return last executed job timestamp
  FUNCTION get_last_job_timestamp 
  RETURN TIMESTAMP
  AS
  BEGIN
    RETURN g_registering_time;
  END get_last_job_timestamp;  

  -- Start job
  PROCEDURE start_job(
    in_action        IN VARCHAR2,
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sql           IN CLOB
  )
  AS
  PRAGMA autonomous_transaction;
    l_rec                 cort_jobs%ROWTYPE;
    l_lines               dbms_output.chararr;
    l_sid                 VARCHAR2(30);
    l_job_owner           VARCHAR2(30);
    l_job_name            VARCHAR2(30);
    l_job_full_name       VARCHAR2(65);
    l_job_action          VARCHAR2(200);
    e_job_already_exists  EXCEPTION; 
    PRAGMA                EXCEPTION_INIT(e_job_already_exists, -27477); 
  BEGIN
    l_sid := dbms_session.unique_session_id;

    $IF cort_options_pkg.gc_single_schema $THEN 
      l_job_owner := cort_aux_pkg.gc_cort_schema;
    $ELSE
      l_job_owner := SYS_CONTEXT('USERENV','SESSION_USER');
    $END  
    l_job_name := gc_cort_job_name||l_sid;
    l_job_full_name := '"'||l_job_owner||'"."'||l_job_name||'"';
    l_job_action := '"'||SYS_CONTEXT('USERENV','CURRENT_USER')||'".CORT_PKG.EXECUTE_ACTION';
        
    cort_exec_pkg.debug('Registering job '||l_job_full_name);
        
    g_registering_time := SYSTIMESTAMP;

    register_job(
      in_sid           => l_sid,
      in_action        => in_action,
      in_object_type   => in_object_type,
      in_object_name   => in_object_name,
      in_object_owner  => in_object_owner,
      in_sql           => in_sql,
      in_job_owner     => l_job_owner,
      in_job_name      => l_job_name
    );

    BEGIN
      dbms_scheduler.create_job(
        job_name            => l_job_full_name,
        job_type            => 'STORED_PROCEDURE',
        job_action          => l_job_action,
        number_of_arguments => 1,
        auto_drop           => TRUE,
        enabled             => FALSE
      );
    EXCEPTION
      WHEN e_job_already_exists THEN
        cort_exec_pkg.debug('Job "'||l_job_owner||'"."'||l_job_name||'" alreasy exists');
        NULL;
    END; 
    dbms_scheduler.set_job_argument_value(
      job_name          => l_job_full_name,
      argument_position => 1,
      argument_value    => l_sid
    );
    dbms_scheduler.enable(
       name => l_job_full_name
    );
    COMMIT;
    l_rec := wait_for_job_end(
      in_sid => l_sid
    );
    cort_exec_pkg.debug('Job status = '||l_rec.status);
    IF (l_rec.status = 'RUNNING') OR (l_rec.status IS NULL) THEN
      fail_job(
        in_sid             => l_sid,
        in_error_code      => sqlcode,
        in_error_message   => NVL(l_rec.error_message, sqlerrm),
        in_error_backtrace => NVL(l_rec.error_backtrace, dbms_utility.format_error_backtrace),
        in_cort_stack      => l_rec.cort_stack
      );
    END IF;
    cort_aux_pkg.clob_to_lines(l_rec.output, l_lines);     
    FOR i IN 1..l_lines.COUNT LOOP
      dbms_output.put_line(l_lines(i));
    END LOOP;
          
    IF (l_rec.status = 'FAILED') OR (l_rec.status IS NULL)  THEN
      RAISE_APPLICATION_ERROR(
        -20000,
        NVL(REGEXP_REPLACE(l_rec.error_message, 'ORA-[0-9]{5}: '),'CORT internal error')||CHR(10)||
        l_rec.error_backtrace
      );
    END IF;
        
    COMMIT;         
  END start_job;
  
  -- Execute under current user
  PROCEDURE run_as_user(
    in_action        IN VARCHAR2,
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sql           IN CLOB
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    l_sid                 VARCHAR2(30);
    l_job_owner           VARCHAR2(30);
    l_job_name            VARCHAR2(30);
    l_job_full_name       VARCHAR2(65);
    l_job_action          VARCHAR2(200);
    e_job_already_exists  EXCEPTION; 
    PRAGMA                EXCEPTION_INIT(e_job_already_exists, -27477); 
  BEGIN
    l_sid := dbms_session.unique_session_id;

    IF cort_options_pkg.gc_single_schema THEN 
      l_job_owner := cort_aux_pkg.gc_cort_schema;
    ELSE
      l_job_owner := SYS_CONTEXT('USERENV','SESSION_USER');
    END IF;  
    l_job_name := gc_run_as_job_name||l_sid;
    l_job_full_name := '"'||l_job_owner||'"."'||l_job_name||'"';
    l_job_action := '"'||SYS_CONTEXT('USERENV','CURRENT_USER')||'".CORT_PKG.EXECUTE_ACTION';

    cort_exec_pkg.debug('Registering job '||l_job_full_name);
        
    g_registering_time := SYSTIMESTAMP;

    register_job(
      in_sid           => l_sid,
      in_action        => in_action,
      in_object_type   => in_object_type,
      in_object_name   => in_object_name,
      in_object_owner  => in_object_owner,
      in_sql           => in_sql,
      in_job_owner     => l_job_owner,
      in_job_name      => l_job_name
    );

    BEGIN
      dbms_scheduler.create_job(
        job_name            => l_job_full_name,
        job_type            => 'STORED_PROCEDURE',
        job_action          => l_job_action,
        number_of_arguments => 1,
        auto_drop           => TRUE,
        enabled             => FALSE
      );
    EXCEPTION
      WHEN e_job_already_exists THEN
        cort_exec_pkg.debug('Job "'||l_job_owner||'"."'||l_job_name||'" alreasy exists');
        NULL;
    END; 
    dbms_scheduler.set_job_argument_value(
      job_name          => l_job_full_name,
      argument_position => 1,
      argument_value    => l_sid
    );
    BEGIN
      -- execute in the current session
      dbms_scheduler.run_job(
         job_name            => l_job_full_name,
         use_current_session => TRUE
      );
    EXCEPTION
      WHEN OTHERS THEN
        -- drop job
        dbms_scheduler.drop_job(
          job_name            => l_job_full_name
        );
        RAISE;
    END;  
    -- drop job
    dbms_scheduler.drop_job(
      job_name            => l_job_full_name
    );
    
    COMMIT;
  END run_as_user;

END cort_job_pkg;
/

