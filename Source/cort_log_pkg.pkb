CREATE OR REPLACE PACKAGE BODY cort_log_pkg 
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
  Description: Logging API
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Removed dependency on v$session
  ----------------------------------------------------------------------------------------------------------------------  
*/

  g_counter         PLS_INTEGER := 0;
  g_sid             VARCHAR2(24);
  g_action          VARCHAR2(30);
  g_job_time        TIMESTAMP_UNCONSTRAINED;
  g_object_type     VARCHAR2(30);
  g_object_owner    VARCHAR2(30);
  g_object_name     VARCHAR2(30);
  
  -- Init execution log
  PROCEDURE init_log(
    in_sid             IN VARCHAR2,
    in_action          IN VARCHAR2,
    in_job_time        IN TIMESTAMP_UNCONSTRAINED,
    in_object_type     IN VARCHAR2,
    in_object_name     IN VARCHAR2,
    in_object_owner    IN VARCHAR2
  )
  AS
  BEGIN
    IF in_job_time <> SYSTIMESTAMP THEN
      g_counter         := 0;
    END IF;
    g_sid             := in_sid;
    g_action          := in_action;
    g_job_time        := in_job_time;   
    g_object_type     := in_object_type;  
    g_object_owner    := in_object_owner;   
    g_object_name     := in_object_name;    
  END init_log;
    
  -- Get global variables values
  PROCEDURE get_last_log(
    out_sid             OUT VARCHAR2,
    out_action          OUT VARCHAR2,
    out_job_time        OUT TIMESTAMP_UNCONSTRAINED,
    out_object_type     OUT VARCHAR2,
    out_object_name     OUT VARCHAR2,
    out_object_owner    OUT VARCHAR2
  )
  AS
  BEGIN
    out_sid             := g_sid;
    out_action          := g_action;
    out_job_time        := g_job_time;   
    out_object_type     := g_object_type;  
    out_object_owner    := g_object_owner;   
    out_object_name     := g_object_name;    
  END get_last_log;

  -- Get g_job_time global variable value
  FUNCTION get_last_job_time 
  RETURN TIMESTAMP_UNCONSTRAINED
  AS
  BEGIN
    RETURN g_job_time;
  END get_last_job_time;


  -- Logs every CORT high level operation
  PROCEDURE log_job(
    in_status          IN VARCHAR2,
    in_job_owner       IN VARCHAR2,
    in_job_name        IN VARCHAR2,
    in_job_sid         IN VARCHAR2,    
    in_sql_text        IN CLOB,
    in_schema_name     IN VARCHAR2,
    in_output          IN CLOB     DEFAULT NULL,
    in_error_code      IN NUMBER   DEFAULT NULL,
    in_error_message   IN VARCHAR2 DEFAULT NULL,
    in_error_backtrace IN VARCHAR2 DEFAULT NULL,
    in_cort_stack      IN VARCHAR2 DEFAULT NULL
  )
  AS
  PRAGMA autonomous_transaction;
    l_log_rec     cort_job_log%ROWTYPE;
    l_params_xml  XMLType;
  BEGIN
    g_counter := g_counter + 1;
    
    cort_xml_pkg.write_to_xml(cort_session_pkg.get_params, l_params_xml);

    l_log_rec.job_log_time    := SYSTIMESTAMP + TO_DSINTERVAL('PT0.'||TO_CHAR(g_counter, 'fm000000009')||'S');
    l_log_rec.sid             := g_sid;
    l_log_rec.action          := g_action;
    l_log_rec.status          := in_status;
    l_log_rec.job_owner       := in_job_owner;
    l_log_rec.job_name        := in_job_name;
    l_log_rec.job_sid         := in_job_sid; 
    l_log_rec.job_time        := g_job_time; 
    l_log_rec.object_type     := g_object_type; 
    l_log_rec.object_owner    := g_object_owner;
    l_log_rec.object_name     := g_object_name; 
    l_log_rec.sql_text        := in_sql_text;    
    l_log_rec.schema_name     := in_schema_name;
    l_log_rec.session_params  := l_params_xml; 
    l_log_rec.build           := SUBSTR(SYS_CONTEXT('USERENV','CLIENT_INFO'),12);
    l_log_rec.session_id      := SYS_CONTEXT('USERENV','SID');
    l_log_rec.username        := USER;
    l_log_rec.osuser          := SYS_CONTEXT('USERENV','OS_USER'); 
    l_log_rec.machine         := SYS_CONTEXT('USERENV','HOST');
    l_log_rec.terminal        := SYS_CONTEXT('USERENV','TERMINAL');
    l_log_rec.module          := SYS_CONTEXT('USERENV','MODULE');
    l_log_rec.output          := in_output;
    l_log_rec.error_code      := in_error_code;
    l_log_rec.error_message   := in_error_message;
    l_log_rec.error_backtrace := in_error_backtrace;
    l_log_rec.cort_stack      := in_cort_stack;
    
    -- insert new record into log table 
    INSERT INTO cort_job_log
    VALUES l_log_rec; 
    
    COMMIT; 
  END log_job;

  -- Generic logging function
  FUNCTION int_log(
    in_log_type     IN VARCHAR2,
    in_package_name IN VARCHAR2,
    in_line_number  IN NUMBER,
    in_text         IN CLOB,
    in_exec_time    IN NUMBER
  )
  RETURN TIMESTAMP_UNCONSTRAINED
  AS
  PRAGMA autonomous_transaction;
    l_log_rec      cort_log%ROWTYPE;
  BEGIN
    g_counter := g_counter + 1;

    l_log_rec.log_time        := SYSTIMESTAMP + TO_DSINTERVAL('PT0.'||TO_CHAR(g_counter, 'fm000000009')||'S');
    l_log_rec.sid             := g_sid;
    l_log_rec.action          := g_action;
    l_log_rec.job_time        := g_job_time;
    l_log_rec.object_type     := g_object_type; 
    l_log_rec.object_owner    := g_object_owner;
    l_log_rec.object_name     := g_object_name; 
    l_log_rec.log_type        := in_log_type;
    l_log_rec.text            := in_text;
    l_log_rec.package_name    := in_package_name;
    l_log_rec.line_number     := in_line_number;
    l_log_rec.execution_time  := in_exec_time; 
    
    INSERT INTO cort_log
    VALUES l_log_rec;
    
    COMMIT;
    
    RETURN l_log_rec.log_time;
  END int_log;
  
  -- Warpper for logging function
  FUNCTION log(
    in_log_type  IN VARCHAR2,
    in_text      IN CLOB
  )
  RETURN TIMESTAMP_UNCONSTRAINED
  AS
    l_caller_owner VARCHAR2(100);
    l_caller_type  VARCHAR2(100);
    l_package_name VARCHAR2(100);
    l_line_number  NUMBER;
  BEGIN
    owa_util.who_called_me(
      owner      => l_caller_owner,
      name       => l_package_name,
      lineno     => l_line_number,
      caller_t   => l_caller_type
    );
    
    RETURN int_log(
             in_log_type     => in_log_type,
             in_package_name => l_package_name,
             in_line_number  => l_line_number, 
             in_text         => in_text,
             in_exec_time    => NULL
           );
  END log;

  -- Wrapper for logging function
  PROCEDURE log(
    in_log_type  IN VARCHAR2,
    in_text      IN CLOB   DEFAULT NULL,
    in_exec_time IN NUMBER DEFAULT NULL
  )
  AS
    l_log_time     TIMESTAMP_UNCONSTRAINED;
    l_caller_owner VARCHAR2(100);
    l_caller_type  VARCHAR2(100);
    l_package_name VARCHAR2(100);
    l_line_number  NUMBER;
  BEGIN
    owa_util.who_called_me(
      owner      => l_caller_owner,
      name       => l_package_name,
      lineno     => l_line_number,
      caller_t   => l_caller_type
    );
    
    l_log_time := int_log(
                    in_log_type     => in_log_type,
                    in_package_name => l_package_name,
                    in_line_number  => l_line_number, 
                    in_text         => in_text,
                    in_exec_time    => in_exec_time 
                  );
  END log;


  PROCEDURE update_exec_time(
    in_log_time IN TIMESTAMP_UNCONSTRAINED
  )
  AS
  PRAGMA autonomous_transaction;
    l_time  TIMESTAMP(6); -- reduce precision to 6
    l_msg   cort_log.error_msg%TYPE;
  BEGIN
    l_time := in_log_time;  -- round to microseconds
    IF SQLCODE <> 0 THEN
      l_msg := SQLERRM;
    END IF;  
    UPDATE cort_log
       SET execution_time = GREATEST(EXTRACT(SECOND FROM SYSTIMESTAMP-l_time),0),
           error_msg = l_msg
     WHERE log_time = in_log_time;
    COMMIT; 
  END update_exec_time;
  
END cort_log_pkg;
/
