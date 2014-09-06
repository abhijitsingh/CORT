CREATE OR REPLACE PACKAGE cort_log_pkg 
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
  14.02   | Rustam Kafarov    | Added get_last_log function
  ----------------------------------------------------------------------------------------------------------------------  
*/


  -- Init execution log
  PROCEDURE init_log(
    in_sid             IN VARCHAR2,
    in_action          IN VARCHAR2,
    in_job_time        IN TIMESTAMP_UNCONSTRAINED,
    in_object_type     IN VARCHAR2,
    in_object_name     IN VARCHAR2,
    in_object_owner    IN VARCHAR2
  );  
  
  -- Get global variables values
  PROCEDURE get_last_log(
    out_sid             OUT VARCHAR2,
    out_action          OUT VARCHAR2,
    out_job_time        OUT TIMESTAMP_UNCONSTRAINED,
    out_object_type     OUT VARCHAR2,
    out_object_name     OUT VARCHAR2,
    out_object_owner    OUT VARCHAR2
  );
  
  -- Get g_job_time global variable value
  FUNCTION get_last_job_time 
  RETURN TIMESTAMP_UNCONSTRAINED;

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
  );
  
  -- Wrapper for  logging function
  FUNCTION log(
    in_log_type  IN VARCHAR2,
    in_text      IN CLOB
  )
  RETURN TIMESTAMP_UNCONSTRAINED;

  -- Wrapper for logging function
  PROCEDURE log(
    in_log_type  IN VARCHAR2,
    in_text      IN CLOB   DEFAULT NULL,
    in_exec_time IN NUMBER DEFAULT NULL
  );

  PROCEDURE update_exec_time(
    in_log_time IN TIMESTAMP_UNCONSTRAINED
  );

END cort_log_pkg;
/