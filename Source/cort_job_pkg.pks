CREATE OR REPLACE PACKAGE cort_job_pkg 
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
  14.02   | Rustam Kafarov    | Added procedure for running job in same session
  ----------------------------------------------------------------------------------------------------------------------  
*/

  gc_cort_job_name    CONSTANT VARCHAR2(30) := '#cort#';
  gc_run_as_job_name  CONSTANT VARCHAR2(30) := '#runas';

  -- Return running job record and update set job id to it
  FUNCTION get_job(
    in_sid IN VARCHAR2
  ) 
  RETURN cort_jobs%ROWTYPE;

  -- Finish job
  PROCEDURE success_job(
    in_sid IN VARCHAR2
  );
  
  -- Finish job with error
  PROCEDURE fail_job(
    in_sid             IN VARCHAR2,
    in_error_code      IN NUMBER,
    in_error_message   IN VARCHAR2,
    in_error_backtrace IN VARCHAR2,
    in_cort_stack      IN VARCHAR2
  );
  
  -- return last executed job timestamp
  FUNCTION get_last_job_timestamp 
  RETURN TIMESTAMP;
  
  -- Start job
  PROCEDURE start_job(
    in_action        IN VARCHAR2,
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sql           IN CLOB
  );

  -- Execute under another user
  PROCEDURE run_as_user(
    in_action        IN VARCHAR2,
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    in_sql           IN CLOB
  );

END cort_job_pkg;
/