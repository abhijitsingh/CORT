CREATE OR REPLACE PACKAGE BODY cort_session_pkg
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
  Description: Session level API for end-user.  
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.02   | Rustam Kafarov    | Added API to read/change params for current session only
  ----------------------------------------------------------------------------------------------------------------------  
*/

  g_init                  BOOLEAN := FALSE;
  g_session_params_rec    gt_params_rec := get_params;

  /* Private */

  PROCEDURE init_params
  AS
  BEGIN
    IF NOT g_init THEN
      -- read cort params with default values from cort_params table
      g_session_params_rec := cort_params_pkg.get_default_cort_params;
      g_init := TRUE;
    END IF;
  END init_params;
  
  /* Public */

  -- getters for session params
  FUNCTION get_params
  RETURN gt_params_rec
  AS
  BEGIN
    init_params;
    RETURN g_session_params_rec;
  END get_params;
  
  
  FUNCTION get_param(
    in_param_name   IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    init_params;
    RETURN cort_params_pkg.get_param_value(g_session_params_rec, in_param_name);
  END get_param;
  
  FUNCTION get_bool_param(
    in_param_name   IN VARCHAR2
  )
  RETURN BOOLEAN
  AS
  BEGIN
    init_params;
    RETURN cort_params_pkg.str_to_bool(cort_params_pkg.get_param_value(g_session_params_rec, in_param_name));
  END get_bool_param;

  -- setters for session params
  PROCEDURE set_param(
    in_param_name   IN VARCHAR2,
    in_param_value  IN NUMBER
  )
  AS
  BEGIN
    init_params;
    cort_params_pkg.set_param_value(g_session_params_rec, in_param_name, in_param_value);
  END set_param;

  PROCEDURE set_param(
    in_param_name   IN VARCHAR2,
    in_param_value  IN VARCHAR2
  )
  AS
  BEGIN
    init_params;
    cort_params_pkg.set_param_value(g_session_params_rec, in_param_name, in_param_value);
  END set_param;
  
  PROCEDURE set_param(
    in_param_name   IN VARCHAR2,
    in_param_value  IN BOOLEAN
  )
  AS
  BEGIN
    init_params;
    cort_params_pkg.set_param_value(g_session_params_rec, in_param_name, cort_params_pkg.bool_to_str(in_param_value));
  END set_param;

  PROCEDURE enable
  AS
  BEGIN
    cort_trg_pkg.set_context('DISABLED_FOR_SESSION',NULL);
  END enable;
  
  PROCEDURE disable
  AS
  BEGIN
    cort_trg_pkg.set_context('DISABLED_FOR_SESSION','TRUE');
  END disable;

  -- get CORT status (ENABLED/DISABLED)
  FUNCTION get_status
  RETURN VARCHAR2
  AS
    l_status VARCHAR2(20);
  BEGIN
    l_status := cort_aux_pkg.get_cort_status;
    IF l_status = 'ENABLED' THEN
      IF cort_trg_pkg.get_context('DISABLED_FOR_SESSION') = 'TRUE' THEN
        l_status := 'DISABLED';
      END IF;  
    END IF;
    RETURN l_status;  
  END get_status;

  -- return timestamp (ID) of the last executed in current session job
  FUNCTION get_last_job_timestamp 
  RETURN TIMESTAMP
  AS
  BEGIN
    RETURN cort_log_pkg.get_last_job_time;
  END get_last_job_timestamp;

  -- revert last change
  PROCEDURE revert(
    in_echo         IN BOOLEAN  DEFAULT NULL,  -- NULL - take from session param 
    in_test         IN BOOLEAN  DEFAULT NULL   -- NULL - take from session param
  )
  AS
    l_rec   cort_objects%ROWTYPE;
  BEGIN
    BEGIN
      SELECT * 
        INTO l_rec 
        FROM (SELECT *
                FROM cort_objects
               WHERE sid = dbms_session.unique_session_id
               ORDER BY exec_time DESC)
       WHERE ROWNUM = 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        cort_exec_pkg.raise_error('No CORT changes in current session');
    END;
    IF l_rec.revert_ddl IS NOT NULL THEN
      cort_pkg.revert_object(
        in_object_type  => l_rec.object_type,
        in_object_name  => l_rec.object_name,
        in_object_owner => l_rec.object_owner,
        in_echo         => in_echo, 
        in_test         => in_test
      );
    ELSE
      cort_exec_pkg.raise_error('Last CORT change is not revertable');
    END IF;
  END revert;

END cort_session_pkg;
/