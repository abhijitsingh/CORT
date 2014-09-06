CREATE OR REPLACE PACKAGE cort_session_pkg
AUTHID CURRENT_USER
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
  Description: session level API for end-user.  
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.02   | Rustam Kafarov    | Added API to read/change params for current session only
  ----------------------------------------------------------------------------------------------------------------------  
*/

  /* This package will be granted to public */
  
  SUBTYPE gt_params_rec IS cort_params_pkg.gt_params_rec;   
  
  -- getters for session params
  FUNCTION get_params
  RETURN gt_params_rec;

  FUNCTION get_param(
    in_param_name   IN VARCHAR2
  )
  RETURN VARCHAR2;
  
  FUNCTION get_bool_param(
    in_param_name   IN VARCHAR2
  )
  RETURN BOOLEAN;

  -- setters for session params
  PROCEDURE set_param(
    in_param_name   IN VARCHAR2,
    in_param_value  IN NUMBER
  );

  PROCEDURE set_param(
    in_param_name   IN VARCHAR2,
    in_param_value  IN VARCHAR2
  );
  
  PROCEDURE set_param(
    in_param_name   IN VARCHAR2,
    in_param_value  IN BOOLEAN
  );
  
  -- enable CORT for session
  PROCEDURE enable;
  
  -- disable CORT for session
  PROCEDURE disable;

  -- get CORT status (ENABLED/DISABLED)
  FUNCTION get_status
  RETURN VARCHAR2;
  
  -- return timestamp (ID) of the last executed in current session job
  FUNCTION get_last_job_timestamp 
  RETURN TIMESTAMP;

  -- revert last change
  PROCEDURE revert(
    in_echo         IN BOOLEAN  DEFAULT NULL,  -- NULL - take from session param 
    in_test         IN BOOLEAN  DEFAULT NULL   -- NULL - take from session param
  );

END cort_session_pkg;
/