CREATE OR REPLACE PACKAGE cort_pkg
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
  Description: API for end-user - wrappers around main procedures/functions.  
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added build API
  ----------------------------------------------------------------------------------------------------------------------  
*/

  /* This package will be granted to public */
  
  -- return CORT param default value 
  FUNCTION get_param_default_value(
    in_param_name   IN VARCHAR2
  )
  RETURN VARCHAR2;
  
  -- setters for modifying CORT default params permanently
  PROCEDURE set_param_default_value(
    in_param_name   IN VARCHAR2,
    in_param_value  IN NUMBER
  );

  PROCEDURE set_param_default_value(
    in_param_name   IN VARCHAR2,
    in_param_value  IN VARCHAR2
  );
  
  PROCEDURE set_param_default_value(
    in_param_name   IN VARCHAR2,
    in_param_value  IN BOOLEAN
  );
  
  -- Procedure is called from job 
  PROCEDURE execute_action(
    in_sid IN VARCHAR2
  );

  -- permanently enable CORT
  PROCEDURE enable;
  
  -- permanently disable CORT
  PROCEDURE disable;

  -- get CORT status (ENABLED/DISABLED/DISABLED FOR SESSION)
  FUNCTION get_status
  RETURN VARCHAR2;
  
  -- Revert the latest change for given object (overloaded)
  PROCEDURE revert_object(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_echo         IN BOOLEAN  DEFAULT NULL,  -- NULL - take from session param 
    in_test         IN BOOLEAN  DEFAULT NULL   -- NULL - take from session param
  );
  
  -- Wrapper for tables 
  PROCEDURE revert_table(
    in_table_name   IN VARCHAR2,
    in_table_owner  IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_echo         IN BOOLEAN  DEFAULT NULL,  -- NULL - take from session param 
    in_test         IN BOOLEAN  DEFAULT NULL   -- NULL - take from session param
  );

  -- drop object if it exists
  PROCEDURE drop_object(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  );
  
  -- Wrapper for tables 
  PROCEDURE drop_table(
    in_table_name   IN VARCHAR2,
    in_table_owner  IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  );

  -- drop revert object if it exists
  PROCEDURE drop_revert_object(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  );

  -- Warpper for tables 
  PROCEDURE drop_revert_table(
    in_table_name  IN VARCHAR2,
    in_table_owner IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  );

  -- disable all foreign keys on given table
  PROCEDURE disable_all_references(
    in_table_name IN VARCHAR2,
    in_owner      IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  );

  -- enable all foreign keys on given table
  PROCEDURE enable_all_references(
    in_table_name IN VARCHAR2,
    in_owner      IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA'),
    in_validate   IN BOOLEAN DEFAULT TRUE
  );

  --enable CORT-session for indexes
  PROCEDURE begin_index_definition(
    in_table_name IN VARCHAR2,
    in_owner      IN VARCHAR2 DEFAULT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
  );
   
  --disable CORT-session for indexes and apply all changes
  PROCEDURE end_index_definition;

  --cancel CORT-session for indexes without applying any changes
  PROCEDURE cancel_index_definition;

  -- start new build
  PROCEDURE start_build(
    in_application IN VARCHAR2,
    in_release     IN VARCHAR2 DEFAULT NULL
  );

  -- Check is any of session (exception current one) linked to given build active  
  FUNCTION is_build_active(
    in_application IN VARCHAR2
  )
  RETURN BOOLEAN;

  -- attach new session to the active(running) build - multi-session build
  PROCEDURE attach_to_build(
    in_application IN VARCHAR2
  );        

  -- detache current session from the build
  PROCEDURE detach_from_build(
    in_application IN VARCHAR2
  );

  -- reactivate stale  build (if build session failed). Suppose to continue from the failure point 
  PROCEDURE resume_build(
    in_application IN VARCHAR2
  );        

  -- drop objects included into previous build but not installed whithin given build
  PROCEDURE drop_excluded_objects(
    in_application IN VARCHAR2
  );
  
  -- finish build
  PROCEDURE end_build(
    in_application IN VARCHAR2
  );

  -- rollback all DDL changes made within build 
  PROCEDURE revert_build(
    in_application IN VARCHAR2,
    in_test        IN BOOLEAN DEFAULT NULL -- null - take value from session params
  );
  
END cort_pkg;
/