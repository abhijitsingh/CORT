CREATE OR REPLACE PACKAGE BODY cort_params_pkg 
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
  Description: Type and API for main application parameters
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added API to read/write param default values
  ----------------------------------------------------------------------------------------------------------------------  
*/

  -- global variable for passing record into dynamic SQL
  g_params_rec      gt_params_rec;
  
  -- global key-value array of CORT param name-types 
  g_attr_type_indx  arrays.gt_str_indx;
  g_key_value_indx  arrays.gt_lstr_indx;
  
  
  -- load into array names and types of CORT parameters record
  PROCEDURE init_session_variables
  AS
    l_attr_name_arr   arrays.gt_str_arr;
    l_attr_type_arr   arrays.gt_str_arr;
  BEGIN
    -- init only once
    IF g_attr_type_indx.COUNT = 0 THEN 
      SELECT argument_name, pls_type
        BULK COLLECT 
        INTO l_attr_name_arr, l_attr_type_arr  
        FROM all_arguments
       WHERE owner = SYS_CONTEXT('USERENV','CURRENT_USER') 
         AND package_name = 'CORT_PARAMS_PKG'   
         AND object_name = 'SET_PARAMS_REC'
         AND data_level = 1;
        
      FOR i IN 1..l_attr_name_arr.COUNT LOOP
        g_attr_type_indx(l_attr_name_arr(i)) := l_attr_type_arr(i);
      END LOOP;
    END IF;
  END init_session_variables;
  
  -- global declarations
  
  -- convert BOOLEAN to VARCHAR2  
  FUNCTION bool_to_str(in_value IN BOOLEAN) 
  RETURN VARCHAR2
  AS
    l_result  VARCHAR2(10);
  BEGIN
    IF in_value = TRUE THEN
      l_result := 'TRUE';
    ELSIF in_value = FALSE THEN
      l_result := 'FALSE';
    ELSE
      l_result := 'NULL';
    END IF;   
    RETURN l_result;
  END bool_to_str;
  
  -- convert VARCHAR2 to BOOLEAN
  FUNCTION str_to_bool(in_value IN VARCHAR2) 
  RETURN BOOLEAN
  AS 
    l_result  BOOLEAN;
  BEGIN
    IF UPPER(in_value) = 'TRUE' THEN
      l_result := TRUE;
    ELSIF UPPER(in_value) = 'FALSE' THEN
      l_result := FALSE;
    ELSIF UPPER(in_value) = 'NULL' THEN 
      l_result := NULL;
    ELSIF in_value IS NULL THEN 
      l_result := NULL;
    ELSE
      cort_exec_pkg.raise_error('Invalid boolean value : '||in_value, -20001);
    END IF;   
    RETURN l_result;
  END str_to_bool;

  -- setter for dynamic SQL - for intenal use only!!!
  PROCEDURE set_params_rec(in_params_rec IN gt_params_rec)
  AS
  BEGIN
    g_params_rec := in_params_rec;
  END set_params_rec;

  -- getter for dynamic SQL - for intenal use only!!!
  FUNCTION get_params_rec
  RETURN gt_params_rec
  AS
  BEGIN
    RETURN g_params_rec;
  END get_params_rec;

  -- return parameter type 
  FUNCTION get_param_type(in_param_name IN VARCHAR2)
  RETURN VARCHAR2
  AS
  BEGIN
    init_session_variables;
    IF g_attr_type_indx.EXISTS(UPPER(in_param_name)) THEN
      RETURN g_attr_type_indx(UPPER(in_param_name));
    ELSE
      cort_exec_pkg.raise_error('Invalid param name : '||in_param_name, -20001);
    END IF;  
  END get_param_type;
  
  -- return TRUE if param with given name exist, otherwise return FALSE
  FUNCTION check_param_type(in_param_name IN VARCHAR2)
  RETURN BOOLEAN
  AS
  BEGIN
    init_session_variables;
    RETURN g_attr_type_indx.EXISTS(UPPER(in_param_name));  
  END check_param_type;

  FUNCTION get_value_by_name(in_param_name IN VARCHAR2)
  RETURN VARCHAR2
  AS
  BEGIN
    IF g_key_value_indx.EXISTS(in_param_name) THEN
      RETURN g_key_value_indx(in_param_name);
    ELSE
      RETURN NULL;
    END IF;  
  END get_value_by_name;

  -- read cort params with default values from cort_params table
  FUNCTION get_default_cort_params
  RETURN gt_params_rec
  AS
    l_def_value_arr   arrays.gt_lstr_arr;
    l_param_name_arr  arrays.gt_str_arr;
    l_param_type_arr  arrays.gt_str_arr;
    l_sql             VARCHAR2(32767);
  BEGIN
    -- read from table
    SELECT param_name, param_type, default_value
      BULK COLLECT 
      INTO l_param_name_arr, l_param_type_arr, l_def_value_arr  
      FROM cort_params;
    
    g_key_value_indx.DELETE;

    -- translate arrays into pl/sql record
    l_sql := '
    DECLARE
      l_rec cort_params_pkg.gt_params_rec;
    BEGIN
      ';
    FOR i IN 1..l_param_name_arr.COUNT LOOP
      g_key_value_indx(l_param_name_arr(i)) := l_def_value_arr(i);
      IF check_param_type(l_param_name_arr(i)) THEN
        CASE l_param_type_arr(i)
        WHEN 'VARCHAR2' THEN
          l_sql := l_sql || 'l_rec.'||l_param_name_arr(i)||' := cort_params_pkg.get_value_by_name('''||l_param_name_arr(i)||''');
        ';
        WHEN 'NUMBER' THEN
          l_sql := l_sql || 'l_rec.'||l_param_name_arr(i)||' := cort_params_pkg.get_value_by_name('''||l_param_name_arr(i)||''');
        ';
        WHEN 'BOOLEAN' THEN
          l_sql := l_sql || 'l_rec.'||l_param_name_arr(i)||' := cort_params_pkg.str_to_bool(cort_params_pkg.get_value_by_name('''||l_param_name_arr(i)||'''));
        ';
        END CASE;  
      END IF;
    END LOOP;
    l_sql := l_sql || 'cort_params_pkg.set_params_rec(l_rec);
    END;';
    
    EXECUTE IMMEDIATE l_sql;

    RETURN g_params_rec;
  END get_default_cort_params;

  -- return parameter default value by name 
  FUNCTION get_param_default_value(
    in_param_name IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_param_type cort_params.param_type%TYPE; 
    l_def_value  cort_params.default_value%TYPE;
  BEGIN
    -- read from table
    SELECT param_type, default_value
      INTO l_param_type, l_def_value  
      FROM cort_params
     WHERE param_name = UPPER(in_param_name); 
    RETURN l_def_value; 
  END get_param_default_value;

  -- set parameter default value 
  PROCEDURE set_param_default_value(
    in_param_name  IN VARCHAR2,
    in_param_value IN VARCHAR2
  )
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    -- write into table
    UPDATE cort_params
       SET default_value = in_param_value
     WHERE param_name = UPPER(in_param_name);

    COMMIT;  
  END set_param_default_value;

  -- return single parameter value by name 
  FUNCTION get_param_value(
    in_params_rec IN gt_params_rec,
    in_param_name IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_type   VARCHAR2(30);
    l_result VARCHAR2(4000); 
    l_sql    VARCHAR2(32767);
  BEGIN
    init_session_variables;
    IF g_attr_type_indx.EXISTS(UPPER(in_param_name)) THEN
      l_type := g_attr_type_indx(UPPER(in_param_name));
      CASE l_type
      WHEN 'VARCHAR2' THEN
        l_result := 'l_result := l_rec.'||in_param_name||';';
      WHEN 'NUMBER' THEN
        l_result := 'l_result := l_rec.'||in_param_name||';';
      WHEN 'BOOLEAN' THEN
        l_result := 'l_result := cort_params_pkg.bool_to_str(l_rec.'||in_param_name||');';
      END CASE;  
      g_params_rec := in_params_rec;
      
      l_sql := '
      DECLARE
        l_rec    cort_params_pkg.gt_params_rec;
        l_result VARCHAR2(4000);
      BEGIN
        l_rec := cort_params_pkg.get_params_rec;
        '||l_result||'
        :out_result := l_result;
      END;';
      
      EXECUTE IMMEDIATE l_sql USING OUT l_result;
      
      RETURN l_result;
    ELSE
      RETURN NULL;
    END IF;
  END get_param_value;
  
  -- set single parameter value by name 
  PROCEDURE set_param_value(
    io_params_rec  IN OUT NOCOPY gt_params_rec,
    in_param_name  IN VARCHAR2,
    in_param_value IN VARCHAR2
  )
  AS
    l_type   VARCHAR2(30);
    l_value  VARCHAR2(4000); 
    l_sql    VARCHAR2(32767);
  BEGIN
    init_session_variables;
    IF g_attr_type_indx.EXISTS(UPPER(in_param_name)) THEN
      IF LENGTH(in_param_value) > 4000 THEN
        cort_exec_pkg.raise_error('Param '||in_param_name||' value is too long', -20001);
      END IF;
      l_type := g_attr_type_indx(UPPER(in_param_name));
      CASE l_type
      WHEN 'VARCHAR2' THEN
        l_value := 'l_rec.'||in_param_name||' := l_value;';
      WHEN 'NUMBER' THEN
        l_value := 'l_rec.'||in_param_name||' := l_value;';
      WHEN 'BOOLEAN' THEN
        l_value := 'l_rec.'||in_param_name||' := cort_params_pkg.str_to_bool(l_value);';
      END CASE;  
      g_params_rec := io_params_rec;
      
      l_sql := '
      DECLARE
        l_rec    cort_params_pkg.gt_params_rec;
        l_value  VARCHAR2(4000);
      BEGIN
        l_rec := cort_params_pkg.get_params_rec;
        l_value := :in_value;
        '||l_value||'
        cort_params_pkg.set_params_rec(l_rec);
      END;';
      
      EXECUTE IMMEDIATE l_sql USING IN in_param_value;
      
      io_params_rec := g_params_rec;
    ELSE
      cort_exec_pkg.raise_error('Invalid param name : '||in_param_name, -20001);
    END IF;
  END set_param_value;

  FUNCTION get_config_param(
    in_param_name IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_value cort_config_params.param_value%TYPE;
  BEGIN
    BEGIN
      SELECT param_value
        INTO l_value
        FROM cort_config_params
       WHERE param_name = UPPER(in_param_name);
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_value := NULL;
    END;  
    RETURN l_value;
  END get_config_param;

END cort_params_pkg;
/
