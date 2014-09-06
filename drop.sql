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
  Description: drop object only if it exists
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01.1 | Rustam Kafarov    | Smart dropping of object
  14.02   | Rustam Kafarov    | Added using of substitution variables
  ----------------------------------------------------------------------------------------------------------------------  
*/

-- Params:
-- 1 - object type
-- 2 - object name

SET DEFINE ON
SET VERIFY OFF
SET FEEDBACK OFF

DECLARE
  l_cnt         NUMBER;
  l_sql         VARCHAR2(32767);
  l_object_type VARCHAR2(30) := UPPER('&1');
  l_object_name VARCHAR2(30) := UPPER('&2');
BEGIN  
  SELECT COUNT(*)
    INTO l_cnt 
    FROM user_objects
   WHERE object_type = l_object_type
     AND object_name = l_object_name;
  IF l_cnt = 1 THEN
    l_sql := 'DROP '||l_object_type||' '||l_object_name;
    IF l_object_type = 'TABLE' THEN
      l_sql := l_sql || ' CASCADE CONSTRAINTS'; 
    END IF;
    dbms_output.put_line(l_sql||chr(10));
    EXECUTE IMMEDIATE l_sql;
  END IF;   
END;     
/

SET FEEDBACK ON