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
  Description: Check default tablspace privilege is granted
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.02   | Rustam Kafarov    | Checks that access privilege on default tablespace is granted
  ----------------------------------------------------------------------------------------------------------------------  
*/

-- Check default tablespace privileges
  
SET TERM OFF
SPOOL OFF

COLUMN c_check_result NEW_VALUE check_result NOPRINT
COLUMN c_prompt_text NEW_VALUE prompt_text NOPRINT

SELECT * 
  FROM (SELECT NVL2(tb.tablespace_name, 
                   'privilege on default tablespace '||u.default_tablespace||' is found', 
                   'privilege on default tablespace '||u.default_tablespace||' not found') AS c_prompt_text,
               NVL2(tb.tablespace_name, 
                   'dummy.sql', 
                   'exit.sql "You need to grant privilege on default tablespace '||u.default_tablespace||' to &_user user"') AS c_check_result
          FROM user_users u
          LEFT JOIN user_tablespaces tb
            ON tb.tablespace_name = u.default_tablespace
        )        
 WHERE ROWNUM = 1;          

SPOOL install.log APPEND
SET TERM ON

PROMPT &prompt_text

@&check_result