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
  Description: Accept only 0, 1, 2 values
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01.1 | Rustam Kafarov    | Accept only Y or N value
  14.02   | Rustam Kafarov    | Accept only 1, 2 or 0 values
  ----------------------------------------------------------------------------------------------------------------------  
*/

ACCEPT v CHAR FORMAT A1 PROMPT "&2 > "

SET TERM OFF
SPOOL OFF

COLUMN c_check_result NEW_VALUE check_result PRINT
COLUMN c_prompt_text NEW_VALUE  prompt_text PRINT

SELECT CASE 
       WHEN '&v' = '0' THEN 'exit.sql "Installation interrupted"' 
       WHEN '&v' = '1' THEN 'dummy.sql' 
       WHEN '&v' = '2' THEN 'dummy.sql'
       ELSE 'accept.sql &1 "&2"'
       END AS c_check_result,
       CASE 
       WHEN '&v' = '0' THEN 'Exiting ...'
       WHEN '&v' = '1' THEN null
       WHEN '&v' = '2' THEN null
       ELSE 'Invalid input. Please try again'
       END AS c_prompt_text
  FROM dual;

SPOOL install.log APPEND
SET TERM ON

PROMPT &prompt_text
@&check_result

DEFINE &1 = &v