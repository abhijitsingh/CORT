
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
  Description: Generic script execution another script conditionally
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.02   | Rustam Kafarov    | If condition from param 1 is true then execute script from param 2
  ----------------------------------------------------------------------------------------------------------------------  
*/

DEFINE condition = "&1" 
DEFINE file_name = "&2" 

SET TERM OFF
SPOOL OFF

COLUMN c_check_result NEW_VALUE check_result NOPRINT

SELECT CASE WHEN &condition THEN q'{&file_name}' ELSE 'dummy.sql' END AS c_check_result
  FROM dual;

SPOOL install.log APPEND
SET TERM ON

@&check_result
