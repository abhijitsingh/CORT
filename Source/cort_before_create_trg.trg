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
  Description: Before create trigger to register recreatable objects change
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.02   | Rustam Kafarov    | Added support build functionality
  ----------------------------------------------------------------------------------------------------------------------  
*/

@DROP TRIGGER cort_before_create_trg 

CREATE OR REPLACE TRIGGER cort_before_create_trg BEFORE CREATE ON &trigger_scope
WHEN (
      (cort_trg_pkg.get_status = 'ENABLED') AND 
      (NVL(ora_dict_obj_owner,'?') NOT IN ('SYS','SYSTEM')) AND 
      (ora_dict_obj_type IN ('PACKAGE','PACKAGE BODY','PROCEDURE','FUNCTION','TRIGGER','VIEW','SYNONYM','TYPE','TYPE BODY','JAVA','CONTEXT','LIBRARY')) AND 
      (cort_trg_pkg.get_execution_mode(ora_dict_obj_type) = 'REPLACE')
     )
BEGIN
  cort_trg_pkg.before_create;
END;  
/
