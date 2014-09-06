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
  Description: Trigger to emulate locking on changing by CORT tables
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.02   | Rustam Kafarov    | Prevent and DDL on tables while they are modifing by CORT
  ----------------------------------------------------------------------------------------------------------------------  
*/

@DROP TRIGGER cort_lock_table_trg

CREATE OR REPLACE TRIGGER cort_lock_table_trg BEFORE DDL ON &trigger_scope
WHEN (
      (cort_trg_pkg.get_status = 'ENABLED') AND 
      (ora_dict_obj_type = 'TABLE') AND
      (ora_dict_obj_owner NOT IN ('SYS','SYSTEM')) 
     )
DECLARE
  l_sid         VARCHAR2(30);
  l_job_sid     VARCHAR2(30);
BEGIN
  BEGIN
    SELECT sid, job_sid
      INTO l_sid, l_job_sid  
      FROM cort_jobs
     WHERE object_type = 'TABLE'
       AND object_owner = NVL(ora_dict_obj_owner,'"NULL"')
       AND object_name = ora_dict_obj_name
       AND DECODE(status, 'RUNNING', '0', sid) = '0';
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      l_sid := NULL;
      l_job_sid := NULL;
  END;     
  IF (l_sid IS NOT NULL AND dbms_session.is_session_alive(l_sid)) OR
     (l_job_sid IS NOT NULL AND dbms_session.is_session_alive(l_job_sid))
  THEN   
    RAISE_APPLICATION_ERROR(-20000, 'Table "'||ora_dict_obj_owner||'"."'||ora_dict_obj_name||'" is changing by another CORT session');
  END IF;  
END;  
/

