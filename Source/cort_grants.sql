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
  Description: Grants and public synonyms for CORT objects
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added synonyms and grants on new package cort_session_pkg and tables cort_builds, plan_table$ 
  ----------------------------------------------------------------------------------------------------------------------  
*/

GRANT EXECUTE ON cort_pkg          TO PUBLIC;
GRANT EXECUTE ON cort_session_pkg  TO PUBLIC;
GRANT SELECT  ON cort_log          TO PUBLIC;
GRANT SELECT  ON cort_job_log      TO PUBLIC;
GRANT SELECT  ON cort_jobs         TO PUBLIC;
GRANT SELECT  ON cort_objects      TO PUBLIC;
GRANT SELECT  ON cort_builds       TO PUBLIC;

GRANT SELECT, INSERT, UPDATE, DELETE ON plan_table TO PUBLIC;

CREATE OR REPLACE PUBLIC SYNONYM cort_pkg          FOR cort_pkg;
CREATE OR REPLACE PUBLIC SYNONYM cort_session_pkg  FOR cort_session_pkg;
CREATE OR REPLACE PUBLIC SYNONYM cort_log          FOR cort_log;
CREATE OR REPLACE PUBLIC SYNONYM cort_job_log      FOR cort_job_log; 
CREATE OR REPLACE PUBLIC SYNONYM cort_jobs         FOR cort_jobs; 
CREATE OR REPLACE PUBLIC SYNONYM cort_objects      FOR cort_objects; 
CREATE OR REPLACE PUBLIC SYNONYM cort_builds       FOR cort_builds;
CREATE OR REPLACE PUBLIC SYNONYM plan_table        FOR plan_table;
