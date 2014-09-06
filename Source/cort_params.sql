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
  Description: Script for population cort_params table
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Changed params: VERSION->RELEASE, ROLLBACK->REVERT. Removed param LOG (now logging is always on)
  ----------------------------------------------------------------------------------------------------------------------  
*/

PROMPT CORT params

-- CORT params

SET FEEDBACK OFF

TRUNCATE TABLE cort_params;

--                              PARAM                 TYPE       DEFAULT VALUE
INSERT INTO cort_params VALUES('APPLICATION',        'VARCHAR2','');
INSERT INTO cort_params VALUES('RELEASE',            'VARCHAR2','');
INSERT INTO cort_params VALUES('ALIAS',              'VARCHAR2','A');
INSERT INTO cort_params VALUES('PARALLEL',           'NUMBER',  '1');
INSERT INTO cort_params VALUES('DEBUG',              'BOOLEAN', 'FALSE');
INSERT INTO cort_params VALUES('ECHO',               'BOOLEAN', 'TRUE');
INSERT INTO cort_params VALUES('TEST',               'BOOLEAN', 'FALSE');
INSERT INTO cort_params VALUES('FORCE_RECREATE',     'BOOLEAN', 'FALSE');
INSERT INTO cort_params VALUES('FORCE_MOVE',         'BOOLEAN', 'FALSE');
INSERT INTO cort_params VALUES('REVERTABLE',         'BOOLEAN', 'TRUE');
INSERT INTO cort_params VALUES('PHYSICAL_ATTR',      'BOOLEAN', 'FALSE');
INSERT INTO cort_params VALUES('KEEP_DATA',          'BOOLEAN', 'TRUE');
INSERT INTO cort_params VALUES('KEEP_REFS',          'BOOLEAN', 'TRUE');
INSERT INTO cort_params VALUES('VALIDATE_REFS',      'BOOLEAN', 'TRUE');
INSERT INTO cort_params VALUES('KEEP_BAD_REFS',      'BOOLEAN', 'TRUE');
INSERT INTO cort_params VALUES('KEEP_PRIVS',         'BOOLEAN', 'TRUE');
INSERT INTO cort_params VALUES('KEEP_INDEXES',       'BOOLEAN', 'TRUE');
INSERT INTO cort_params VALUES('KEEP_TRIGGERS',      'BOOLEAN', 'TRUE');
INSERT INTO cort_params VALUES('KEEP_POLICIES',      'BOOLEAN', 'TRUE');
INSERT INTO cort_params VALUES('KEEP_COMMENTS',      'BOOLEAN', 'TRUE');
INSERT INTO cort_params VALUES('KEEP_STATS',         'BOOLEAN', 'TRUE');
INSERT INTO cort_params VALUES('KEEP_PARTITIONS',    'BOOLEAN', 'FALSE');
INSERT INTO cort_params VALUES('KEEP_SUBPARTITIONS', 'BOOLEAN', 'FALSE');
INSERT INTO cort_params VALUES('KEEP_TEMP_TABLE',    'BOOLEAN', 'FALSE');

COMMIT;

SET FEEDBACK ON

PROMPT 
