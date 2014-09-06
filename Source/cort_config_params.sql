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
  Description: Default value for configuration parameters
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Added general configuration parameters for better customization
  ----------------------------------------------------------------------------------------------------------------------  
*/

PROMPT CORT configuration params

-- CORT params

SET FEEDBACK OFF

TRUNCATE TABLE cort_config_params;

--                                     NAME                  VALUE
INSERT INTO cort_config_params VALUES('CORT_TEXT_PREFIX',    '#');
INSERT INTO cort_config_params VALUES('CORT_VALUE',          '=');
INSERT INTO cort_config_params VALUES('CORT_FORCE_VALUE',    '==');
INSERT INTO cort_config_params VALUES('APPLICATION_REGEXP',  '[A-Za-z][A-z0-9_]{0,19}');
INSERT INTO cort_config_params VALUES('RELEASE_REGEXP',      '[A-Za-z][A-z0-9_\.\-]{0,19}');
INSERT INTO cort_config_params VALUES('CORT_RLBK_PREFIX',    'rlbk#');
INSERT INTO cort_config_params VALUES('CORT_TEMP_PREFIX',    '~tmp#');
INSERT INTO cort_config_params VALUES('CORT_SWAP_PREFIX',    '~swp#');
INSERT INTO cort_config_params VALUES('CORT_STAT_TABLE',     'CORT_STAT');
INSERT INTO cort_config_params VALUES('BEGIN_MESSAGE',       'Create or replace [object_type] "[object_owner]"."[object_name]" ...');
INSERT INTO cort_config_params VALUES('END_MESSAGE',         ' ');

COMMIT;

SET FEEDBACK ON

PROMPT 
