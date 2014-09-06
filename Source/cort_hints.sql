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
  Description: Script populating cort_hints table      
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added hints APPLICATION, RELEASE, replaced ROLLBACK/NO_ROLLBACK hint with REVERT/NO_REVERT
          |                   | Removed hints LOG/NO_LOG, NO_TEST
  ----------------------------------------------------------------------------------------------------------------------  
*/


PROMPT CORT hints

-- CORT hints
SET FEEDBACK OFF

TRUNCATE TABLE cort_hints;

INSERT INTO cort_hints 
SELECT 'APPLICATION' as hint,
       'application' as param_name,
       '\s*=\s*('||param_value||')' as param_value, 
       'Y' as expression_flag
  FROM cort_config_params
 WHERE param_name = 'APPLICATION_REGEXP';

INSERT INTO cort_hints
SELECT 'RELEASE' as hint,
       'release' as param_name,
       '\s*=\s*('||param_value||')' as param_value, 
       'Y' as expression_flag
  FROM cort_config_params
 WHERE param_name = 'RELEASE_REGEXP';

INSERT INTO cort_hints VALUES('ALIAS',                  'alias',              '\s*=\s*([A-Za-z][A-Za-z0-9_#$]{0,29})', 'Y');

INSERT INTO cort_hints VALUES('PARALLEL',               'parallel',           '\s*=\s*([0-9]{1,5})', 'Y');
INSERT INTO cort_hints VALUES('NO_PARALLEL',            'parallel',           NULL,    'N');

INSERT INTO cort_hints VALUES('TEST',                   'test',               'TRUE',  'N');

INSERT INTO cort_hints VALUES('DEBUG',                  'debug',              'TRUE',  'N');
INSERT INTO cort_hints VALUES('NO_DEBUG',               'debug',              'FALSE', 'N');

INSERT INTO cort_hints VALUES('ECHO',                   'echo',               'TRUE',  'N');
INSERT INTO cort_hints VALUES('NO_ECHO',                'echo',               'FALSE', 'N');

INSERT INTO cort_hints VALUES('ALTER',                  'force_recreate',     'FALSE', 'N');
INSERT INTO cort_hints VALUES('NO_ALTER',               'force_recreate',     'TRUE',  'N');

INSERT INTO cort_hints VALUES('MOVE',                   'force_move',         'FALSE', 'N');
INSERT INTO cort_hints VALUES('NO_MOVE' ,               'force_move',         'TRUE',  'N');

INSERT INTO cort_hints VALUES('REVERT',                 'revertable',         'TRUE',  'N');
INSERT INTO cort_hints VALUES('NO_REVERT',              'revertable',         'FALSE', 'N');

INSERT INTO cort_hints VALUES('PHYSICAL_ATTRIBUTES',    'physical_attr',      'TRUE',  'N');
INSERT INTO cort_hints VALUES('NO_PHYSICAL_ATTRIBUTES', 'physical_attr',      'FALSE', 'N');

INSERT INTO cort_hints VALUES('KEEP_DATA',              'keep_data',          'TRUE',  'N');
INSERT INTO cort_hints VALUES('NO_DATA',                'keep_data',          'FALSE', 'N');

INSERT INTO cort_hints VALUES('KEEP_REFS',              'keep_refs',          'TRUE',  'N');
INSERT INTO cort_hints VALUES('NO_REFS',                'keep_refs',          'FALSE', 'N');

INSERT INTO cort_hints VALUES('VALIDATE',               'validate_refs',      'TRUE',  'N');
INSERT INTO cort_hints VALUES('NO_VALIDATE',            'validate_refs',      'FALSE', 'N');

INSERT INTO cort_hints VALUES('KEEP_BAD_REFS',          'keep_bad_refs',      'TRUE',  'N');
INSERT INTO cort_hints VALUES('NO_BAD_REFS',            'keep_bad_refs',      'FALSE', 'N');

INSERT INTO cort_hints VALUES('KEEP_PRIVS',             'keep_privs',         'TRUE',  'N');
INSERT INTO cort_hints VALUES('NO_PRIVS',               'keep_privs',         'FALSE', 'N');

INSERT INTO cort_hints VALUES('KEEP_INDEXES',           'keep_indexes',       'TRUE',  'N');
INSERT INTO cort_hints VALUES('NO_INDEXES',             'keep_indexes',       'FALSE', 'N');

INSERT INTO cort_hints VALUES('KEEP_TRIGGERS',          'keep_triggers',      'TRUE',  'N');
INSERT INTO cort_hints VALUES('NO_TRIGGERS',            'keep_triggers',      'FALSE', 'N');

INSERT INTO cort_hints VALUES('KEEP_POLICIES',          'keep_policies',      'TRUE',  'N');
INSERT INTO cort_hints VALUES('NO_POLICIES',            'keep_policies',      'FALSE', 'N');

INSERT INTO cort_hints VALUES('KEEP_COMMENTS',          'keep_comments',      'TRUE',  'N');
INSERT INTO cort_hints VALUES('NO_COMMENTS',            'keep_comments',      'FALSE', 'N');

INSERT INTO cort_hints VALUES('KEEP_STATS',             'keep_stats',         'TRUE',  'N');
INSERT INTO cort_hints VALUES('NO_STATS',               'keep_stats',         'FALSE', 'N');

INSERT INTO cort_hints VALUES('KEEP_PARTITIONS',        'keep_partitions',    'TRUE',  'N');
INSERT INTO cort_hints VALUES('NO_PARTITIONS',          'keep_partitions',    'FALSE', 'N');

INSERT INTO cort_hints VALUES('KEEP_SUBPARTITIONS',     'keep_subpartitions', 'TRUE',  'N');
INSERT INTO cort_hints VALUES('NO_SUBPARTITIONS',       'keep_subpartitions', 'FALSE', 'N');

INSERT INTO cort_hints VALUES('KEEP_TEMP_TABLE',        'keep_temp_table',    'TRUE',  'N');
INSERT INTO cort_hints VALUES('DROP_TEMP_TABLE',        'keep_temp_table',    'FALSE', 'N');

COMMIT;

SET FEEDBACK ON

PROMPT 
