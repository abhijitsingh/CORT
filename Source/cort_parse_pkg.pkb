CREATE OR REPLACE PACKAGE BODY cort_parse_pkg
AS
 
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
  Description: Parser utility for SQL commands and CORT hints 
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added support of indexes and sequences, create table as select
  ----------------------------------------------------------------------------------------------------------------------  
*/

  TYPE gt_sql_positions IS RECORD(
    cort_param_start_pos       PLS_INTEGER,
    cort_param_end_pos         PLS_INTEGER,
    schema_name                VARCHAR2(30),
    definition_start_pos       PLS_INTEGER,
    columns_start_pos          PLS_INTEGER,
    columns_end_pos            PLS_INTEGER,
    partition_type             VARCHAR2(30),
    subpartition_type          VARCHAR2(30),
    partitions_start_pos       PLS_INTEGER,
    partitions_end_pos         PLS_INTEGER,
    as_select_start_pos        PLS_INTEGER,
    subquery_start_pos         PLS_INTEGER,
    table_definition_start_pos PLS_INTEGER,   -- index's table start definition (after keyword ON {CLUSTER})
    is_cluster                 BOOLEAN,       -- is index created in cluster
    table_name                 VARCHAR2(30),  -- index's table name
    table_owner                VARCHAR2(30)   -- index's owner name
  );
  g_sql_positions  gt_sql_positions;

  TYPE gt_cort_text_rec IS RECORD(
    text_type      VARCHAR2(30),    -- Could be: COMMENT, LINE COMMENT, LITERAL, CORT PARAM, QUOTED NAME
    text           VARCHAR2(32767), -- original text  
    start_position PLS_INTEGER,     -- start position in original SQL text (array index)
    end_position   PLS_INTEGER      -- end position in original SQL text
  );

  TYPE gt_cort_text_arr IS TABLE OF gt_cort_text_rec INDEX BY PLS_INTEGER;

  TYPE gt_hint_arr      IS TABLE OF cort_hints%ROWTYPE INDEX BY VARCHAR2(30); 

  TYPE gt_replace_rec IS RECORD(
    object_type    VARCHAR2(30),
    object_name    VARCHAR2(30),
    start_pos      PLS_INTEGER, 
    end_pos        PLS_INTEGER,
    new_name       VARCHAR2(100)
  );
  
  TYPE gt_replace_arr IS TABLE OF gt_replace_rec INDEX BY PLS_INTEGER;
  
  g_normalized_sql        CLOB;
  g_cort_text_arr         gt_cort_text_arr; -- indexed by start position
  g_replace_arr           gt_replace_arr;
  g_temp_name_arr         arrays.gt_str_indx;

  g_params                cort_params_pkg.gt_params_rec;

  PROCEDURE debug(
    in_text  IN VARCHAR2
  )
  AS
  BEGIN
    cort_exec_pkg.debug(in_text);
  END debug;

  /*  Masks all reg exp key symbols */
  FUNCTION get_regexp_const(
    in_value          IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    TYPE t_regexp_keys  IS TABLE OF VARCHAR2(1);
    TYPE t_regexp_masks IS TABLE OF VARCHAR2(4);
    l_regexp_keys  t_regexp_keys  := t_regexp_keys(  '\',  '[',  ']',  '*',  '?',  '.',  '+',  '*',  '-',  '^',  '{',  '}',  '|',  '$',  '(',  ')');
    l_regexp_masks t_regexp_masks := t_regexp_masks('\\', '\[', '\]', '\*', '\?', '\.', '\+', '\*', '\-', '\^', '\{', '\}', '\|', '\$', '\(', '\)');
    l_value   VARCHAR2(32767);
  BEGIN
    l_value := in_value;
    FOR I IN 1..l_regexp_keys.COUNT LOOP
      l_value := REPLACE(l_value, l_regexp_keys(i), l_regexp_masks(i));
    END LOOP;
    RETURN l_value;
  END get_regexp_const;

  /* Return TRUE is given name is simple SQL name and doesnt require double quotes */
  FUNCTION is_simple_name(in_name IN VARCHAR2)
  RETURN BOOLEAN
  AS
  BEGIN
    RETURN REGEXP_LIKE(in_name, '^[A-Z][A-Z0-9_$#]{0,29}$');
  END is_simple_name;

  FUNCTION get_parse_only_sql(
    in_name  IN VARCHAR2,
    in_delim IN VARCHAR2 DEFAULT NULL
  )
  RETURN VARCHAR2
  AS
    l_regexp VARCHAR2(200);
  BEGIN
    IF is_simple_name(in_name) THEN
      l_regexp := '('||get_regexp_const(in_name)||')'||in_delim;
    ELSE
      l_regexp := '("'||get_regexp_const(in_name)||'")';
    END IF;
    RETURN l_regexp;
  END get_parse_only_sql;

  FUNCTION get_owner_name_regexp(
    in_name  IN VARCHAR2,
    in_owner IN VARCHAR2,
    in_delim IN VARCHAR2 DEFAULT NULL
  )
  RETURN VARCHAR2 
  AS
    l_regexp VARCHAR2(200);
  BEGIN
    IF is_simple_name(in_owner) THEN
      l_regexp := get_regexp_const(in_owner);
    ELSE
      l_regexp := '"'||get_regexp_const(in_owner)||'"';
    END IF;
    RETURN '('||l_regexp||'\s*\.\s*)?'||get_parse_only_sql(in_name, in_delim);
  END get_owner_name_regexp;

  /* Returns regular expression to find given constraint of gven type */
  FUNCTION get_column_regexp(
    in_column_rec  IN  cort_exec_pkg.gt_column_rec 
  )
  RETURN VARCHAR2
  AS
    TYPE t_type_exprs  IS TABLE OF VARCHAR2(4000) INDEX BY VARCHAR2(30);
    l_type_expr_arr       t_type_exprs;
    l_regexp              VARCHAR2(1000);
  BEGIN
    l_type_expr_arr('NUMBER') := '((NUMBER)|(INTEGER)|(INT)|(SMALLINT)|(DECIMAL)|(NUMERIC)|(DEC))';
    l_type_expr_arr('FLOAT') := '((FLOAT)|(REAL)|(DOUBLE\s+PRECISION))';
    l_type_expr_arr('BINARY_DOUBLE') := '((BINARY_DOUBLE)|(BINARY_FLOAT))';
    l_type_expr_arr('VARCHAR2') := '((VARCHAR2)|(VARCHAR)|(CHARACTER\s+VARYING)|(CHAR\s+VARYING))';
    l_type_expr_arr('CHAR') := '((CHAR)|(CHARACTER))';
    l_type_expr_arr('NVARCHAR2') := '((NVARCHAR2)|(NATIONAL\s+CHARACTER\s+VARYING)|(NATIONAL\s+CHAR\s+VARYING)|(NCHAR\s+VARYING))';
    l_type_expr_arr('NCHAR') := '((NCHAR)|(NATIONAL\s+CHARACTER)|(NATIONAL\s+CHAR))';
    l_type_expr_arr('TIMESTAMP') := '(TIMESTAMP(\([0-9]\))?)';
    l_type_expr_arr('TIMESTAMP WITH TIMEZONE') := '(TIMESTAMP\s*(\(\s*[0-9]\s*\))?\s*WITH\s+TIME\s+ZONE)';
    l_type_expr_arr('TIMESTAMP WITH LOCAL TIMEZONE') := '(TIMESTAMP\s*(\(\s*[0-9]\s*\))?\s+WITH\s+LOCAL\s+TIME\s+ZONE)';
    l_type_expr_arr('INTERVAL YEAR TO MONTH') := '(INTERVAL\s+YEAR\s*(\(\s*[0-9]\s*\))?\s+TO\s+MONTH)';
    l_type_expr_arr('RAW') := '(RAW\s*\(\s*[0-9]+\s*\))';

    l_regexp := '('||get_parse_only_sql(in_column_rec.column_name, '\s')||'\s*';
    IF in_column_rec.data_type_mod IS NOT NULL THEN
      l_regexp := l_regexp||get_regexp_const(in_column_rec.data_type_mod)||'\s+';
    END IF;
    
    IF in_column_rec.virtual_column = 'YES' THEN
      l_regexp := l_regexp||'(GENERATED\s+ALWAYS\s+)?AS)\W';
    ELSE 
      IF in_column_rec.data_type_owner IS NOT NULL THEN
        l_regexp := l_regexp||get_owner_name_regexp(in_column_rec.data_type, in_column_rec.data_type_owner, ')\W');
      ELSE
        IF l_type_expr_arr.EXISTS(in_column_rec.data_type) THEN
          l_regexp := l_regexp||l_type_expr_arr(in_column_rec.data_type)||')\W';
        ELSE
          l_regexp := l_regexp||get_regexp_const(in_column_rec.data_type)||')\W';
        END IF;
      END IF;
    END IF;
    RETURN l_regexp;
  END get_column_regexp;

  -- Returns position of close bracket - ) - ignoring all nested pairs of brackets ( ) and quoted SQL names
  FUNCTION get_closed_bracket(
    in_sql              IN CLOB,
    in_search_position  IN PLS_INTEGER -- Position AFTER open bracket
  )
  RETURN PLS_INTEGER
  AS
    l_search_pos         PLS_INTEGER;
    l_open_bracket_cnt   PLS_INTEGER;
    l_close_bracket_cnt  PLS_INTEGER;
    l_key_pos            PLS_INTEGER;
    l_key                VARCHAR2(1);
  BEGIN
    l_search_pos := in_search_position;
    l_open_bracket_cnt := 1;
    l_close_bracket_cnt := 0;
    LOOP
      l_key_pos := REGEXP_INSTR(in_sql, '\(|\)|"', l_search_pos, 1, 0);
      EXIT WHEN l_key_pos = 0 OR l_key_pos IS NULL
             OR l_open_bracket_cnt > 4000 OR l_close_bracket_cnt > 4000;
      l_key := SUBSTR(in_sql, l_key_pos, 1);
      CASE l_key
        WHEN '(' THEN
          l_open_bracket_cnt := l_open_bracket_cnt + 1;
        WHEN ')' THEN
          l_close_bracket_cnt := l_close_bracket_cnt + 1;
        WHEN '"' THEN
          l_key_pos := REGEXP_INSTR(in_sql, '"', l_search_pos, 1, 0);
      END CASE;
      IF l_open_bracket_cnt = l_close_bracket_cnt THEN
        RETURN l_key_pos;
      END IF;
      l_search_pos := l_key_pos + 1;
    END LOOP;
  END get_closed_bracket;


  PROCEDURE add_text(
    io_text_arr  IN OUT NOCOPY gt_cort_text_arr, 
    in_text_type IN VARCHAR2,
    in_text      IN VARCHAR2,
    in_start_pos IN PLS_INTEGER,
    in_end_pos   IN PLS_INTEGER
  )
  AS
    l_text_rec gt_cort_text_rec;
  BEGIN
    l_text_rec.text_type := in_text_type;
    l_text_rec.text := in_text;
    l_text_rec.start_position := in_start_pos;
    l_text_rec.end_position := in_end_pos;
    io_text_arr(in_start_pos) := l_text_rec;
  END add_text;

  -- find all entries for given name 
  FUNCTION find_substitable_name(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_new_name     IN VARCHAR2,
    in_pattern      IN VARCHAR2,
    in_search_pos   IN PLS_INTEGER DEFAULT 1,
    in_subexpr      IN PLS_INTEGER DEFAULT NULL
  )
  RETURN PLS_INTEGER
  AS
    l_replace_rec gt_replace_rec;
    l_start_pos   PLS_INTEGER;
  BEGIN
    l_start_pos := REGEXP_INSTR(g_normalized_sql, in_pattern, in_search_pos, 1, 0, NULL, in_subexpr);
    IF l_start_pos > 0 THEN 
      debug('found substitutable name @'||l_start_pos||'. Pattern = '||in_pattern);
      l_replace_rec.object_type := in_object_type;
      l_replace_rec.object_name := in_object_name;
      l_replace_rec.start_pos := l_start_pos;
      l_replace_rec.end_pos   := REGEXP_INSTR(g_normalized_sql, in_pattern, in_search_pos, 1, 1, NULL, in_subexpr);
      l_replace_rec.new_name  := in_new_name;
      g_replace_arr(l_start_pos) := l_replace_rec;
      g_temp_name_arr(in_object_type||':"'||in_new_name||'"') := in_object_name;
      RETURN l_replace_rec.end_pos;
    ELSE
      RETURN 0;
    END IF;
  END find_substitable_name;

  -- find all entries for given table name 
  PROCEDURE find_table_name(
    in_table_name  IN VARCHAR2,
    in_table_owner IN VARCHAR2,
    in_temp_name   IN VARCHAR2
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
    l_save_pos    PLS_INTEGER;
  BEGIN
    -- find table declaration
    l_search_pos := 1;    
    l_pattern := '\WTABLE\s+'||get_owner_name_regexp(in_table_name, in_table_owner)||'(\s|\()';
    LOOP
      EXIT WHEN l_search_pos = 0;              
      l_search_pos := find_substitable_name(
                        in_object_type  => 'TABLE',
                        in_object_name  => in_table_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 2
                      );
    END LOOP;                
    -- find self references   
    l_search_pos := g_sql_positions.columns_start_pos;
    l_pattern := '\WREFERENCES\s+'||get_owner_name_regexp(in_table_name, in_table_owner)||'(\s|\()';
    LOOP
      EXIT WHEN l_search_pos = 0;              
      l_search_pos := find_substitable_name(
                        in_object_type  => 'TABLE',
                        in_object_name  => in_table_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 2
                      );
    END LOOP;                
    -- find create index statement
    l_pattern := '\WON\s+'||get_owner_name_regexp(in_table_name, in_table_owner)||'(\s|\()';
    l_search_pos := g_sql_positions.columns_start_pos;
    LOOP
      EXIT WHEN l_search_pos = 0;              
      l_search_pos := find_substitable_name(
                        in_object_type  => 'TABLE',
                        in_object_name  => in_table_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 2
                      );
    END LOOP;
  END find_table_name;

  -- find all entries for given constraint name 
  PROCEDURE find_constraint_name(
    in_constraint_name IN VARCHAR2,
    in_temp_name       IN VARCHAR2
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
    l_replace_rec gt_replace_rec;
    l_start_pos   PLS_INTEGER;
  BEGIN
    l_search_pos := g_sql_positions.columns_start_pos;
    l_pattern := '\WCONSTRAINT\s+'||get_parse_only_sql(in_constraint_name, '\s')||'\s*((PRIMARY\s+KEY)|(UNIQUE)|(CHECK)|(FOREIGN\s+KEY))';
    debug(l_pattern);
    LOOP
      EXIT WHEN l_search_pos = 0;               
      l_search_pos := find_substitable_name(
                        in_object_type  => 'CONSTRAINT',
                        in_object_name  => in_constraint_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 1
                      );
      debug('l_search_pos = '||l_search_pos);
    END LOOP;                  
    l_search_pos := g_sql_positions.columns_end_pos;
    l_pattern := '\WPARTITION\s+BY\s+REFERENCE\s*\(\s*'||get_parse_only_sql(in_constraint_name)||'\s*\)';
    LOOP
      EXIT WHEN l_search_pos = 0;               
      l_search_pos := find_substitable_name(
                        in_object_type  => 'CONSTRAINT',
                        in_object_name  => in_constraint_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 1
                      );
    END LOOP;                  
  END find_constraint_name;

  -- find all entries for given constraint name 
  PROCEDURE find_log_group_name(
    in_log_group_name IN VARCHAR2,
    in_temp_name      IN VARCHAR2
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
    l_replace_rec gt_replace_rec;
    l_start_pos   PLS_INTEGER;
  BEGIN
    l_search_pos := g_sql_positions.columns_start_pos;
    l_pattern := '\WSUPPLEMENTAL\s+LOG\s+GROUP\s+'||get_parse_only_sql(in_log_group_name, '\W');
    LOOP
      EXIT WHEN l_search_pos = 0;               
      l_search_pos := find_substitable_name(
                        in_object_type  => 'LOG_GROUP',
                        in_object_name  => in_log_group_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 1
                      );
    END LOOP;                  
  END find_log_group_name;
  
  -- find all entries for given constraint name 
  PROCEDURE find_index_name(
    in_index_name IN VARCHAR2,
    in_temp_name  IN  VARCHAR2
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
    l_replace_rec gt_replace_rec;
    l_start_pos   PLS_INTEGER;
  BEGIN
    l_search_pos := g_sql_positions.columns_start_pos;
    l_pattern := '\WOIDINDEX\s*'||get_parse_only_sql(in_index_name)||'\s*\(';
    LOOP
      EXIT WHEN l_search_pos = 0;               
      l_search_pos := find_substitable_name(
                        in_object_type  => 'INDEX',
                        in_object_name  => in_index_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 1
                      );
    END LOOP;                
    l_search_pos := g_sql_positions.columns_start_pos;
    l_pattern := '\WINDEX\s*'||get_parse_only_sql(in_index_name, '\W');
    LOOP
      EXIT WHEN l_search_pos = 0;               
      l_search_pos := find_substitable_name(
                        in_object_type  => 'INDEX',
                        in_object_name  => in_index_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 1
                      );
    END LOOP;                
  END find_index_name;
  

  -- find all entries for given lob column 
  PROCEDURE find_lob_segment_name(
    in_column_name  IN VARCHAR2,
    in_segment_name IN VARCHAR2,
    in_temp_name    IN VARCHAR2
  )
  AS                        
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
    l_replace_rec gt_replace_rec;
    l_start_pos   PLS_INTEGER;
  BEGIN
    l_search_pos := g_sql_positions.columns_end_pos;
    l_pattern := '\WLOB\s*\(\s*'||get_parse_only_sql(in_column_name)||'\s*\)\s+STORE\s+AS\s+(BASICFILE\s+|SECUREFILE\s+)?'||get_parse_only_sql(in_segment_name, '\W');
    LOOP
      EXIT WHEN l_search_pos = 0;               
      l_search_pos := find_substitable_name(
                        in_object_type  => 'LOB',
                        in_object_name  => in_segment_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 3
                      );
    END LOOP;                  
    l_search_pos := g_sql_positions.columns_end_pos;
    l_pattern := '\WSTORE\s+AS\s+(BASICFILE\s+|SECUREFILE\s+)?(LOB|CLOB|BINARY\s+XML)\s+'||get_parse_only_sql(in_segment_name, '\W');
    LOOP
      EXIT WHEN l_search_pos = 0;               
      l_search_pos := find_substitable_name(
                        in_object_type  => 'LOB',
                        in_object_name  => in_segment_name,
                        in_new_name     => in_temp_name,
                        in_pattern      => l_pattern,
                        in_search_pos   => l_search_pos,
                        in_subexpr      => 3
                      );
    END LOOP;                  
  END find_lob_segment_name;

  -- find all substitution entries (CREATE TABLE)
  PROCEDURE find_all_substitutions(
    in_table_rec IN cort_exec_pkg.gt_table_rec
  )
  AS
    l_lob_rec   cort_exec_pkg.gt_lob_rec;
    l_indx      PLS_INTEGER; 
  BEGIN
    g_replace_arr.DELETE;
    g_temp_name_arr.DELETE;

    -- find all instances for table name
    find_table_name(
      in_table_name  => in_table_rec.table_name, 
      in_table_owner => in_table_rec.owner, 
      in_temp_name   => in_table_rec.rename_rec.temp_name
    );
    -- find all named constraints
    FOR i IN 1..in_table_rec.constraint_arr.COUNT LOOP
      IF in_table_rec.constraint_arr(i).generated = 'USER NAME' THEN
        find_constraint_name(
          in_constraint_name => in_table_rec.constraint_arr(i).constraint_name, 
          in_temp_name       => in_table_rec.constraint_arr(i).rename_rec.temp_name
        );
      END IF;  
    END LOOP;
    -- find all named log groups
    FOR i IN 1..in_table_rec.log_group_arr.COUNT LOOP
      IF in_table_rec.log_group_arr(i).generated = 'USER NAME' THEN
        find_log_group_name(
          in_log_group_name => in_table_rec.log_group_arr(i).log_group_name, 
          in_temp_name      => in_table_rec.log_group_arr(i).rename_rec.temp_name
        );
      END IF;  
    END LOOP;
    -- find all indexes 
    FOR i IN 1..in_table_rec.index_arr.COUNT LOOP
      IF in_table_rec.index_arr(i).rename_rec.generated = 'N' THEN
        find_index_name(
          in_index_name => in_table_rec.index_arr(i).index_name, 
          in_temp_name  => in_table_rec.index_arr(i).rename_rec.temp_name
        );
        IF NOT g_temp_name_arr.EXISTS('INDEX:"'||in_table_rec.index_arr(i).rename_rec.temp_name||'"') AND 
           in_table_rec.index_arr(i).constraint_name IS NOT NULL AND
           in_table_rec.constraint_indx_arr.EXISTS(in_table_rec.index_arr(i).constraint_name) 
        THEN
          l_indx := in_table_rec.constraint_indx_arr(in_table_rec.index_arr(i).constraint_name);
          IF g_temp_name_arr.EXISTS('CONSTRAINT:"'||in_table_rec.constraint_arr(l_indx).rename_rec.temp_name||'"') THEN
            g_temp_name_arr('INDEX:"'||in_table_rec.constraint_arr(l_indx).rename_rec.temp_name||'"') := in_table_rec.index_arr(i).index_name;
          END IF; 
        END IF;
      END IF;  
    END LOOP;
    -- find all named lob segments
    l_indx := in_table_rec.lob_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      -- for lob columns
      IF in_table_rec.lob_arr(l_indx).rename_rec.generated = 'N' THEN
        find_lob_segment_name(
          in_column_name  => in_table_rec.lob_arr(l_indx).column_name, 
          in_segment_name => in_table_rec.lob_arr(l_indx).lob_name, 
          in_temp_name    => in_table_rec.lob_arr(l_indx).rename_rec.temp_name
        );
      END IF;  
      l_indx := in_table_rec.lob_arr.NEXT(l_indx);
    END LOOP;
  END find_all_substitutions;
  
  -- find all substitution entries (CREATE INDEX)
  PROCEDURE find_all_substitutions(
    in_index_rec IN cort_exec_pkg.gt_index_rec,
    in_table_rec IN cort_exec_pkg.gt_table_rec
  )
  AS
    l_search_pos   PLS_INTEGER;
    l_pattern      VARCHAR2(100);
  BEGIN
    g_replace_arr.DELETE;
    g_temp_name_arr.DELETE;

    -- find index name 
    l_pattern := '\s*' || get_owner_name_regexp(in_index_rec.index_name, in_index_rec.owner, '\W'); 
    l_search_pos := 1;
    l_search_pos := find_substitable_name(
                      in_object_type  => 'INDEX',
                      in_object_name  => in_index_rec.index_name,
                      in_new_name     => in_index_rec.rename_rec.temp_name,
                      in_pattern      => l_pattern,
                      in_search_pos   => l_search_pos,
                      in_subexpr      => 2
                    );

    -- find table name
    l_pattern := '\WON\s+' || get_owner_name_regexp(in_table_rec.table_name, in_table_rec.owner, '\W');
    l_search_pos := g_sql_positions.definition_start_pos;
    l_search_pos := find_substitable_name(
                      in_object_type  => 'TABLE',
                      in_object_name  => in_table_rec.table_name,
                      in_new_name     => in_table_rec.rename_rec.temp_name,
                      in_pattern      => l_pattern,
                      in_search_pos   => l_search_pos,
                      in_subexpr      => 2
                    );
  END find_all_substitutions;  
  
  PROCEDURE find_all_substitutions(
    in_sequence_rec IN cort_exec_pkg.gt_sequence_rec
  )
  AS
    l_pattern     VARCHAR2(1000);
    l_search_pos  PLS_INTEGER;
  BEGIN
    g_replace_arr.DELETE;
    g_temp_name_arr.DELETE;

    -- find all instances for table name
    l_search_pos := 1;    
    l_pattern := '\WSEQUENCE\s+'||get_owner_name_regexp(in_sequence_rec.sequence_name, in_sequence_rec.owner, '\W');

    debug('l_pattern = '||l_pattern);

    l_search_pos := find_substitable_name(
                      in_object_type  => 'SEQUENCE',
                      in_object_name  => in_sequence_rec.sequence_name,
                      in_new_name     => in_sequence_rec.rename_rec.temp_name,
                      in_pattern      => l_pattern,
                      in_search_pos   => l_search_pos,
                      in_subexpr      => 2
                    );
  END find_all_substitutions;

  /* Replaces all comments and string literals with blank symbols */
  PROCEDURE parse_sql(
    in_sql          IN CLOB,
    out_sql         OUT NOCOPY CLOB,
    out_text_arr    OUT NOCOPY gt_cort_text_arr,
    in_quoted_names IN BOOLEAN,
    in_str_literals IN BOOLEAN,
    in_comments     IN BOOLEAN
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_key                       VARCHAR2(32767);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_expr_start_pos            PLS_INTEGER;
    l_expr_end_pos              PLS_INTEGER;
    l_expr_start_pattern        VARCHAR2(100);
    l_expr_end_pattern          VARCHAR2(100);
    l_expr_type                 VARCHAR2(30);
    l_expr                      CLOB;
    l_quoted_name               VARCHAR2(32767);
    l_expr_cnt                  PLS_INTEGER;
  BEGIN
    l_search_pos := 1;
    l_expr_cnt := 0;

    LOOP
      l_expr_start_pattern := q'{(/\*)|(--)|((N|n)?')|((N|n)?(Q|q)'\S)|"}';
      l_key_start_pos := REGEXP_INSTR(in_sql, l_expr_start_pattern, l_search_pos, 1, 0);
      l_key_end_pos   := REGEXP_INSTR(in_sql, l_expr_start_pattern, l_search_pos, 1, 1);
      EXIT WHEN l_key_start_pos = 0 OR l_key_start_pos IS NULL
             OR l_key_end_pos = 0 OR l_key_end_pos IS NULL
             OR l_expr_cnt > 10000;
      l_key := SUBSTR(in_sql, l_key_start_pos, l_key_end_pos-l_key_start_pos);
      CASE
      WHEN l_key = '/*' THEN
        l_expr_end_pattern := '\*/';
        l_expr_start_pos := REGEXP_INSTR(in_sql, l_expr_end_pattern, l_key_end_pos, 1, 0);
        l_expr_end_pos := l_expr_start_pos + 2;
        l_expr_type := 'COMMENT';
      WHEN l_key = '--' THEN
        l_expr_end_pattern := '$';
        l_expr_start_pos := REGEXP_INSTR(in_sql, l_expr_end_pattern, l_key_end_pos, 1, 0, 'm');
        l_expr_end_pos := l_expr_start_pos;
        l_expr_type := 'LINE COMMENT';
      WHEN l_key = '"' THEN
        l_expr_end_pattern := '"';
        l_expr_start_pos := REGEXP_INSTR(in_sql, l_expr_end_pattern, l_key_end_pos, 1, 0);
        l_expr_end_pos := l_expr_start_pos + 1;
        l_expr_type := 'QUOTED NAME';
      WHEN l_key = 'N'''
        OR l_key = 'n'''
        OR l_key = '''' THEN
        l_expr_end_pattern := '''';
        l_expr_start_pos := REGEXP_INSTR(in_sql, l_expr_end_pattern, l_key_end_pos, 1, 0);
        l_expr_end_pos := l_expr_start_pos + 1;
        l_expr_type := 'LITERAL';
      WHEN REGEXP_LIKE(l_key, q'{(N|n)?(Q|q)'\S}') THEN
        CASE SUBSTR(l_key, -1)
          WHEN '{' THEN l_expr_end_pattern := '}''';
          WHEN '[' THEN l_expr_end_pattern := ']''';
          WHEN '(' THEN l_expr_end_pattern := ')''';
          WHEN '<' THEN l_expr_end_pattern := '>''';
          ELSE l_expr_end_pattern := SUBSTR(l_key, -1)||'''';
        END CASE;
        l_expr_start_pos := INSTR(in_sql, l_expr_end_pattern, l_key_end_pos);
        l_expr_end_pos := l_expr_start_pos + 2;
        l_expr_type := 'LITERAL';
      ELSE
        l_key_start_pos := NULL;
        l_expr_end_pos := NULL; 
        l_key_start_pos := NULL;
      END CASE;
      IF l_expr_start_pos = 0 THEN
        cort_exec_pkg.raise_error( 'Invalid SQL');
      END IF;
      l_expr := SUBSTR(in_sql, l_key_start_pos, l_expr_end_pos - l_key_start_pos);
      add_text(out_text_arr, l_expr_type, l_expr, l_key_start_pos, l_expr_end_pos);
      CASE  
      WHEN l_expr_type IN ('COMMENT','LINE COMMENT') THEN
        IF NOT in_comments THEN
          l_expr := RPAD(' ', LENGTH(l_expr), ' ');
        END IF;     
      WHEN l_expr_type = 'QUOTED NAME' THEN
        -- if simple name quoted then simplify it
        l_quoted_name := SUBSTR(l_expr, 2, LENGTH(l_expr)-2);
        IF is_simple_name(l_quoted_name) THEN
          l_expr := ' '||UPPER(l_quoted_name)||' ';
        ELSE
          IF NOT in_quoted_names THEN
            l_expr := RPAD(' ', LENGTH(l_expr), ' ');
          END IF;  
        END IF;
      WHEN l_expr_type = 'LITERAL' THEN
        IF NOT in_str_literals THEN
          l_expr := RPAD(' ', LENGTH(l_expr), ' ');
        END IF;     
      ELSE
        l_expr := NULL;
      END CASE;

      out_sql := out_sql || UPPER(SUBSTR(in_sql, l_search_pos, l_key_start_pos-l_search_pos)) || l_expr;
      l_search_pos := l_expr_end_pos;
      l_expr_cnt := l_expr_cnt + 1;
    END LOOP;
    out_sql := out_sql || UPPER(SUBSTR(in_sql, l_search_pos));
  END parse_sql;
  
  /* Parse sql and save it into global variable */
  PROCEDURE normalize_sql(
    in_sql IN  CLOB
  )
  AS
  BEGIN
    g_normalized_sql := NULL;
    g_cort_text_arr.DELETE;
    
    parse_sql(
      in_sql          => in_sql,
      out_sql         => g_normalized_sql,
      out_text_arr    => g_cort_text_arr,  
      in_quoted_names => TRUE,
      in_str_literals => FALSE,
      in_comments     => FALSE
    );
    --debug('g_normalized_sql = '||g_normalized_sql);
  END normalize_sql;

  FUNCTION get_normalized_sql(
    in_quoted_names IN BOOLEAN DEFAULT TRUE,
    in_str_literals IN BOOLEAN DEFAULT TRUE,
    in_comments     IN BOOLEAN DEFAULT TRUE
  )
  RETURN CLOB
  AS
    l_sql     CLOB;
    l_len     PLS_INTEGER;
    l_replace VARCHAR2(32767);
    l_indx    PLS_INTEGER;
  BEGIN
    l_sql := g_normalized_sql;
    l_indx := g_cort_text_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      IF (g_cort_text_arr(l_indx).text_type = 'QUOTED NAME' AND NOT in_quoted_names) OR
         (g_cort_text_arr(l_indx).text_type = 'LITERAL' AND in_str_literals) OR 
         (g_cort_text_arr(l_indx).text_type IN ('COMMENT', 'LINE COMMENT') AND in_comments) 
      THEN
        l_len := LENGTH(g_cort_text_arr(l_indx).text);
        l_replace := g_cort_text_arr(l_indx).text;
        dbms_lob.write(l_sql, l_len, g_cort_text_arr(l_indx).start_position, l_replace);
      END IF;
      l_indx := g_cort_text_arr.NEXT(l_indx);
    END LOOP;
    RETURN l_sql;
  END get_normalized_sql;
  
  -- Return list of all registered cort-hints
  FUNCTION get_cort_hints RETURN gt_hint_arr
  $IF dbms_db_version.version >= 11 AND dbms_db_version.release >=1 $THEN
  RESULT_CACHE
  RELIES_ON (cort_hints)
  $END
  AS
    TYPE t_temp_arr IS TABLE OF cort_hints%ROWTYPE INDEX BY PLS_INTEGER;
    l_temp_arr   t_temp_arr;
    l_hint_arr   gt_hint_arr;
  BEGIN
    SELECT * 
      BULK COLLECT 
      INTO l_temp_arr 
      FROM cort_hints;
    FOR i IN 1..l_temp_arr.COUNT LOOP
      l_hint_arr(UPPER(l_temp_arr(i).hint)) := l_temp_arr(i);
    END LOOP;   
    RETURN l_hint_arr;
  END get_cort_hints;

  -- parse cort-hint value and apply it to param records
  PROCEDURE apply_cort_hint(
    in_hint_string  IN VARCHAR2,
    io_params_rec   IN OUT NOCOPY cort_params_pkg.gt_params_rec
  )
  AS
    l_regexp_str    VARCHAR2(32767);
    l_key_start_pos PLS_INTEGER;
    l_key_end_pos   PLS_INTEGER;
    l_key           VARCHAR2(32767);
    l_value         VARCHAR2(32767);
    l_hint_arr      gt_hint_arr;
  BEGIN
    l_hint_arr := get_cort_hints;
    l_key := l_hint_arr.FIRST;
    debug('parsing hint string = '||in_hint_string);
    WHILE l_key IS NOT NULL LOOP 
      l_regexp_str := '\W('||l_key||')\W';
      l_key_start_pos := REGEXP_INSTR(in_hint_string, l_regexp_str, 1, 1, 0, 'im', 1);
      IF l_key_start_pos > 0 THEN
        l_key_end_pos := REGEXP_INSTR(in_hint_string, l_regexp_str, 1, 1, 1, 'im', 1);
        IF l_key = UPPER(SUBSTR(in_hint_string, l_key_start_pos, l_key_end_pos-l_key_start_pos)) THEN
          debug('Found hint '||l_key);
          IF l_hint_arr(l_key).expression_flag = 'Y' THEN
            l_value := REGEXP_SUBSTR(in_hint_string, l_hint_arr(l_key).param_value, l_key_end_pos, 1, NULL, 1);
            debug(l_hint_arr(l_key).param_name||'='||l_value);
            cort_params_pkg.set_param_value(io_params_rec, l_hint_arr(l_key).param_name, l_value);
          ELSE
            debug(l_hint_arr(l_key).param_name||'='||l_hint_arr(l_key).param_value);
            cort_params_pkg.set_param_value(io_params_rec, l_hint_arr(l_key).param_name, l_hint_arr(l_key).param_value);
          END IF;
        ELSE  
          debug('Hint '||l_key||' not recognized');
        END IF;
      END IF;  

      l_key := l_hint_arr.NEXT(l_key);
    END LOOP;
  END apply_cort_hint;

  -- parse cort hints
  PROCEDURE parse_cort_hints(
    io_params_rec IN OUT NOCOPY cort_params_pkg.gt_params_rec
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_key                       VARCHAR2(30);
    l_text                      VARCHAR2(32767);
    l_value                     VARCHAR2(32767);
    l_indx                      PLS_INTEGER;
  BEGIN
    l_indx := g_cort_text_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      IF g_cort_text_arr(l_indx).text_type IN ('LINE COMMENT', 'COMMENT') THEN
        CASE g_cort_text_arr(l_indx).text_type 
        WHEN 'LINE COMMENT' THEN
          l_text := SUBSTR(g_cort_text_arr(l_indx).text, 3);
        WHEN 'COMMENT' THEN
          l_text := SUBSTR(g_cort_text_arr(l_indx).text, 3, LENGTH(g_cort_text_arr(l_indx).text)-4);
        END CASE;
        IF g_cort_text_arr(l_indx).start_position BETWEEN g_sql_positions.cort_param_start_pos 
                                                 AND g_sql_positions.cort_param_end_pos 
        THEN                                            
          l_regexp := get_regexp_const(cort_exec_pkg.gc_cort_text_prefix);
          l_key_start_pos := REGEXP_INSTR(l_text, l_regexp, 1, 1, 0);
          l_key_end_pos := REGEXP_INSTR(l_text, l_regexp, 1, 1, 1);
          IF l_key_start_pos = 1 THEN
            l_value := ' '||SUBSTR(l_text, l_key_end_pos+1)||' ';
            apply_cort_hint(
              in_hint_string  => l_value,
              io_params_rec   => io_params_rec
            );
          END IF;            
        END IF;
      END IF;
      l_indx := g_cort_text_arr.NEXT(l_indx);
    END LOOP;

  END parse_cort_hints;
  
  -- read and return SQL name following in_search_pos. If valid name is not found then return NULL
  FUNCTION read_next_name(
    io_search_pos IN OUT PLS_INTEGER
  )
  RETURN VARCHAR2
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_key                       VARCHAR2(30);
  BEGIN
    l_search_pos := io_search_pos;
    l_regexp := '\s*(\S)';
    l_key := REGEXP_SUBSTR(g_normalized_sql, l_regexp, l_search_pos, 1, NULL, 1);
    CASE 
    WHEN l_key = '"' THEN
      l_regexp := '"';
      l_key_start_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 1);
      l_search_pos := l_key_start_pos;
      l_key_end_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 0);
      l_key := SUBSTR(g_normalized_sql, l_key_start_pos, l_key_end_pos - l_key_start_pos);
      io_search_pos := l_key_end_pos + 1;
    WHEN l_key >= 'A' AND l_key <= 'Z' THEN
      l_regexp := '[A-Z][A-Z0-9_$#]{0,29}';
      l_key_start_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 0);
      l_key_end_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 1);
      l_key := SUBSTR(g_normalized_sql, l_key_start_pos, l_key_end_pos - l_key_start_pos);
      io_search_pos := l_key_end_pos;
    ELSE
      l_key := NULL;  
    END CASE;
    RETURN l_key;
  END read_next_name;
  
  -- Finds cort_value params and assigns them to the nearest column
  PROCEDURE parse_column_cort_values(
    io_table_rec  IN OUT NOCOPY cort_exec_pkg.gt_table_rec
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_key                       VARCHAR2(30);
    l_release                   VARCHAR2(20);
    l_text                      VARCHAR2(32767);
    l_value                     VARCHAR2(32767);
    l_indx                      PLS_INTEGER;
    l_cort_index                PLS_INTEGER;
    l_last_column_index         PLS_INTEGER;
    l_column_index              PLS_INTEGER;
    
    FUNCTION get_column_at(
      in_position      IN  PLS_INTEGER, 
      out_column_index OUT PLS_INTEGER
    )
    RETURN BOOLEAN  
    AS
    BEGIN
      FOR i IN l_last_column_index..io_table_rec.column_arr.COUNT LOOP
        IF in_position BETWEEN io_table_rec.column_arr(i).sql_end_position 
                           AND io_table_rec.column_arr(i).sql_next_start_position
        THEN
          l_last_column_index := i;
          out_column_index := i; 
          RETURN TRUE;
        END IF;                    
      END LOOP;
      l_last_column_index := io_table_rec.column_arr.COUNT + 1;
      out_column_index := -1;
      RETURN FALSE;
    END get_column_at;
    
  BEGIN
    l_last_column_index := 1;
    l_indx := g_cort_text_arr.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      IF l_indx BETWEEN g_sql_positions.columns_start_pos AND g_sql_positions.columns_end_pos AND
         g_cort_text_arr(l_indx).text_type IN ('LINE COMMENT', 'COMMENT') 
      THEN
        CASE g_cort_text_arr(l_indx).text_type 
        WHEN 'LINE COMMENT' THEN
          l_text := SUBSTR(g_cort_text_arr(l_indx).text, 3);
        WHEN 'COMMENT' THEN
          l_text := SUBSTR(g_cort_text_arr(l_indx).text, 3, LENGTH(g_cort_text_arr(l_indx).text)-4);
        END CASE; 
        l_regexp := get_regexp_const(cort_exec_pkg.gc_cort_text_prefix)||'('||
                    cort_exec_pkg.gc_release_regexp||')?('||
                    get_regexp_const(cort_exec_pkg.gc_force_value)||'|'||
                    get_regexp_const(cort_exec_pkg.gc_value)||')';
        l_key_start_pos := REGEXP_INSTR(l_text, l_regexp, 1, 1, 0);
        l_key_end_pos := REGEXP_INSTR(l_text, l_regexp, 1, 1, 1);
        IF l_key_start_pos = 1 THEN
          l_release := REGEXP_SUBSTR(l_text, l_regexp, 1, 1, NULL, 1);
          l_key := REGEXP_SUBSTR(l_text, l_regexp, 1, 1, NULL, 2);
          l_value := SUBSTR(l_text, l_key_end_pos);
          IF TRIM(l_value) IS NOT NULL THEN
            CASE l_key
            WHEN cort_exec_pkg.gc_force_value THEN
              IF get_column_at(l_indx, l_column_index) THEN
                debug('Parsing: Column '||io_table_rec.column_arr(l_column_index).column_name||' has cort force value (release='||l_release||') = '||l_value);
                l_cort_index := io_table_rec.column_arr(l_column_index).cort_values.COUNT + 1;
                io_table_rec.column_arr(l_column_index).cort_values(l_cort_index).release := l_release; 
                io_table_rec.column_arr(l_column_index).cort_values(l_cort_index).expression := l_value;
                io_table_rec.column_arr(l_column_index).cort_values(l_cort_index).force_value := TRUE;
              ELSE
                debug('Parsing: Column at position '||l_indx||' not found');
              END IF;
            WHEN cort_exec_pkg.gc_value THEN
              IF get_column_at(l_indx, l_column_index) THEN
                debug('Parsing: Column '||io_table_rec.column_arr(l_column_index).column_name||' has cort value (release='||l_release||')= '||l_value);
                l_cort_index := io_table_rec.column_arr(l_column_index).cort_values.COUNT + 1;
                io_table_rec.column_arr(l_column_index).cort_values(l_cort_index).release := l_release; 
                io_table_rec.column_arr(l_column_index).cort_values(l_cort_index).expression := l_value;
                io_table_rec.column_arr(l_column_index).cort_values(l_cort_index).force_value := FALSE;
              ELSE
                debug('Parsing: Column at position '||l_indx||' not found');
              END IF;
            ELSE 
              debug('Unknown cort key = '||l_key);
            END CASE;
          END IF;
        END IF;            
      END IF;
      EXIT WHEN l_indx > g_sql_positions.columns_end_pos;
      l_indx := g_cort_text_arr.NEXT(l_indx);
    END LOOP;

  END parse_column_cort_values;
  
  -- Parse column definition start/end positions
  PROCEDURE parse_table_sql(
    in_table_name IN VARCHAR2,
    in_owner_name IN VARCHAR2
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_parse_only_sql            CLOB;
    l_schema_name               VARCHAR2(4000);
  BEGIN
    l_search_pos := g_sql_positions.definition_start_pos;
    -- find table name 
    l_regexp := '\s*' || get_owner_name_regexp(in_table_name, in_owner_name, '\W'); 
    debug('Table name regexp = '||l_regexp);
    l_key_end_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 1, NULL, 2);
    IF l_key_end_pos > 0 THEN
      g_sql_positions.definition_start_pos := l_key_end_pos;
      l_schema_name := REGEXP_SUBSTR(g_normalized_sql, l_regexp, l_search_pos, 1, NULL, 1);
      l_schema_name := REGEXP_REPLACE(l_schema_name, '^\s+', NULL);
      l_schema_name := REGEXP_REPLACE(l_schema_name, '\s*\.\s*$', NULL);
      g_sql_positions.schema_name := l_schema_name; 
      g_sql_positions.columns_start_pos := l_key_end_pos;
      g_sql_positions.columns_end_pos := l_key_end_pos;
      debug('definition_start_pos = '||g_sql_positions.definition_start_pos);
      debug('schema_name = '||g_sql_positions.schema_name);
    ELSE
      debug('Table name not found');
      cort_exec_pkg.raise_error( 'Table name not found');
    END IF; 
    
    l_parse_only_sql := get_normalized_sql(
                          in_quoted_names => FALSE,
                          in_str_literals => FALSE,
                          in_comments     => FALSE
                       );
  
    l_search_pos := g_sql_positions.definition_start_pos;
    l_regexp := '\s*(\()'; -- find a open bracket
    l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos); 
    l_key_end_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
    debug('Relational columns - l_search_pos = '||l_search_pos);
    debug('Relational columns - l_key_start_pos = '||l_key_start_pos);
    debug('Relational columns - l_key_end_pos = '||l_key_end_pos);
    IF l_key_start_pos = l_search_pos AND l_key_end_pos > 0 THEN    
      g_sql_positions.columns_start_pos := l_key_end_pos;
      debug('Relational columns definition is found');
      -- find a close bracket
      g_sql_positions.columns_end_pos := get_closed_bracket(
                                           in_sql             => l_parse_only_sql,
                                           in_search_position => g_sql_positions.columns_start_pos
                                         );
    ELSE
      debug('Relational columns definition is not found');
      -- find object properties definition 
      l_search_pos := g_sql_positions.definition_start_pos;
      l_regexp := '\WOF\W'; -- check that this is object table
      l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos);
      IF l_key_start_pos = l_search_pos AND l_key_start_pos > 0 THEN 
        debug('Object table definition is found');
        l_search_pos := l_key_start_pos;
        l_regexp := '\(';
        g_sql_positions.columns_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1); -- find a open bracket
        IF g_sql_positions.columns_start_pos > 0 THEN
          debug('Object table columns definition is found');
          -- This contingently position for columns definition. It could be something else.
          -- find a close bracket
          g_sql_positions.columns_end_pos := get_closed_bracket(
                                               in_sql             => l_parse_only_sql,
                                               in_search_position => g_sql_positions.columns_start_pos
                                             );
        ELSE
          debug('Object table columns definition is not found');
        END IF;
      ELSE
        debug('Object table definition is not found');
      END IF;  
    END IF;
  END parse_table_sql;
  
  -- Parse index definition to find table name 
  PROCEDURE parse_index_sql(
    in_index_name IN VARCHAR2,
    in_owner_name IN VARCHAR2
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_parse_only_sql            CLOB;
    l_name                      VARCHAR2(1000);
  BEGIN
    l_search_pos := g_sql_positions.definition_start_pos;
    -- find index name 
    l_regexp := '\s*' || get_owner_name_regexp(in_index_name, in_owner_name, '\W'); 
    debug('Index name regexp = '||l_regexp);
    l_key_end_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 1, NULL, 2);
    IF l_key_end_pos > 0 THEN
      g_sql_positions.definition_start_pos := l_key_end_pos;
      g_sql_positions.schema_name := REGEXP_SUBSTR(g_normalized_sql, l_regexp, l_search_pos, 1, NULL, 1);
      g_sql_positions.schema_name := REGEXP_REPLACE(g_sql_positions.schema_name, '^\s+', NULL);
      g_sql_positions.schema_name := REGEXP_REPLACE(g_sql_positions.schema_name, '\s*\.\s*$', NULL);
      debug('definition_start_pos = '||g_sql_positions.definition_start_pos);
      debug('schema_name = '||g_sql_positions.schema_name);
    ELSE
      debug('Index name not found');
      cort_exec_pkg.raise_error( 'Index name not found');
    END IF; 
    
    l_search_pos := g_sql_positions.definition_start_pos;
    l_regexp := '\s*(ON\s+CLUSTER)\W';
    l_key_start_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos);
    debug('l_key_start_pos '||l_key_start_pos);
    IF l_key_start_pos > 0 AND l_key_start_pos = l_search_pos THEN
      debug('Found ON CLUSTER');
      g_sql_positions.is_cluster := TRUE;
      l_key_end_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
      g_sql_positions.table_definition_start_pos := l_key_end_pos; 
      l_search_pos := l_key_end_pos;
    ELSE  
      g_sql_positions.is_cluster := FALSE;
      l_regexp := '\s*(ON)\W';
      l_key_start_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos);
      debug('l_key_start_pos '||l_key_start_pos);
      IF l_key_start_pos > 0 AND l_key_start_pos = l_search_pos THEN
        debug('Found ON');
        l_key_end_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
        g_sql_positions.table_definition_start_pos := l_key_end_pos; 
        l_search_pos := l_key_end_pos;
      ELSE
        debug('Index ON keyword not found');
        cort_exec_pkg.raise_error( 'Index ON keyword not found');
      END IF;  
    END IF;
    
    l_name := read_next_name(l_search_pos);
    debug('l_name 1 = '||l_name);
    IF l_name IS NOT NULL THEN
      debug('l_search_pos = '||l_search_pos);
      l_regexp := '\s*\.'; -- find a dot
      l_key_start_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos);
      debug('l_key_start_pos = '||l_key_start_pos);
      IF l_key_start_pos = l_search_pos AND l_key_end_pos > 0 THEN
        g_sql_positions.table_owner := l_name;
        l_search_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 1);
        debug('l_search_pos = '||l_search_pos);
        l_name := read_next_name(l_search_pos);
        debug('l_name 2 = '||l_name);
        g_sql_positions.table_name := l_name;
      ELSE
        g_sql_positions.table_name := l_name;
      END IF;
    END IF;
    
    
    IF NOT g_sql_positions.is_cluster THEN
      l_parse_only_sql := get_normalized_sql(
                            in_quoted_names => FALSE,
                            in_str_literals => FALSE,
                            in_comments     => FALSE
                         );
      l_regexp := '\s*(\()'; -- find a open bracket
      l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos); 
      l_key_end_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
      debug('Relational columns - l_search_pos = '||l_search_pos);
      debug('Relational columns - l_key_start_pos = '||l_key_start_pos);
      debug('Relational columns - l_key_end_pos = '||l_key_end_pos);
      IF l_key_start_pos = l_search_pos AND l_key_end_pos > 0 THEN    
        g_sql_positions.columns_start_pos := l_key_end_pos;
        debug('Relational columns definition is found');
        -- find a close bracket
        g_sql_positions.columns_end_pos := get_closed_bracket(
                                             in_sql             => l_parse_only_sql,
                                             in_search_position => g_sql_positions.columns_start_pos
                                           );
      END IF;     
    END IF;                                
  END parse_index_sql;
     
  -- Parse index definition to find table name 
  PROCEDURE parse_sequence_sql(
    in_sequence_name IN VARCHAR2,
    in_owner_name    IN VARCHAR2
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_end_pos               PLS_INTEGER;
    l_parse_only_sql            CLOB;
  BEGIN
    l_search_pos := g_sql_positions.definition_start_pos;
    -- find index name 
    l_regexp := '\s*' || get_owner_name_regexp(in_sequence_name, in_owner_name, '(\W|$)'); 
    debug('Sequence name regexp = '||l_regexp);
    l_key_end_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 1, NULL, 2);
    IF l_key_end_pos > 0 THEN
      g_sql_positions.definition_start_pos := l_key_end_pos;
      g_sql_positions.schema_name := REGEXP_SUBSTR(g_normalized_sql, l_regexp, l_search_pos, 1, NULL, 1);
      g_sql_positions.schema_name := REGEXP_REPLACE(g_sql_positions.schema_name, '^\s+', NULL);
      g_sql_positions.schema_name := REGEXP_REPLACE(g_sql_positions.schema_name, '\s*\.\s*$', NULL);
      debug('definition_start_pos = '||g_sql_positions.definition_start_pos);
      debug('schema_name = '||g_sql_positions.schema_name);
    ELSE
      debug('Sequence name not found');
      cort_exec_pkg.raise_error( 'Sequence name not found');
    END IF; 
  END parse_sequence_sql;

  -- parses SQL command header and find location for cort-hints
  PROCEDURE parse_object_sql(
    in_operation     IN VARCHAR2,-- CREATE/DROP
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    io_params_rec    IN OUT NOCOPY cort_params_pkg.gt_params_rec
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_parse_only_sql            CLOB;
  BEGIN
    l_search_pos := 1;
    CASE in_object_type 
    WHEN 'TABLE' THEN
      CASE in_operation
      WHEN 'CREATE' THEN 
        l_regexp := '(\s*)(CREATE)\s+((GLOBAL\s+TEMPORARY\s+)?TABLE)\W';
      WHEN 'DROP' THEN
        l_regexp := '(\s*)(DROP)\s+(TABLE)\W';  
      END CASE;
    WHEN 'INDEX' THEN
      CASE in_operation
      WHEN 'CREATE' THEN 
        l_regexp := '(\s*)(CREATE)\s+((UNIQUE\s+|BITMAP\s+)?INDEX)\W';
      END CASE;
    WHEN 'SEQUENCE' THEN
      CASE in_operation
      WHEN 'CREATE' THEN 
        l_regexp := '(\s*)(CREATE)\s+(SEQUENCE)\W';
      END CASE;
    ELSE  
      NULL;  
    END CASE;  
    l_key_start_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 0); -- find table definition
    l_key_end_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 1, NULL, 3); -- find end of table definition
    IF l_key_start_pos = 1 THEN
      debug('found '||l_regexp);
      g_sql_positions.cort_param_start_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 1, NULL, 2); -- end of keyword CREATE
      g_sql_positions.cort_param_end_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 0, NULL, 3); -- start of keyword following "CREATE"
      g_sql_positions.definition_start_pos := l_key_end_pos;

      parse_cort_hints(
        io_params_rec => io_params_rec
      );

      debug('g_normalized_sql = '||g_normalized_sql);
      debug('definition_start_pos = '||g_sql_positions.definition_start_pos);

      -- for create table
      IF in_operation = 'CREATE' AND in_object_type = 'TABLE' THEN 
        parse_table_sql(
          in_table_name => in_object_name,
          in_owner_name => in_object_owner
        );
      END IF;

      -- for create index
      IF in_operation = 'CREATE' AND in_object_type = 'INDEX' THEN 
        parse_index_sql(
          in_index_name => in_object_name,
          in_owner_name => in_object_owner
        );
      END IF;

      -- for create sequence
      IF in_operation = 'CREATE' AND in_object_type = 'SEQUENCE' THEN 
        parse_sequence_sql(
          in_sequence_name => in_object_name,
          in_owner_name    => in_object_owner
        );
      END IF;
    END IF;
  END parse_object_sql;
  
  -- Public declaration
   
  -- parses SQL
  PROCEDURE initial_parse_sql(
    in_sql           IN CLOB,
    in_operation     IN VARCHAR2,-- CREATE/DROP
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,
    io_params_rec    IN OUT NOCOPY cort_params_pkg.gt_params_rec
  )
  AS
  BEGIN
    g_params := io_params_rec;
    
    g_sql_positions.cort_param_start_pos := 0;
    g_sql_positions.cort_param_end_pos := 0;
    g_sql_positions.definition_start_pos := 0; 
    g_sql_positions.columns_start_pos := 0;
    g_sql_positions.columns_end_pos := 0;
    g_sql_positions.partitions_start_pos := 0;
    g_sql_positions.partitions_end_pos := 0;
    g_sql_positions.as_select_start_pos := 0;
    g_sql_positions.subquery_start_pos := 0;
    
    normalize_sql(
      in_sql => in_sql
    );
   
    parse_object_sql(
      in_operation     => in_operation,
      in_object_type   => in_object_type, 
      in_object_name   => in_object_name, 
      in_object_owner  => in_object_owner,
      io_params_rec    => io_params_rec 
    );
  
    g_params := io_params_rec;
  END initial_parse_sql;
  
  PROCEDURE parse_as_select(
    out_as_select OUT BOOLEAN
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_parse_only_sql            CLOB;
  BEGIN
    out_as_select := FALSE; 
    l_parse_only_sql := get_normalized_sql(
                          in_quoted_names => FALSE,
                          in_str_literals => FALSE,
                          in_comments     => FALSE
                       );

    IF g_sql_positions.columns_end_pos > 0 THEN
      l_search_pos := g_sql_positions.columns_end_pos;
    ELSE  
      l_search_pos := g_sql_positions.definition_start_pos;
    END IF;

    debug('l_search_pos for AS SELECT clause lookup = '||l_search_pos);
    IF l_search_pos > 0 THEN
      -- find AS <subquery> clause 
      l_regexp := '\W(AS)(\s|\()+\s*(SELECT|WITH)\W'; -- AS SELECT|AS WITH
      g_sql_positions.as_select_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0, NULL, 1);
      IF g_sql_positions.as_select_start_pos > 0 THEN
        debug('AS SELECT clause found');
        g_sql_positions.subquery_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
        debug('subquery_start_pos = '||g_sql_positions.subquery_start_pos);
        out_as_select := TRUE;
      ELSE
        debug('AS SELECT clause not found');
      END IF;
    END IF;      
  END parse_as_select;

  -- set subquery return 0 rows
  PROCEDURE modify_as_select(
    io_sql IN OUT NOCOPY CLOB
  )
  AS
    l_subquery CLOB;
  BEGIN
    IF g_sql_positions.subquery_start_pos > 0 THEN
      io_sql := 
      SUBSTR(io_sql, 1, g_sql_positions.subquery_start_pos-1)||CHR(10)||
      'SELECT *'||CHR(10)|| 
      '  FROM ('||CHR(10)||SUBSTR(io_sql, g_sql_positions.subquery_start_pos)||CHR(10)||
      '       )'||CHR(10)||
      ' WHERE 1=0';
--      debug('modify subquery for test : '||io_sql);
     END IF;
  END modify_as_select;
  
  -- return TRUE if as_select subquery contains table name
  FUNCTION as_select_from(in_table_name IN VARCHAR)
  RETURN BOOLEAN
  AS
    l_search VARCHAR2(30);
  BEGIN
    IF is_simple_name(in_table_name) THEN
      l_search := '\W'||in_table_name||'\W';
    ELSE
      l_search := '"'||get_regexp_const(in_table_name)||'"';
    END IF;
    RETURN REGEXP_INSTR(g_normalized_sql, l_search, g_sql_positions.subquery_start_pos) > 0; 
  END as_select_from;
  
  -- determines partitions position
  PROCEDURE parse_partitioning(
    out_partitioning_type    OUT VARCHAR2,  
    out_subpartitioning_type OUT VARCHAR2 
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_key                       VARCHAR2(32767);
    l_regexp                    VARCHAR2(1000);
    l_parse_only_sql            CLOB;
    
    PROCEDURE parse_subpartitioning
    AS
    BEGIN
      -- find subpartitioning  
      l_regexp := '\s*SUBPARTITION\s+BY\s+(LIST|RANGE|HASH)\s*\(';
      l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0);
      IF l_key_start_pos = l_search_pos THEN
        l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0, NULL, 1);
        l_key_end_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
        g_sql_positions.subpartition_type := SUBSTR(l_parse_only_sql, l_key_start_pos, l_key_end_pos-l_key_start_pos);
        l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1);
        l_search_pos := REGEXP_INSTR(l_parse_only_sql, '\)', l_search_pos, 1, 1);
      END IF;

      out_subpartitioning_type := g_sql_positions.subpartition_type; 

      debug('subpartitioning_type = '||g_sql_positions.subpartition_type);
    

      -- if subpartitioned
      IF g_sql_positions.subpartition_type IS NOT NULL THEN
        -- skip template
        l_regexp := '\s*(SUBPARTITION\s+TEMPLATE)\W';
        l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0);
        IF l_key_start_pos = l_search_pos THEN
          l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
          l_regexp := '\s*\(';
          l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0);
          IF l_key_start_pos = l_search_pos THEN
            l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1);
            l_search_pos := get_closed_bracket(l_parse_only_sql, l_search_pos) + 1;
          ELSIF g_sql_positions.subpartition_type = 'HASH' THEN
            l_regexp := '\s*(SUBPARTITIONS\s+[0-9]+)\W';
            IF REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0) = l_search_pos THEN
              l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
              -- skip tablespaces
              l_regexp := '\s*STORE\s+IN\s*\(';
              l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0);
              IF l_key_start_pos = l_search_pos THEN
                l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1); 
                l_search_pos := get_closed_bracket(l_parse_only_sql, l_key_start_pos);
              END IF;
            END IF;
          END IF;
        END IF;
      END IF;  

    END parse_subpartitioning;
    
  BEGIN
    l_parse_only_sql := get_normalized_sql(
                           in_quoted_names => FALSE,
                           in_str_literals => FALSE,
                           in_comments     => FALSE
                        );

    l_search_pos := g_sql_positions.columns_end_pos+1;
    l_regexp := '\W(PARTITION\s+BY\s+(LIST|RANGE|HASH|REFERENCE|SYSTEM))\W';
    l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos);
    IF l_key_start_pos > 0 THEN
      l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0, NULL, 2);
      l_key_end_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 2);
      g_sql_positions.partition_type := SUBSTR(l_parse_only_sql, l_key_start_pos, l_key_end_pos-l_key_start_pos);
      l_search_pos := l_key_end_pos; 
    END IF;

    out_partitioning_type := g_sql_positions.partition_type;  

    debug('partitioning_type = '||g_sql_positions.partition_type);
    
    
    IF g_sql_positions.partition_type IS NOT NULL THEN
      -- skip column/reference definition 
      IF g_sql_positions.partition_type <> 'SYSTEM' THEN
        l_search_pos := REGEXP_INSTR(l_parse_only_sql, '\(', l_search_pos, 1, 1);
        l_search_pos := REGEXP_INSTR(l_parse_only_sql, '\)', l_search_pos, 1, 1);
      END IF;
      
      CASE g_sql_positions.partition_type
      WHEN 'RANGE' THEN
        -- skip interval definition
        l_regexp := '\s*INTERVAL\s*\(';
        l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0);
        IF l_key_start_pos = l_search_pos THEN
          l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1); 
          l_search_pos := get_closed_bracket(l_parse_only_sql, l_key_start_pos);
          -- skip tablespaces
          l_regexp := '\s*STORE\s+IN\s*\(';
          l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0);
          IF l_key_start_pos = l_search_pos THEN
            l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1); 
            l_search_pos := get_closed_bracket(l_parse_only_sql, l_key_start_pos);
          END IF;
        END IF;
        -- find subpartitioning  
        parse_subpartitioning;
      WHEN 'LIST' THEN
        -- find subpartitioning  
        parse_subpartitioning;
      WHEN 'HASH' THEN
        -- find subpartitioning  
        parse_subpartitioning;
        l_regexp := '\s*(PARTITIONS\s+[0-9]+)\W';
        l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0); 
        IF l_key_start_pos = l_search_pos THEN
          g_sql_positions.partitions_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0, NULL, 1);
          l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
          -- skip tablespaces
          l_regexp := '\s*STORE\s+IN\s*\(';
          l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0);
          IF l_key_start_pos = l_search_pos THEN
            l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1); 
            l_search_pos := get_closed_bracket(l_parse_only_sql, l_key_start_pos);
          END IF;
          -- skip overflow tablespace
          l_regexp := '\s*OVERFLOW\s+STORE\s+IN\s*\(';
          l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0);
          IF l_key_start_pos = l_search_pos THEN
            l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1); 
            l_search_pos := get_closed_bracket(l_parse_only_sql, l_key_start_pos);
          END IF;
          g_sql_positions.partitions_end_pos := l_search_pos; 
          debug('partitioning_start_pos = '||g_sql_positions.partitions_start_pos);
          debug('partitioning_end_pos = '||g_sql_positions.partitions_end_pos);
          RETURN;
        END IF;  
      WHEN 'SYSTEM' THEN
        -- skip store in
        l_regexp := '\s*(PARTITIONS\s+[0-9]+)\W';
        l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0); 
        IF l_key_start_pos = l_search_pos THEN
          g_sql_positions.partitions_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0, NULL, 1); 
          g_sql_positions.partitions_end_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
          debug('partitioning_start_pos = '||g_sql_positions.partitions_start_pos);
          debug('partitioning_end_pos = '||g_sql_positions.partitions_end_pos);
          RETURN;
        END IF;  
      ELSE NULL;
      END CASE;
      
      l_regexp := '\s*\(';
      l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 0); 
      IF l_key_start_pos = l_search_pos THEN
        -- include brackets into partitions definition
        g_sql_positions.partitions_start_pos := l_key_start_pos; 
        l_search_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1);
        g_sql_positions.partitions_end_pos := get_closed_bracket(l_parse_only_sql, l_search_pos) + 1;
  
        debug('partitioning_start_pos = '||g_sql_positions.partitions_start_pos);
        debug('partitioning_end_pos = '||g_sql_positions.partitions_end_pos);
      END IF;
    END IF;
  END parse_partitioning;

  -- replaces partitions definition in original_sql
  PROCEDURE replace_partitions_sql(
    io_sql           IN OUT NOCOPY CLOB,
    in_partition_sql IN CLOB
  )
  AS
  BEGIN
    IF g_sql_positions.partitions_start_pos > 0 AND 
       g_sql_positions.partitions_end_pos > 0 
    THEN
      debug('replace partitions : '||in_partition_sql);
      io_sql := SUBSTR(io_sql, 1, g_sql_positions.partitions_start_pos-1)||
                in_partition_sql||
                SUBSTR(io_sql, g_sql_positions.partitions_end_pos);
    END IF;            
  END replace_partitions_sql;

  -- parses columns positions and cort-values
  PROCEDURE parse_columns(
    io_table_rec      IN OUT NOCOPY cort_exec_pkg.gt_table_rec
  )
  AS
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_parse_only_sql            CLOB;
    l_next_position             PLS_INTEGER;
  BEGIN
    l_parse_only_sql := get_normalized_sql(
                          in_quoted_names => FALSE,
                          in_str_literals => FALSE,
                          in_comments     => FALSE
                       );
  
    l_search_pos := g_sql_positions.definition_start_pos;
    
    
    IF io_table_rec.table_type IS NULL THEN     
      l_regexp := '\s*(\()'; -- find a open bracket
      l_key_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos); 
      l_key_end_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);
      debug('Relational columns - l_search_pos = '||l_search_pos);
      debug('Relational columns - l_key_start_pos = '||l_key_start_pos);
      debug('Relational columns - l_key_end_pos = '||l_key_end_pos);
      IF l_key_start_pos = l_search_pos AND l_key_end_pos > 0 THEN    
        g_sql_positions.columns_start_pos := l_key_end_pos;
        debug('Relational columns definition is found');
        -- find a close bracket
        g_sql_positions.columns_end_pos := get_closed_bracket(
                                             in_sql             => l_parse_only_sql,
                                             in_search_position => g_sql_positions.columns_start_pos
                                           );
      ELSE
        debug('Relational columns definition is not found');
      END IF;
    ELSE    
      -- find object properties definition 
      l_search_pos := g_sql_positions.definition_start_pos;
      l_regexp := '\WOF\s+'||get_owner_name_regexp(io_table_rec.table_type, io_table_rec.table_type_owner, '\W'); -- check that this is object table
      l_key_start_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos); 
      l_key_end_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 1, NULL, 1); -- find end of object table definition
      IF l_key_start_pos = l_search_pos AND l_key_end_pos > 0 THEN 
        debug('Object table definition is found');
        l_search_pos := l_key_end_pos;
        l_regexp := '\(';
        g_sql_positions.columns_start_pos := REGEXP_INSTR(l_parse_only_sql, l_regexp, l_search_pos, 1, 1); -- find a open bracket
        IF g_sql_positions.columns_start_pos > 0 THEN
          debug('Object table columns definition is found');
          -- This contingently position for columns definition. It could be something else.
          -- find a close bracket
          g_sql_positions.columns_end_pos := get_closed_bracket(
                                               in_sql             => l_parse_only_sql,
                                               in_search_position => g_sql_positions.columns_start_pos
                                             );
        ELSE
          debug('Object table columns definition is not found');
        END IF;
      ELSE
        debug('Object table definition is not found');
      END IF;  
    END IF;
    
    
    l_search_pos := g_sql_positions.columns_start_pos;
    IF g_sql_positions.columns_start_pos > 0 AND
       g_sql_positions.columns_end_pos > g_sql_positions.columns_start_pos
    THEN  
      FOR i IN 1..io_table_rec.column_arr.COUNT LOOP
        IF io_table_rec.column_arr(i).hidden_column = 'NO' THEN
          l_regexp := get_column_regexp(in_column_rec => io_table_rec.column_arr(i));
          debug('lookup column regexp = '||l_regexp);
          l_key_start_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 0, null, 1);
          l_key_end_pos := REGEXP_INSTR(g_normalized_sql, l_regexp, l_search_pos, 1, 1, null, 1);
          IF l_key_start_pos > 0 THEN
            io_table_rec.column_arr(i).sql_start_position := l_key_start_pos;
            io_table_rec.column_arr(i).sql_end_position := l_key_end_pos;
            l_search_pos := l_key_end_pos;
            debug('Parsing: Column '||io_table_rec.column_arr(i).column_name||' startpos = '||l_key_start_pos||' endpos = '||l_key_end_pos);        
          ELSE
            debug('Parsing: Column '||io_table_rec.column_arr(i).column_name||' not found. Regexp = '||l_regexp);        
          END IF;
        END IF;
      END LOOP;

      l_next_position := g_sql_positions.columns_end_pos;
      FOR i IN REVERSE 1..io_table_rec.column_arr.COUNT LOOP
        IF io_table_rec.column_arr(i).hidden_column = 'NO' THEN
          io_table_rec.column_arr(i).sql_next_start_position := l_next_position;
          debug('Parsing: Column '||io_table_rec.column_arr(i).column_name||' next startpos = '||l_next_position);        
          l_next_position := io_table_rec.column_arr(i).sql_start_position;
        END IF;
      END LOOP;
    END IF;  
    
    parse_column_cort_values(
      io_table_rec  => io_table_rec
    );
  END parse_columns;
  
  -- normalize check constraint search condition
  PROCEDURE normalize_expression(
    in_expr  IN  VARCHAR2,
    out_expr OUT VARCHAR2
  )
  AS
    l_expr       VARCHAR2(32767);
    l_text_arr   gt_cort_text_arr;
  BEGIN
    parse_sql(
      in_sql          => in_expr,
      out_sql         => l_expr,
      out_text_arr    => l_text_arr,  
      in_quoted_names => TRUE,
      in_str_literals => TRUE,
      in_comments     => TRUE
    );
    out_expr := l_expr; 
  END normalize_expression;

  PROCEDURE int_replace_names(
    io_sql IN OUT NOCOPY CLOB 
  )
  AS
    l_indx         PLS_INTEGER;
    l_replace_rec  gt_replace_rec;
  BEGIN
    -- loop all names start from the end
    l_indx := g_replace_arr.LAST;
    WHILE l_indx IS NOT NULL LOOP
      l_replace_rec := g_replace_arr(l_indx);
      debug('l_replace_rec.new_name = '||l_replace_rec.new_name);
      IF g_cort_text_arr.EXISTS(l_replace_rec.start_pos-1) AND 
         g_cort_text_arr(l_replace_rec.start_pos-1).text_type = 'QUOTED NAME'
      THEN
        l_replace_rec.start_pos := l_replace_rec.start_pos - 1;
        l_replace_rec.end_pos := l_replace_rec.end_pos + 1;
      END IF;          
      io_sql := SUBSTR(io_sql, 1, l_replace_rec.start_pos - 1)||'"'||l_replace_rec.new_name||'"'||SUBSTR(io_sql, l_replace_rec.end_pos);
      l_indx := g_replace_arr.PRIOR(l_indx);
    END LOOP;
    io_sql := SUBSTR(io_sql, 1, g_sql_positions.cort_param_start_pos - 1)||' '||SUBSTR(io_sql, g_sql_positions.cort_param_end_pos);
    debug('io_sql = '||io_sql);
  END int_replace_names;


  -- replaces table name and all names of existing depending objects (constraints, log groups, indexes, lob segments) 
  PROCEDURE replace_names(
    in_table_rec IN cort_exec_pkg.gt_table_rec,
    io_sql       IN OUT NOCOPY CLOB 
  )
  AS
  BEGIN
    -- get all names in g_normalized sql
    find_all_substitutions(
      in_table_rec => in_table_rec
    );

    -- when use normalized SQL then text in check constaints is also normalized
    -- need to normalize it when we read the from dictionary before comparison
    -- io_sql := get_normalized_sql;

    int_replace_names(io_sql => io_sql);
  END replace_names;

  -- replaces index and table names for CREATE INDEX statement 
  PROCEDURE replace_names(
    in_table_rec IN cort_exec_pkg.gt_table_rec,
    in_index_rec IN cort_exec_pkg.gt_index_rec,
    io_sql       IN OUT NOCOPY CLOB 
  )
  AS
  BEGIN
    -- get all names
    find_all_substitutions(
      in_index_rec => in_index_rec,
      in_table_rec => in_table_rec
    );

    io_sql := get_normalized_sql;

    int_replace_names(io_sql => io_sql);
  END replace_names;

  -- replaces sequence name 
  PROCEDURE replace_names(
    in_sequence_rec IN cort_exec_pkg.gt_sequence_rec,
    io_sql          IN OUT NOCOPY CLOB 
  )
  AS
  BEGIN
    -- get all names
    find_all_substitutions(
      in_sequence_rec => in_sequence_rec
    );

    io_sql := get_normalized_sql;

    int_replace_names(io_sql => io_sql);
  END replace_names;

  -- replaces names in expression  
  PROCEDURE update_expression(
    io_expression      IN OUT NOCOPY VARCHAR2,
    in_replace_names   IN arrays.gt_str_indx
  )
  AS
    l_expr       VARCHAR2(32767);
    l_text_arr   gt_cort_text_arr;
    l_text_rec   gt_cort_text_rec;
    l_col_name   VARCHAR2(100);
    l_indx       VARCHAR2(30);
    l_new_name   VARCHAR2(30);
    l_search_pos PLS_INTEGER;
    l_key_start  PLS_INTEGER;
    l_key_end    PLS_INTEGER;
    l_pattern    VARCHAR2(100);
    l_int_idx    PLS_INTEGER;
  BEGIN
    parse_sql(
      in_sql          => io_expression,
      out_sql         => l_expr,
      out_text_arr    => l_text_arr,  
      in_quoted_names => FALSE,
      in_str_literals => FALSE,
      in_comments     => FALSE
    );
    
    FOR i IN REVERSE 1..l_text_arr.COUNT LOOP
      IF l_text_arr(i).text_type = 'QUOTED NAME' THEN
        l_col_name := SUBSTR(l_text_arr(i).text, 2, LENGTH(l_text_arr(i).text)-2);
        IF in_replace_names.EXISTS(l_col_name) THEN
          l_new_name := '"'||in_replace_names(l_col_name)||'"';
          io_expression := SUBSTR(io_expression, 1, l_text_arr(i).start_position-1)||l_new_name||SUBSTR(io_expression, l_text_arr(i).end_position);
        END IF;   
      END IF;   
    END LOOP;

    parse_sql(
      in_sql          => io_expression,
      out_sql         => l_expr,
      out_text_arr    => l_text_arr,  
      in_quoted_names => FALSE,
      in_str_literals => FALSE,
      in_comments     => FALSE
    );

    l_text_arr.DELETE;
    
    l_indx := in_replace_names.FIRST;
    WHILE l_indx IS NOT NULL LOOP
      IF is_simple_name(l_indx) THEN
        l_search_pos := 1;    
        l_pattern := '(^|\W)('||get_regexp_const(l_indx)||')($|\W)';
        LOOP
          l_key_start := REGEXP_INSTR(l_expr, l_pattern, l_search_pos, 1, 0, NULL, 2);
          EXIT WHEN l_key_start = 0;
          l_key_end := REGEXP_INSTR(l_expr, l_pattern, l_search_pos, 1, 1, NULL, 2); 
          l_text_rec.text_type := 'SIMPLE NAME';
          l_text_rec.text := SUBSTR(l_expr, l_key_start, l_key_end - l_key_start);
          l_text_rec.start_position := l_key_start;  
          l_text_rec.end_position := l_key_end; 
          l_text_arr(l_text_rec.start_position) := l_text_rec;
          l_search_pos := l_key_end;
        END LOOP;
      END IF;
      l_indx := in_replace_names.NEXT(l_indx);
    END LOOP;
    
    l_int_idx := l_text_arr.LAST;
    WHILE l_int_idx IS NOT NULL LOOP
      IF l_text_arr(l_int_idx).text_type = 'SIMPLE NAME' THEN
        l_col_name := l_text_arr(l_int_idx).text;
        IF in_replace_names.EXISTS(l_col_name) THEN
          l_new_name := in_replace_names(l_col_name);
          IF NOT is_simple_name(l_new_name) THEN
            l_new_name := '"'||l_new_name||'"';
          END IF;
          io_expression := SUBSTR(io_expression, 1, l_text_arr(l_int_idx).start_position-1)||l_new_name||SUBSTR(io_expression, l_text_arr(l_int_idx).end_position);
        END IF;   
      END IF;   
      l_int_idx := l_text_arr.PRIOR(l_int_idx); 
    END LOOP;
    
    debug('io_expression := '||io_expression);
  END update_expression;

  -- return schema name specified in SQL 
  FUNCTION get_sql_schema_name
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN g_sql_positions.schema_name;
  END get_sql_schema_name;

  -- return index's table/cluster name and owner
  PROCEDURE get_index_main_object(
    out_object_type  OUT VARCHAR2,
    out_object_owner OUT VARCHAR2,
    out_object_name  OUT VARCHAR2
  )
  AS
  BEGIN
    IF g_sql_positions.is_cluster THEN
      out_object_type := 'CLUSTER';
    ELSE
      out_object_type := 'TABLE';
    END IF;
    out_object_owner := g_sql_positions.table_owner; 
    out_object_name := g_sql_positions.table_name;
  END get_index_main_object;

  -- return original name for renamed object. If it wasn't rename return current name 
  FUNCTION get_original_name(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_indx   VARCHAR2(50); 
  BEGIN
    l_indx := in_object_type||':"'||in_object_name||'"';
    IF g_temp_name_arr.EXISTS(l_indx) THEN
      RETURN g_temp_name_arr(l_indx);
    ELSE
      RETURN in_object_name;
    END IF;  
  END get_original_name;

  -- parses create statement and return object type, owner and name
  PROCEDURE parse_create_statement(
    in_sql           IN CLOB,
    out_object_type  OUT VARCHAR2,
    out_object_owner OUT VARCHAR2,
    out_object_name  OUT VARCHAR2
  )
  AS
    l_sql                       CLOB;
    l_cort_text_arr             gt_cort_text_arr;
    l_search_pos                PLS_INTEGER;
    l_regexp                    VARCHAR2(1000);
    l_key_start_pos             PLS_INTEGER;
    l_key_end_pos               PLS_INTEGER;
    l_key                       VARCHAR2(100);
    l_name                      VARCHAR2(100);
    
    FUNCTION read_name
    RETURN VARCHAR2
    AS
      l_ident                  VARCHAR2(100);
    BEGIN
      l_regexp := '\S';
      l_key_start_pos := REGEXP_INSTR(l_sql, l_regexp, l_search_pos);
      IF l_key_start_pos > 0 THEN
        l_key := SUBSTR(l_sql, l_key_start_pos, 1);
        IF l_key = '"' THEN
          l_regexp := '"';
          l_key_start_pos := l_key_start_pos + 1;
        ELSIF l_key BETWEEN 'A' AND 'Z' THEN
          l_regexp := '[^A-Z0-9_\#\$]';
        ELSE
          RETURN NULL;  
        END IF;
        l_search_pos := l_key_start_pos;
        l_key_end_pos := REGEXP_INSTR(l_sql, l_regexp, l_search_pos);
        l_ident := SUBSTR(l_sql, l_key_start_pos, l_key_end_pos-l_key_start_pos);
        IF l_key = '"' THEN
          l_key_end_pos := l_key_end_pos + 1; 
        END IF;   
      END IF;
      RETURN l_ident;
    END read_name;
     
  BEGIN
    parse_sql(
      in_sql          => in_sql,
      out_sql         => l_sql,
      out_text_arr    => l_cort_text_arr,  
      in_quoted_names => TRUE,
      in_str_literals => FALSE,
      in_comments     => FALSE
    );
    debug(l_sql);
    
    -- add trailing space to simplify parsing names.
    l_sql := l_sql ||' ';
    
    l_search_pos := 1;
    l_regexp := '\s*CREATE\s+(((GLOBAL\s+TEMPORARY\s+)?(TABLE))|((UNIQUE\s+|BITMAP\s+)?(INDEX))|(SEQUENCE))\W';
    l_key_start_pos := REGEXP_INSTR(l_sql, l_regexp, l_search_pos, 1, 0); -- find table definition
    IF l_key_start_pos = 1 THEN
      -- check is it table
      out_object_type := REGEXP_SUBSTR(l_sql, l_regexp, l_search_pos, 1, NULL, 4);
      IF out_object_type IS NULL THEN
        -- check is it index 
        out_object_type := REGEXP_SUBSTR(l_sql, l_regexp, l_search_pos, 1, NULL, 7);
      END IF;   
      IF out_object_type IS NULL THEN
        -- check is it sequence 
        out_object_type := REGEXP_SUBSTR(l_sql, l_regexp, l_search_pos, 1, NULL, 8);
      END IF;   
      l_search_pos := REGEXP_INSTR(l_sql, l_regexp, l_search_pos, 1, 1, NULL, 1);

      l_name := read_name;
      
      IF l_name IS NOT NULL THEN
        l_search_pos := l_key_end_pos;
        l_regexp := '\S';
        l_key_start_pos := REGEXP_INSTR(l_sql, l_regexp, l_search_pos);
        IF l_key_start_pos > 0 AND 
           SUBSTR(l_sql, l_key_start_pos, 1) = '.'
        THEN
          out_object_owner := l_name;
          l_search_pos := l_key_start_pos + 1;
          out_object_name := read_name;       
        ELSE
          out_object_name := l_name;      
        END IF; 
      END IF;
        
    END IF;
    
  END parse_create_statement;

END cort_parse_pkg;
/
