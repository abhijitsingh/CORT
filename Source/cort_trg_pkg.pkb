CREATE OR REPLACE PACKAGE BODY cort_trg_pkg
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
  Description: functionality called from create trigger     
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.02   | Rustam Kafarov    | Added support for new objects types and explain plan functionality extension
  ----------------------------------------------------------------------------------------------------------------------  
*/


  g_last_id             NUMBER := 0;
  
  
  -- parses main CORT hint
  FUNCTION is_replace_mode(
    in_sql         IN CLOB
  )
  RETURN BOOLEAN
  AS
    l_prfx           VARCHAR2(30);
    l_create_expr    VARCHAR2(30);
    l_regexp         VARCHAR2(1000);
  BEGIN
    l_prfx := '#';
    l_create_expr := '^\s*CREATE\s*';
    l_regexp := '('||l_create_expr||'\/\*'||l_prfx||'\s*OR\s+REPLACE\W)|'||
                '('||l_create_expr|| '--' ||l_prfx||'[ \t]*OR[ \t]*+REPLACE\W)';
    RETURN REGEXP_INSTR(in_sql, l_regexp, 1, 1, 0, 'imn') = 1;
  END is_replace_mode;

 -- Function returns currently executing ddl statement. It could be called only from DDL triggers
  FUNCTION ora_dict_ddl
  RETURN CLOB
  AS
    l_ddl_arr   dbms_standard.ora_name_list_t;
    l_ddl       CLOB;
    l_cnt       PLS_INTEGER;
  BEGIN
    l_cnt := ora_sql_txt(l_ddl_arr);
    IF l_ddl_arr IS NOT NULL THEN
      FOR i IN 1..l_cnt LOOP
        -- TRIM(CHR(0) is workaroung to remove trailing #0 symbol. This symbol breask down convertion into XML 
        l_ddl := l_ddl || TRIM(CHR(0) FROM l_ddl_arr(i));
      END LOOP;
    END IF;
    RETURN l_ddl;
  END ora_dict_ddl;
  
-- Sets value to CORT context
  PROCEDURE set_context(
    in_name  IN VARCHAR2,
    in_value IN VARCHAR2
  )
  AS
  BEGIN
    dbms_session.set_context(g_context_name, in_name, in_value);
  END set_context;

  -- Gets value of CORT context
  FUNCTION get_context(
    in_name  IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
  BEGIN
    RETURN SYS_CONTEXT(g_context_name, in_name);
  END get_context;

  FUNCTION get_status
  RETURN VARCHAR2
  AS
    l_status VARCHAR2(10);
  BEGIN
    IF get_context('DISABLED_FOR_SESSION') = 'TRUE' THEN
      l_status := 'DISABLED';
    ELSE
      IF cort_options_pkg.gc_single_schema THEN
        IF ora_dict_obj_owner = SYS_CONTEXT('USERENV','CURRENT_USER') THEN
          l_status := 'ENABLED';
        ELSE
          l_status := 'DISABLED';
        END IF; 
      ELSE 
        l_status := 'ENABLED';
      END IF;  
    END IF;  
    RETURN l_status;
  END get_status;

  -- Returns 'REPLACE' if there is #OR REPLACE hint in given DDL or if this parameter is turned on for session.
  -- Otherwise returns 'CREATE'. It could be called only from DDL triggers
  FUNCTION get_execution_mode(
    in_object_type IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_result VARCHAR2(30);
  BEGIN
    CASE in_object_type 
    WHEN 'TABLE' THEN 
      IF is_replace_mode(ora_dict_ddl)  
      THEN
        l_result := 'REPLACE';
      ELSE
        l_result := 'CREATE';
      END IF;
    WHEN 'SEQUENCE' THEN 
      IF is_replace_mode(ora_dict_ddl) 
      THEN
        l_result := 'REPLACE';
      ELSE
        l_result := 'CREATE';
      END IF;
    WHEN 'INDEX' THEN
      IF get_context('CREATE_OR_REPLACE_INDEXES') = 'ACTIVE' THEN
        l_result := 'REPLACE';
      ELSE
        l_result := 'CREATE';
      END IF;
    ELSE 
      IF (SYS_CONTEXT('USERENV', 'CLIENT_INFO') LIKE 'CORT_BUILD=%') THEN
        l_result := 'REPLACE';
      ELSE
        l_result := 'CREATE';
      END IF;
    END CASE;  
    RETURN l_result;
  END get_execution_mode;
  
  
  -- Returns 'REPLACE' if there is #OR REPLACE hint in given DDL or if this parameter is turned on for session.
  -- Otherwise returns 'CREATE'. It could be called only from DDL triggers
  -- Overrided for xplan
  FUNCTION get_execution_mode(
    in_sql_text    IN CLOB
  )
  RETURN VARCHAR2
  AS
    l_result VARCHAR2(30);
  BEGIN
    IF is_replace_mode(in_sql_text) 
    THEN
      l_result := 'REPLACE';
    ELSE
      l_result := 'CREATE';
    END IF;
    RETURN l_result;
  END get_execution_mode;

  -- Main procedure is called from instead of trigger
  PROCEDURE instead_of_create
  AS
  BEGIN
    EXECUTE IMMEDIATE q'{  BEGIN
    cort_aux_pkg.instead_of_create(
      in_object_type    => :in_ora_dict_obj_type,
      in_object_name    => :in_ora_dict_obj_name,
      in_object_owner   => :in_ora_dict_obj_owner,
      in_sql            => :in_ora_dict_ddl
    );
  END;}' USING ora_dict_obj_type,
               ora_dict_obj_name,
               ora_dict_obj_owner,               
               ora_dict_ddl;     
  END instead_of_create;
  
  -- Main procedure is called from before create trigger
  PROCEDURE before_create
  AS
  BEGIN
    EXECUTE IMMEDIATE q'{  BEGIN
    cort_aux_pkg.before_create(
      in_object_type    => :in_ora_dict_obj_type,
      in_object_name    => :in_ora_dict_obj_name,
      in_object_owner   => :in_ora_dict_obj_owner,
      in_sql            => :in_ora_dict_ddl
    );
  END;}' USING ora_dict_obj_type,
               ora_dict_obj_name,
               NVL(ora_dict_obj_owner,'"NULL"'),
               ora_dict_ddl;     
  END before_create;

  -- run CORT statment in TEST mode and populate explain_plan table with results
  PROCEDURE explain_statement(
    in_statement_id  IN VARCHAR2,
    in_plan_id       IN NUMBER,
    in_timestamp     IN DATE,
    out_sql          OUT NOCOPY CLOB 
  )
  AS
  BEGIN
    g_last_id := 0;
    EXECUTE IMMEDIATE q'{  BEGIN
    cort_aux_pkg.explain_cort_sql(
      in_statement_id  => :in_statement_id,
      in_plan_id       => :in_plan_id,
      in_timestamp     => :in_timestamp,
      out_sql          => :out_sql,
      out_last_id      => :out_last_id
    );
  END;}' USING in_statement_id,
               in_plan_id,
               in_timestamp,
           OUT out_sql, 
           OUT g_last_id;     
  END explain_statement;
  
  -- called from row-level trigger on plan table
  PROCEDURE before_insert_xplan(
    io_id           IN OUT INTEGER,
    io_parent_id    IN OUT INTEGER,
    io_depth        IN OUT INTEGER,  
    io_operation    IN OUT VARCHAR2, 
    in_statement_id IN VARCHAR2,
    in_plan_id      IN NUMBER,
    in_timestamp    IN TIMESTAMP, 
    out_other_xml   OUT NOCOPY CLOB  
  )
  AS
  BEGIN
    IF io_id = 0 THEN
      g_last_id := 0;
      IF io_operation = 'CREATE TABLE STATEMENT' THEN
        cort_trg_pkg.explain_statement(
          in_statement_id  => in_statement_id,
          in_plan_id       => in_plan_id,
          in_timestamp     => in_timestamp,
          out_sql          => out_other_xml 
        );
        IF g_last_id > 0 THEN 
          io_operation := 'CREATE OR REPLACE TABLE (CORT) STATEMENT';
        END IF;  
      END IF;
    ELSE
      IF g_last_id > 0 THEN 
        io_id := io_id + g_last_id; 
        io_parent_id := io_parent_id + g_last_id;
        io_depth := io_depth + g_last_id;
      END IF;     
    END IF;
  END before_insert_xplan;


END cort_trg_pkg;
/