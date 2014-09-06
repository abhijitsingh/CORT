CREATE OR REPLACE PACKAGE cort_parse_pkg
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

  FUNCTION get_regexp_const(
    in_value          IN VARCHAR2
  )
  RETURN VARCHAR2;

  FUNCTION is_simple_name(in_name IN VARCHAR2)
  RETURN BOOLEAN;

  -- parses SQL
  PROCEDURE initial_parse_sql(
    in_sql           IN CLOB,
    in_operation     IN VARCHAR2,-- CREATE/DROP
    in_object_type   IN VARCHAR2,
    in_object_name   IN VARCHAR2,
    in_object_owner  IN VARCHAR2,  
    io_params_rec    IN OUT NOCOPY cort_params_pkg.gt_params_rec
  );

  -- Parse AS subquery definition
  PROCEDURE parse_as_select(
    out_as_select OUT BOOLEAN
  );

  -- set subquery return 0 rows
  PROCEDURE modify_as_select(
    io_sql IN OUT NOCOPY CLOB
  );

  -- return TRUE if as_select subquery contains table name
  FUNCTION as_select_from(in_table_name IN VARCHAR)
  RETURN BOOLEAN;
  
  -- determines partitions position
  PROCEDURE parse_partitioning(
    out_partitioning_type    OUT VARCHAR2,  
    out_subpartitioning_type OUT VARCHAR2 
  );
  
  -- replaces partitions definition in original_sql
  PROCEDURE replace_partitions_sql(
    io_sql           IN OUT NOCOPY CLOB,
    in_partition_sql IN CLOB
  );

  -- parses columns positions and cort-values
  PROCEDURE parse_columns(
    io_table_rec    IN OUT NOCOPY cort_exec_pkg.gt_table_rec
  );

  -- normalize check constraint search condition
  PROCEDURE normalize_expression(
    in_expr  IN  VARCHAR2,
    out_expr OUT VARCHAR2
  );

  -- replaces table name and all names of existing depending objects (constraints, log groups, indexes, lob segments) 
  PROCEDURE replace_names(
    in_table_rec IN cort_exec_pkg.gt_table_rec,
    io_sql       IN OUT NOCOPY CLOB 
  );
  
  -- replaces index and table names for CREATE INDEX statement 
  PROCEDURE replace_names(
    in_table_rec IN cort_exec_pkg.gt_table_rec,
    in_index_rec IN cort_exec_pkg.gt_index_rec,
    io_sql       IN OUT NOCOPY CLOB 
  );
  
  -- replaces sequence name 
  PROCEDURE replace_names(
    in_sequence_rec IN cort_exec_pkg.gt_sequence_rec,
    io_sql          IN OUT NOCOPY CLOB 
  );
  
  -- replaces names in expression  
  PROCEDURE update_expression(
    io_expression      IN OUT NOCOPY VARCHAR2,
    in_replace_names   IN arrays.gt_str_indx
  );

  -- return schema name specified in SQL 
  FUNCTION get_sql_schema_name
  RETURN VARCHAR2;

  -- return index' table/cluster name and owner
  PROCEDURE get_index_main_object(
    out_object_type  OUT VARCHAR2,
    out_object_owner OUT VARCHAR2,
    out_object_name  OUT VARCHAR2
  );

  -- return original name for renamed object. If it wasn't rename return current name 
  FUNCTION get_original_name(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2
  )
  RETURN VARCHAR2;

  -- parses create statement and return object type, owner and name
  PROCEDURE parse_create_statement(
    in_sql           IN CLOB,
    out_object_type  OUT VARCHAR2,
    out_object_owner OUT VARCHAR2,
    out_object_name  OUT VARCHAR2
  );
 
END cort_parse_pkg;
/