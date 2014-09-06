CREATE OR REPLACE PACKAGE cort_exec_pkg
AUTHID CURRENT_USER
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
  Description: Main package executing table recreation and rollback with current user privileges
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.01   | Rustam Kafarov    | Added support for sequences, indexes, builds, create table as select. Orevall improvements
  ----------------------------------------------------------------------------------------------------------------------  
*/

  gc_max_end_time            CONSTANT TIMESTAMP(9) := TIMESTAMP'9999-12-31 23:59:59.999999999';

  gc_cort_text_prefix        CONSTANT VARCHAR2(1)  := cort_params_pkg.get_config_param('CORT_TEXT_PREFIX');
  gc_cort_rlbk_prefix        CONSTANT VARCHAR2(5)  := cort_params_pkg.get_config_param('CORT_RLBK_PREFIX');
  gc_cort_temp_prefix        CONSTANT VARCHAR2(5)  := cort_params_pkg.get_config_param('CORT_TEMP_PREFIX');
  gc_cort_swap_prefix        CONSTANT VARCHAR2(5)  := cort_params_pkg.get_config_param('CORT_SWAP_PREFIX');
  gc_cort_stat_prefix        CONSTANT VARCHAR2(5)  := cort_params_pkg.get_config_param('CORT_STAT_PREFIX');

  gc_begin_message           CONSTANT VARCHAR2(1000) := cort_params_pkg.get_config_param('BEGIN_MESSAGE');
  gc_end_message             CONSTANT VARCHAR2(1000) := cort_params_pkg.get_config_param('END_MESSAGE');

  -- column level params
  gc_value                   CONSTANT VARCHAR2(30) := cort_params_pkg.get_config_param('CORT_VALUE');
  gc_force_value             CONSTANT VARCHAR2(30) := cort_params_pkg.get_config_param('CORT_FORCE_VALUE');
  gc_application_regexp      CONSTANT VARCHAR2(30) := cort_params_pkg.get_config_param('APPLICATION_REGEXP');
  gc_release_regexp          CONSTANT VARCHAR2(30) := cort_params_pkg.get_config_param('RELEASE_REGEXP');
  gc_cort_stat_table         CONSTANT VARCHAR2(30) := cort_params_pkg.get_config_param('CORT_STAT_TABLE');

  

  -- table level params
  -- param hints
  gc_application             CONSTANT VARCHAR2(30) := 'APPLICATION';
  gc_release                 CONSTANT VARCHAR2(30) := 'RELEASE';
  gc_parallel                CONSTANT VARCHAR2(30) := 'PARALLEL';
  gc_no_parallel             CONSTANT VARCHAR2(30) := 'NO_PARALLEL';
  gc_alter                   CONSTANT VARCHAR2(30) := 'ALTER';
  gc_no_alter                CONSTANT VARCHAR2(30) := 'NO_ALTER';
  gc_debug                   CONSTANT VARCHAR2(30) := 'DEBUG';
  gc_no_debug                CONSTANT VARCHAR2(30) := 'NO_DEBUG'; 
  gc_echo                    CONSTANT VARCHAR2(30) := 'ECHO';
  gc_no_echo                 CONSTANT VARCHAR2(30) := 'NO_ECHO';
  gc_log                     CONSTANT VARCHAR2(30) := 'LOG';
  gc_no_log                  CONSTANT VARCHAR2(30) := 'NO_LOG';
  gc_physical_attr           CONSTANT VARCHAR2(30) := 'PHYSICAL_ATTRIBUTES';
  gc_no_physical_attr        CONSTANT VARCHAR2(30) := 'NO_PHYSICAL_ATTRIBUTES';
  gc_keep_data               CONSTANT VARCHAR2(30) := 'KEEP_DATA';
  gc_no_data                 CONSTANT VARCHAR2(30) := 'NO_DATA';
  gc_keep_refs               CONSTANT VARCHAR2(30) := 'KEEP_REFS';
  gc_no_refs                 CONSTANT VARCHAR2(30) := 'NO_REFS';
  gc_bad_refs                CONSTANT VARCHAR2(30) := 'KEEP_BAD_REFS';
  gc_no_bad_refs             CONSTANT VARCHAR2(30) := 'NO_BAD_REFS';
  gc_validate                CONSTANT VARCHAR2(30) := 'VALIDATE';
  gc_no_validate             CONSTANT VARCHAR2(30) := 'NO_VALIDATE';
  gc_keep_privs              CONSTANT VARCHAR2(30) := 'KEEP_PRIVS';
  gc_no_privs                CONSTANT VARCHAR2(30) := 'NO_PRIVS';
  gc_keep_indexes            CONSTANT VARCHAR2(30) := 'KEEP_INDEXES';
  gc_no_indexes              CONSTANT VARCHAR2(30) := 'NO_INDEXES';
  gc_keep_triggers           CONSTANT VARCHAR2(30) := 'KEEP_TRIGGERS';
  gc_no_triggers             CONSTANT VARCHAR2(30) := 'NO_TRIGGERS';
  gc_keep_comments           CONSTANT VARCHAR2(30) := 'KEEP_COMMENTS';
  gc_no_comments             CONSTANT VARCHAR2(30) := 'NO_COMMENTS';
  gc_keep_stats              CONSTANT VARCHAR2(30) := 'KEEP_STATS';
  gc_no_stats                CONSTANT VARCHAR2(30) := 'NO_STATS';
  gc_keep_partitions         CONSTANT VARCHAR2(30) := 'KEEP_PARTITIONS';
  gc_no_partitions           CONSTANT VARCHAR2(30) := 'NO_PARTITIONS';
  gc_keep_subpartitions      CONSTANT VARCHAR2(30) := 'KEEP_SUBPARTITIONS';
  gc_no_partitions           CONSTANT VARCHAR2(30) := 'NO_SUBPARTITIONS';
  gc_keep_temp_table         CONSTANT VARCHAR2(30) := 'KEEP_TEMP_TABLE';
  gc_drop_temp_table         CONSTANT VARCHAR2(30) := 'DROP_TEMP_TABLE';

  TYPE gt_rename_rec IS RECORD(
    object_name                  VARCHAR2(30),  -- orginal object name
    object_owner                 VARCHAR2(30),  -- object owner
    object_id                    NUMBER,        -- object_id
    generated                    CHAR(1),       -- Specify is name generated (Y) or not (N)
    current_name                 VARCHAR2(30),  -- current name
    cort_name                    VARCHAR2(30),  -- cort name
    temp_name                    VARCHAR2(30)   -- temp name
  );
  
  TYPE gt_storage_rec IS RECORD(
    initial_extent               VARCHAR2(40), -- Size of the initial extent (in bytes); NULL for partitioned tables
--    next_extent                  VARCHAR2(40), -- Size of secondary extents (in bytes); NULL for partitioned tables  /* NOT USED IN LOCAL TBS */
    min_extents                  VARCHAR2(40), -- Minimum number of extents allowed in the segment; NULL for partitioned tables
--    max_extents                  VARCHAR2(40), -- Maximum number of extents allowed in the segment; NULL for partitioned tables. /* NOT USED IN LOCAL TBS */
    max_size                     VARCHAR2(40), -- Maximum number of blocks allowed in the segment of the LOB data partition, or DEFAULT
    pct_increase                 VARCHAR2(40), -- Percentage increase in extent size; NULL for partitioned tables
    freelists                    VARCHAR2(40), -- Number of process freelists allocated to the segment; NULL for partitioned tables
    freelist_groups              VARCHAR2(40), -- Number of freelist groups allocated to the segment; NULL for partitioned tables
--    optimal_size  for rollback segments only (NOT USED)
    buffer_pool                  VARCHAR2(7)   -- Default buffer pool for the table; NULL for partitioned tables:
                                               --   DEFAULT, KEEP, RECYCLE, NULL
--    encrypt   for tablespaces only (NOT USED)                                           
  );
  
  TYPE gt_physical_attr_rec IS RECORD(
    pct_free                     PLS_INTEGER, -- Minimum percentage of free space in a block; NULL for partitioned tables
    pct_used                     PLS_INTEGER, -- Minimum percentage of used space in a block; NULL for partitioned tables
    ini_trans                    PLS_INTEGER, -- Initial number of transactions; NULL for partitioned tables
    max_trans                    PLS_INTEGER, -- Maximum number of transactions; NULL for partitioned tables
    storage                      gt_storage_rec
  );
  

  TYPE gt_compression_rec IS RECORD(
    compression                  VARCHAR2(8),  -- Indicates whether table compression is enabled (ENABLED) or not (DISABLED); NULL for partitioned tables
    compress_for                 VARCHAR2(18)  -- Default compression for what kind of operations:
                                               --   11.1: DIRECT LOAD ONLY, FOR ALL OPERATIONS, NULL
                                               --   11.2: BASIC, OLTP, QUERY LOW, QUERY HIGH, ARCHIVE LOW, ARCHIVE HIGH, NULL
                                               --   10.1 and 10.2: not used
  
  );
  
  TYPE gt_parallel_rec IS RECORD( 
    degree                       VARCHAR2(10), -- Number of threads per instance for scanning the table, or DEFAULT
    instances                    VARCHAR2(10)  -- Number of instances across which the table is to be scanned, or DEFAULT
  );
  
  
  TYPE gt_index_rec IS RECORD(
    owner                        VARCHAR2(30),        -- Owner of the index
    index_name                   VARCHAR2(30),        -- Name of the index
    index_type                   VARCHAR2(27),        -- Type of the index:
                                                      --   NORMAL, NORMAL/REV, BITMAP, FUNCTION-BASED NORMAL, 
                                                      --   FUNCTION-BASED NORMAL/REV, FUNCTION-BASED BITMAP, 
                                                      --   CLUSTER, IOT - TOP, DOMAIN
    table_owner                  VARCHAR2(30),        -- Owner of the indexed object
    table_name                   VARCHAR2(30),        -- Name of the indexed object
    table_type                   VARCHAR(30),         -- Type of the indexed object:
                                                      --   NEXT OBJECT, INDEX, TABLE, CLUSTER, VIEW, SYNONYM, SEQUENCE
    table_object_owner           VARCHAR2(30),        -- object owner for object table
    table_object_type            VARCHAR2(30),        -- object name for object table                                          
    column_arr                   arrays.gt_str_arr,   -- Column names ordered by position
    sort_order_arr               arrays.gt_str_arr,   -- Sort orders (ASC,DESC) ordered by position
    column_expr_arr              arrays.gt_xlstr_arr, -- Column expressions ordered by position
    uniqueness                   VARCHAR2(9),         -- Indicates whether the index is unique (UNIQUE) or nonunique (NONUNIQUE)
    compression                  VARCHAR2(8),         -- Indicates whether index compression is enabled (ENABLED) or not (DISABLED)
    prefix_length                PLS_INTEGER,         -- Number of columns in the prefix of the compression key
    tablespace_name              VARCHAR2(30),        -- Name of the tablespace containing the index
    physical_attr_rec            gt_physical_attr_rec,--  physical attributes
    pct_threshold                NUMBER,              -- Threshold percentage of block space allowed per index entry
    include_column               PLS_INTEGER,         -- Column ID of the last column to be included in index-organized table primary key (non-overflow) index. This column maps to the COLUMN_ID column of the *_TAB_COLUMNS view.
    parallel_rec                 gt_parallel_rec,     -- parallel degree 
    logging                      VARCHAR2(7),         -- Indicates whether or not changes to the table are logged; NULL for partitioned tables: YES NO
    partitioned                  VARCHAR2(3),         -- Indicates whether the table is partitioned (YES) or not (NO)
    partitioning_type            VARCHAR2(9),         -- Type of the partitioning method:
                                                      --     RANGE, HASH, SYSTEM, LIST, REFERENCE
    subpartitioning_type         VARCHAR2(7),         -- Type of the composite partitioning method:
                                                      --     RANGE, HASH, SYSTEM, LIST
    part_key_column_arr          arrays.gt_str_arr,   -- List of partition key columns
    subpart_key_column_arr       arrays.gt_str_arr,   -- List of subpartition key columns
    locality                     VARCHAR2(6),         -- Indicates whether the partitioned index is local (LOCAL) or global (GLOBAL)
    alignment                    VARCHAR2(12),        -- Indicates whether the partitioned index is prefixed (PREFIXED) or non-prefixed (NON_PREFIXED)    
    interval                     VARCHAR2(1000),      -- String of the interval value
    temporary                    VARCHAR2(1),         -- Indicates whether the index is on a temporary table (Y) or not (N)
    duration                     VARCHAR2(15),        -- Indicates the duration of a temporary table:
                                                      --    SYS$SESSION - Rows are preserved for the duration of the session
                                                      --    SYS$TRANSACTION - Rows are deleted after COMMIT
    generated                    VARCHAR2(1),         -- Indicates whether the name of the index is system-generated (Y) or not (N)
    secondary                    VARCHAR2(1),         -- Indicates whether the index is a secondary object created by theODCIIndexCreate method of the Oracle Data Cartridge (Y) or not (N)
    pct_direct_access            NUMBER,              -- For a secondary index on an index-organized table, the percentage of rows with VALID guess
    ityp_owner                   VARCHAR2(30),        -- For a domain index, the owner of the indextype
    ityp_name                    VARCHAR2(30),        -- For a domain index, the name of the indextype
    parameters                   VARCHAR2(1000),      -- For a domain index, the parameter string
    domidx_status                VARCHAR2(12),        -- Status of a domain index:
                                                      --    NULL - Index is not a domain index
                                                      --    VALID - Index is a valid domain index
                                                      --    IDXTYP_INVLD - Indextype of the domain index is invalid
    domidx_opstatus              VARCHAR2(6),         -- Status of the operation on a domain index:
                                                      --    NULL - Index is not a domain index
                                                      --    VALID - Operation performed without errors
                                                      --    FAILED - Operation failed with an error
    funcidx_status               VARCHAR2(8),         -- Status of a function-based index:
                                                      --    NULL - Index is not a function-based index
                                                      --    ENABLED - Function-based index is enabled
                                                      --    DISABLED - Function-based index is disabled
    visibility                   VARCHAR2(10),        -- Indicates whether the index is VISIBLE or INVISIBLE to the optimizer
    join_index                   VARCHAR2(3),         -- Indicates whether the index is a join index (YES) or not (NO)
    join_inner_owner_arr         arrays.gt_str_arr,   -- Array of owner of join inner table (for join index)
    join_inner_table_arr         arrays.gt_str_arr,   -- Array of name of join inner table (for join index)
    join_inner_column_arr        arrays.gt_str_arr,   -- Array of column of join inner table (for join index)
    join_outer_owner_arr         arrays.gt_str_arr,   -- Array of owner of join inner table (for join index)
    join_outer_table_arr         arrays.gt_str_arr,   -- Array of name of join inner table (for join index)
    join_outer_column_arr        arrays.gt_str_arr,   -- Array of column of join inner table (for join index)
    column_table_owner_arr       arrays.gt_str_arr,   -- Array of owners of tables of columns (for join index)
    column_table_arr             arrays.gt_str_arr,   -- Array of tables of columns (for join index)
    recreate_flag                BOOLEAN,             -- Indicates index need to be recreated
    constraint_name              VARCHAR2(30),        -- Constraint (PK/UK only) name assigned to this index. 
    rename_rec                   gt_rename_rec,       -- record holding names for renaming
    drop_flag                    BOOLEAN              -- Indicates that DROP claue has been already generated for this index or index has been dropped implicitly
  );
                       
  TYPE gt_index_arr IS TABLE OF gt_index_rec INDEX BY PLS_INTEGER;
  
         
  TYPE gt_constraint_rec IS RECORD(
    owner                        VARCHAR2(30),   -- Owner of the constraint definition
    constraint_name              VARCHAR2(30),   -- Name of the constraint definition
    constraint_type              VARCHAR2(1),    -- Type of the constraint definition:
                                                 --    C - Check constraint on a table
                                                 --    P - Primary key
                                                 --    U - Unique key
                                                 --    R - Referential integrity
                                                 --    V - With check option, on a view
                                                 --    O - With read only, on a view
                                                 --    H - Hash expression
                                                 --    F - Constraint that involves a REF column
                                                 --    S - Supplemental logging
    table_name                   VARCHAR2(30),   -- Name associated with the table with the constraint definition
    search_condition             VARCHAR2(32767),-- Text of search condition for a check constraint
    column_arr                   arrays.gt_str_arr,  -- Column names ordered by position
    r_owner                      VARCHAR2(30),   -- Owner of the table referred to in a referential constraint
    r_constraint_name            VARCHAR2(30),   -- Name of the unique constraint definition for the referenced table
    r_table_name                 VARCHAR2(30),   -- Referencing table name
    r_column_arr                 arrays.gt_str_arr, -- Referencing table key column names ordered by position
    delete_rule                  VARCHAR2(9),    -- Delete rule for a referential constraint:
                                                 --    CASCADE
                                                 --    SET NULL
                                                 --    NO ACTION
    status                       VARCHAR2(8),    -- Enforcement status of the constraint:
                                                 --    ENABLED
                                                 --    DISABLED
    deferrable                   VARCHAR2(14),   -- Indicates whether the constraint is deferrable (DEFERRABLE) or not (NOT DEFERRABLE)
    deferred                     VARCHAR2(9),    -- Indicates whether the constraint was initially deferred (DEFERRED) or not (IMMEDIATE)
    validated                    VARCHAR2(13),   -- Indicates whether all data obeys the constraint (VALIDATED) or not (NOT VALIDATED)
    generated                    VARCHAR2(30),   -- Indicates whether the name of the constraint is user-generated (USER NAME) or system-generated (GENERATED NAME)
    bad                          VARCHAR2(3),    -- Indicates whether this constraint specifies a century in an ambiguous manner (BAD) or not (NULL). 
                                                 -- To avoid errors resulting from this ambiguity, rewrite the constraint using the TO_DATE function with a four-digit year.
    rely                         VARCHAR2(4),    -- Indicates whether an enabled constraint is enforced (RELY) or unenforced (NULL)
    index_owner                  VARCHAR2(30),   -- Name of the user owning the index
    index_name                   VARCHAR2(30),   -- Name of the index (only shown for unique and primary-key constraints)
    rename_rec                   gt_rename_rec,  -- record holding names for renaming
    has_references               BOOLEAN,        -- Has references or not. Valid only for PK/UK
    drop_flag                    BOOLEAN         -- Indicates that DROP claue has been already generated for this constraint or constraint has been dropped implicitly
  );
  
  TYPE gt_constraint_arr IS TABLE OF gt_constraint_rec INDEX BY PLS_INTEGER;
  
  
  TYPE gt_log_group_rec IS RECORD(
    owner                        VARCHAR2(30),   --   Owner of the log group definition
    log_group_name               VARCHAR2(30),   --   Name of the log group definition
    table_name                   VARCHAR2(30),   --   Name of the table on which the log group is defined
    log_group_type               VARCHAR2(19),   --   Type of the log group:
                                                 --      PRIMARY KEY LOGGING
                                                 --      UNIQUE KEY LOGGING
                                                 --      FOREIGN KEY LOGGING
                                                 --      ALL COLUMN LOGGING
                                                 --      USER LOG GROUP
    always                       VARCHAR2(11),   -- Y indicates the log group is logged any time a row is updated; N indicates the log group is logged any time a member column is updated
    generated                    VARCHAR2(14),   -- Indicates whether the name of the supplemental log group was system generated (GENERATED NAME) or not (USER NAME)    
    column_arr                   arrays.gt_lstr_arr, -- Column names ordered by position
    column_log_arr               arrays.gt_str_arr,  -- Column logging property ordered by position
    rename_rec                   gt_rename_rec,  -- record holding names for renaming
    drop_flag                    BOOLEAN         -- Indicates that DROP claue has been already generated for this constraint
  );

  TYPE gt_log_group_arr   IS TABLE OF gt_log_group_rec  INDEX BY PLS_INTEGER;
  

  TYPE gt_cort_value_rec IS RECORD(
    expression                   VARCHAR2(32767), -- SQL expression
    release                      VARCHAR2(20),    -- release
    valid                        BOOLEAN,         -- TRUE if expression is valid, FALSE if it is not
    force_value                  BOOLEAN          -- TRUE for "force_value", FALSE for "value"
  );

  TYPE gt_cort_value_arr IS TABLE OF gt_cort_value_rec INDEX BY PLS_INTEGER; 

    
  TYPE gt_column_rec IS RECORD(
    owner                        VARCHAR2(30),  -- Owner of the table, view, or cluster
    table_name                   VARCHAR2(30),  -- Name of the table, view, or cluster
    column_name                  VARCHAR2(30),  -- Column name
    column_indx                  PLS_INTEGER,   -- Index in column_arr array
    data_type                    VARCHAR2(106), -- Datatype of the column
    data_type_mod                VARCHAR2(3),   -- Datatype modifier of the column
    data_type_owner              VARCHAR2(30),  -- Owner of the datatype of the column
    data_length                  PLS_INTEGER,   -- Length of the column (in bytes)
    data_precision               PLS_INTEGER,   -- Decimal precision for NUMBER datatype; binary precision for FLOAT datatype; NULL for all other datatypes
    data_scale                   PLS_INTEGER,   -- Digits to the right of the decimal point in a number
    nullable                     VARCHAR2(1),   -- Indicates whether a column allows NULLs. The value is N if there is a NOT NULL constraint on the column or if the column is part of aPRIMARY KEY
    notnull_constraint_name      VARCHAR2(30),  -- Name of NOT NULL constraint (populated only if name is USER GENERATED)
    column_id                    PLS_INTEGER,   -- Sequence number of the column as created
    data_default                 VARCHAR2(32767),--Default value for the column
    character_set_name           VARCHAR2(44),  -- Name of the character set: CHAR_CS, NCHAR_CS
    char_col_decl_length         PLS_INTEGER,   -- Declaration length of the character type column
    char_length                  PLS_INTEGER,   -- Displays the length of the column in characters. This value only applies to the following datatypes:
                                                --  CHAR  VARCHAR2  NCHAR  NVARCHAR
    char_used                    VARCHAR2(1),   -- Indicates that the column uses BYTE length semantics (B) or CHAR length semantics (C), or whether the datatype is not any of the following (NULL):
                                                --  CHAR  VARCHAR2  NCHAR  NVARCHAR
    hidden_column                VARCHAR2(3),   -- Indicates whether the column is a hidden column (YES) or not (NO)
    virtual_column               VARCHAR2(3),   -- Indicates whether the column is a virtual column (YES) or not (NO)
    segment_column_id            PLS_INTEGER,   -- Sequence number of the column in the segment
    internal_column_id           PLS_INTEGER,   -- Internal sequence number of the column
    qualified_col_name           VARCHAR2(4000),-- Qualified column name
    encryption_alg               VARCHAR2(29),  -- Encryption algorithm used to protect secrecy of data in this column:
                                                --   3 Key Triple DES 168 bits key, AES 128 bits key, AES 192 bits key, AES 256 bits key
    salt                         VARCHAR2(3),   -- Indicates whether the column is encrypted with SALT (YES) or not (NO)
    join_index_column            BOOLEAN,       -- indicate whether column is included into at least one join index (TRUE) or not (FALSE)
    join_index_arr               arrays.gt_lstr_arr, -- array of join indexes use this column
    partition_key                BOOLEAN,       -- indicates whether column is inclided into partition/subpartition key columns
    new_column_name              VARCHAR2(30),  -- new column name in case of renaming
    temp_column_name             VARCHAR2(30),  -- temp column name for 2-way renaming process
    sql_start_position           PLS_INTEGER,   -- Start position of definition 
    sql_end_position             PLS_INTEGER,   -- End position of definition
    sql_next_start_position      PLS_INTEGER,   -- Start position of definition of the next column
    cort_values                  gt_cort_value_arr, -- cort values array
    cort_value_indx              PLS_INTEGER,   -- index of valid values. NULL if there is not value
    matched_column_id            PLS_INTEGER    -- column_id of matched column from another table structure
  );                    
  
  
  TYPE gt_column_arr IS TABLE OF gt_column_rec INDEX BY PLS_INTEGER;
  
  TYPE gt_lob_rec IS RECORD (
    owner                        VARCHAR2(30),   -- Owner of the object containing the LOB
    table_name                   VARCHAR2(30),   -- Name of the object containing the LOB
    column_name                  VARCHAR2(4000), -- Name of the LOB column or attribute
    column_indx                  PLS_INTEGER,    -- Column index in column_arr 
    lob_name                     VARCHAR2(30),   -- Name of the LOB segment
    lob_index_name               VARCHAR2(30),   -- Name of the LOB index
    partition_name               VARCHAR2(30),   -- Name of the table partition/subpartition
    partition_level              VARCHAR2(30),   -- PARTITION/SUBPARTITION
    partition_position           NUMBER,         -- Position of the LOB data partition/subpartition within the LOB item
    parent_lob_part_name         VARCHAR2(30),   -- LOB_PARTITION_NAME for lob subpartition 
    lob_partition_name           VARCHAR2(30),   -- Name of the LOB data partition/subpartition
    lob_indpart_name             VARCHAR2(30),   -- Name of the corresponding LOB index partition/subpartition
    tablespace_name              VARCHAR2(30),   -- Name of the tablespace containing the LOB segment
    chunk                        NUMBER,         -- Size (in bytes) of the LOB chunk as a unit of allocation or manipulation
    pctversion                   VARCHAR2(30),   -- Maximum percentage of the LOB space used for versioning
    retention                    VARCHAR2(30),   -- Maximum time duration for versioning of the LOB space
    freepools                    VARCHAR2(30),   -- Number of freepools for this LOB segment
    cache                        VARCHAR2(10),   -- Indicates whether and how the LOB data is to be cached in the buffer cache:
                                                 --   YES - LOB data is placed in the buffer cache
                                                 --   NO - LOB data either is not brought into the buffer cache or is brought into the buffer cache and placed at the least recently used end of the LRU list
                                                 --   CACHEREADS - LOB data is brought into the buffer cache only during read operations but not during write operations
    logging                      VARCHAR2(7),    -- Indicates whether or not changes to the LOB are logged:
                                                 --   NONE, YES, NO
    encrypt                      VARCHAR2(4),    -- Indicates whether or not the LOB is encrypted:
                                                 --   YES,  NO,  NONE - Not applicable to BasicFile LOBs
    compression                  VARCHAR2(6),    -- Level of compression used for this LOB:
                                                 --   MEDIUM,  HIGH,  NO,  NONE - Not applicable to BasicFile LOBs
    deduplication                VARCHAR2(15),   -- Kind of deduplication used for this LOB:
                                                 --   LOB - Deduplicate, NO - Keep duplicates, NONE - Not applicable to BasicFile LOBs
    in_row                       VARCHAR2(3),    -- Indicates whether some of the LOBs are stored inline with the base row (YES) or not (NO). For partitioned objects, refer to the *_LOB_PARTITIONS and *_PART_LOBS views.
    partitioned                  VARCHAR2(3),    -- Indicates whether the LOB column is in a partitioned table (YES) or not (NO)
    securefile                   VARCHAR2(3),    -- Indicates whether the LOB is a SecureFile LOB (YES) or not (NO)
    storage                      gt_storage_rec,  
    minretention                 VARCHAR2(40),   -- Minimum retention duration for SECUREFILE segment, or DEFAULT
    -- 11.2 onnly
    flash_cache                  VARCHAR2(7),    -- Database Smart Flash Cache hint to be used for partition blocks:
                                                 --    DEFAULT, KEEP, NONE
    cell_flash_cache             VARCHAR2(7),    -- Cell flash cache hint to be used for partition blocks:
                                                 --    DEFAULT, KEEP, NONE
    xml_column_indx              PLS_INTEGER,    -- index of related XMLTYPE column. Normally equals to column_indx-1                                             
    varray_column_indx           PLS_INTEGER,    -- index of related VARRAY column. Normally equals to column_indx                                             
    rename_rec                   gt_rename_rec   -- record holding names for renaming
  );
 
  TYPE gt_lob_arr IS TABLE OF gt_lob_rec INDEX BY PLS_INTEGER;


  TYPE gt_lob_template_rec IS RECORD (
    subpartition_name            VARCHAR2(34),   -- Name of the subpartition
    lob_column_name              VARCHAR2(30),   -- Name of the LOB column
    lob_segment_name             VARCHAR2(30),   -- Name of the LOB segment
    tablespace_name              VARCHAR2(30)    -- Tablespace name 
  );  
    
  TYPE gt_lob_template_arr IS TABLE OF gt_lob_template_rec INDEX BY PLS_INTEGER;
  
  
  TYPE gt_xml_col_rec IS RECORD(
    owner                        VARCHAR2(30),    -- Owner of the XML table
    table_name                   VARCHAR2(30),    -- Name of the XML table
    column_name                  VARCHAR2(4000),  -- Name of the XML table column
    column_indx                  PLS_INTEGER,     -- Column index in column_arr
    lob_column_indx              PLS_INTEGER,     -- Lob column index in column_arr
    xmlschema                    VARCHAR2(700),   -- Name of the XML Schema that is used for the table definition
    schema_owner                 VARCHAR2(30),    -- Owner of the XML Schema that is used for the table definition
    element_name                 VARCHAR2(2000),  -- Name of the XML SChema element that is used for the table
    storage_type                 VARCHAR2(17),    -- Storage option for the XMLtype data:
                                                  --   OBJECT-RELATIONAL,  BINARY,  CLOB
    anyschema                    VARCHAR2(3),     -- If storage is BINARY, indicates whether the column allows ANYSCHEMA (YES) or not (NO), else NULL
    nonschema                    VARCHAR2(3)      -- If storage is BINARY, indicates whether the column allows NONSCHEMA (YES) or not (NO), else NULL  
  );

  TYPE gt_xml_col_arr IS TABLE OF gt_xml_col_rec INDEX BY PLS_INTEGER;


  TYPE gt_varray_rec IS RECORD(
    owner                        VARCHAR2(30),    -- Owner of the table containing the varray
    table_name                   VARCHAR2(30),    -- Name of the containing table
    column_name                  VARCHAR2(4000),  -- Name of the varray column or attribute
    column_indx                  PLS_INTEGER,     -- Column index in column_arr
    type_owner                   VARCHAR2(30),    -- Owner of the varray type
    type_name                    VARCHAR2(30),    -- Name of the varray type
    lob_name                     VARCHAR2(30),    -- Name of the LOB if the varray is stored in a LOB
    lob_column_indx              PLS_INTEGER,     -- Lob column index in column_arr
    storage_spec                 VARCHAR2(30),    -- Indicates whether the storage was defaulted (DEFAULT) or user-specified (USER_SPECIFIED)
    return_type                  VARCHAR2(20),    -- Return type of the column: LOCATOR,  VALUE
    element_substitutable        VARCHAR2(25)     -- Indicates whether the varray element is substitutable (Y) or not (N)
  ); 

  TYPE gt_varray_arr IS TABLE OF gt_varray_rec INDEX BY PLS_INTEGER;


  TYPE gt_partition_rec IS RECORD (
    table_owner                  VARCHAR2(30),   -- Owner of the table
    table_name                   VARCHAR2(30),   -- Name of the table
    composite                    VARCHAR2(3),    -- Indicates whether the table is composite-partitioned (YES) or not (NO)
    partition_level              VARCHAR2(30),   -- PARTITION/SUBPARTITION
    partition_type               VARCHAR2(30),   -- LIST/RANGE/HASH/SYSTEM/REFERENCE
    partition_name               VARCHAR2(30),   -- Name of the partition
    parent_partition_name        VARCHAR2(30),   -- Name of the parent partition (for subpartitions)
    high_value                   VARCHAR2(32767),-- Partition/subartition bound value expression
    position                     NUMBER,         -- Position of the partition/subpartition within the table/partition
    physical_attr_rec            gt_physical_attr_rec, -- physical attributes
    compression_rec              gt_compression_rec,   -- compression
    parallel_rec                 gt_parallel_rec,      -- parallel degree 
    logging                      VARCHAR2(7),    -- Indicates whether or not changes to the table are logged:
                                                 --   NONE - Not specified, YES, NO
    tablespace_name              VARCHAR2(30),   -- Tablespace name
    iot_key_compression          VARCHAR2(8),    -- Indicates whether index compression is enabled (ENABLED) or not (DISABLED)
    overflow_tablespace          VARCHAR2(30),   -- Tablespace of overflow tablespace name
    overflow_physical_attr_rec   gt_physical_attr_rec, -- physical attributes
    overflow_logging             VARCHAR2(7),    -- Indicates whether or not changes to the table are logged; NULL for partitioned tables: YES NO
    parent_table_partition       VARCHAR2(30),   -- Parent table's corresponding partition
    interval                     VARCHAR2(3),    -- Indicates whether the partition is in the interval section of an interval partitioned table (YES) or whether the partition is in the range section (NO)                                              
    subpartition_from_indx       PLS_INTEGER,    -- left range in subpartition array 
    subpartition_to_indx         PLS_INTEGER,    -- right range in subpartition array
    lob_arr                      gt_lob_arr,     -- LOB attributes
    lob_indx_arr                 arrays.gt_int_indx, -- index by lob columnid
    indx                         PLS_INTEGER,    -- Index in array
    matching_indx                PLS_INTEGER,    -- index of matching partition from source table ([populated only for target table)
    is_partition_empty           BOOLEAN         -- indicates is partition empty or not
  );     
  
  TYPE gt_partition_arr IS TABLE OF gt_partition_rec INDEX BY PLS_INTEGER;
  
  
  TYPE gt_subpartition_template_rec IS RECORD (
    subpartition_type            VARCHAR2(30),   -- Type of the subpartitioning
    subpartition_name            VARCHAR2(34),   -- Name of the subpartition
    subpartition_position        NUMBER,         -- Position of the subpartition
    tablespace_name              VARCHAR2(30),   -- Tablespace name of the subpartition
    high_bound                   VARCHAR2(32767),-- Literal list values of the subpartition
    lob_template_arr             gt_lob_template_arr -- LOB templates
  );  
    
  TYPE gt_subpartition_template_arr IS TABLE OF gt_subpartition_template_rec INDEX BY PLS_INTEGER;
  
  
  TYPE gt_privilege_rec IS RECORD (
    grantor                      VARCHAR2(30),   -- Name of the user who performed the grant
    grantee                      VARCHAR2(30),   -- Name of the user to whom access was granted
    table_schema                 VARCHAR2(30),   -- Schema of the object
    table_name                   VARCHAR2(30),   -- Name of the object
    column_name                  VARCHAR2(30),   -- Name of the column
    privilege                    VARCHAR2(40),   -- Privilege on the object
    grantable                    VARCHAR2(3),    -- Indicates whether the privilege was granted with the GRANT OPTION (YES) or not (NO) 
    hierarchy                    VARCHAR2(3)     -- Indicates whether the privilege was granted with the HIERARCHY OPTION (YES) or not (NO)
  );  
    
  TYPE gt_privilege_arr IS TABLE OF gt_privilege_rec INDEX BY PLS_INTEGER;
  
  
  TYPE gt_trigger_rec IS RECORD(
    owner                        VARCHAR2(30),   -- Owner of the trigger
    trigger_name                 VARCHAR2(30),   -- Name of the trigger
    trigger_type                 VARCHAR2(16),   -- When the trigger fires: BEFORE STATEMENT, BEFORE EACH ROW, AFTER STATEMENT, AFTER EACH ROW, INSTEAD OF, COMPOUND
    triggering_event             VARCHAR2(227),  -- DML, DDL, or database event that fires the trigger
    table_owner                  VARCHAR2(30),   -- Owner of the table on which the trigger is defined
    base_object_type             VARCHAR2(16),   -- Base object on which the trigger is defined: TABLE, VIEW, SCHEMA, DATABASE
    table_name                   VARCHAR2(30),   -- If the base object type of the trigger is SCHEMA or DATABASE, then this column is NULL; if the base object type of the trigger is TABLE or VIEW, then this column indicates the table or view name on which the trigger is defined
    column_name                  VARCHAR2(4000), -- Name of the nested table column (if a nested table trigger), else NULL
    referencing_names            VARCHAR2(128),  -- Names used for referencing OLD and NEW column values from within the trigger
    when_clause                  VARCHAR2(4000), -- Must evaluate to TRUE for TRIGGER_BODY to execute
    status                       VARCHAR2(8),    -- Indicates whether the trigger is enabled (ENABLED) or disabled (DISABLED)
    description                  VARCHAR2(4000), -- Trigger description; useful for re-creating a trigger creation statement
    action_type                  VARCHAR2(11),   -- Action type of the trigger body: CALL, PL/SQL
    trigger_body                 VARCHAR2(32767),-- Statements executed by the trigger when it fires
    referenced_trigger_indx      VARCHAR2(30),   -- Owner of the referenced trigger
    rename_rec                   gt_rename_rec   -- record holding names for renaming
  );    

  TYPE gt_trigger_arr IS TABLE OF gt_trigger_rec INDEX BY PLS_INTEGER;
  
  
  TYPE gt_policy_rec IS RECORD(
    object_owner                 VARCHAR2(30),  -- Owner of the synonym, table, or view
    object_name                  VARCHAR2(30),  -- Name of the synonym, table, or view
    policy_group                 VARCHAR2(30),  -- Name of the policy group
    policy_name                  VARCHAR2(30),  -- Name of the policy
    pf_owner                     VARCHAR2(30),  -- Owner of the policy function
    package                      VARCHAR2(30),  -- Name of the package containing the policy function
    function                     VARCHAR2(30),  -- Name of the policy function
    sel                          VARCHAR2(3),   -- Indicates whether the policy is applied to queries on the object (YES) or not (NO)
    ins                          VARCHAR2(3),   -- Indicates whether the policy is applied to INSERT statements on the object (YES) or not (NO)
    upd                          VARCHAR2(3),   -- Indicates whether the policy is applied to UPDATE statements on the object (YES) or not (NO)
    del                          VARCHAR2(3),   -- Indicates whether the policy is applied to DELETE statements on the object (YES) or not (NO)
    idx                          VARCHAR2(3),   -- Indicates whether the policy is enforced for index maintenance on the object (YES) or not (NO)
    chk_option                   VARCHAR2(3),   -- Indicates whether the check option is enforced for the policy (YES) or not (NO)
    enable                       VARCHAR2(3),   -- Indicates whether the policy is enabled (YES) or disabled (NO)
    static_policy                VARCHAR2(3),   -- Indicates whether the policy is static (YES) or not (NO)
    policy_type                  VARCHAR2(24),  -- Policy type: STATIC, SHARED_STATIC, CONTEXT_SENSITIVE, SHARED_CONTEXT_SENSITIVE, DYNAMIC
    long_predicate               VARCHAR2(3),   -- Indicates whether the policy function can return a maximum of 32 KB of predicate (YES) or not (NO). If NO, the default maximum predicate size is 4000 bytes.
    sec_rel_col_arr              arrays.gt_str_arr, -- Name of the security relevant column
    column_option                BINARY_INTEGER     -- Option of the security relevant column: NONE  ALL_ROWS 
  );
  
  TYPE gt_policy_arr IS TABLE OF gt_policy_rec INDEX BY PLS_INTEGER;

 
  TYPE gt_table_rec      IS RECORD(
    owner                        VARCHAR2(30),       -- Owner of the table
    table_name                   VARCHAR2(30),       -- Name of the table
    tablespace_name              VARCHAR2(30),       -- Name of the tablespace containing the table; NULL for partitioned, temporary, and index-organized tables
    cluster_name                 VARCHAR2(30),       -- Name of the cluster, if any, to which the table belongs
    cluster_owner                VARCHAR2(30),       -- Owner of the cluster, if any, to which the table belongs
    iot_name                     VARCHAR2(30),       -- Name of the index-organized table, if any, to which the overflow or mapping table entry belongs. If the IOT_TYPE column is not NULL, then this column contains the base table name.  
    iot_type                     VARCHAR2(12),       -- If the table is an index-organized table, then IOT_TYPE is IOT, IOT_OVERFLOW, or IOT_MAPPING. If the table is not an index-organized table, then IOT_TYPE is NULL.
    iot_index_name               VARCHAR2(30),       -- Name of the index (primary key) for IOT table
    iot_index_owner              VARCHAR2(30),       -- Owner of the index (primary key) for IOT table
    iot_pk_column_arr            arrays.gt_str_arr,  -- List of PK key columns
    iot_pk_column_sort_type_arr  arrays.gt_str_arr,  -- List of PK key columns sort order (descend/ascend)
    iot_pct_threshold            NUMBER,             -- Threshold percentage of block space allowed per index entry
    iot_include_column           PLS_INTEGER,        -- Column ID of the last column to be included in index-organized table primary key (non-overflow) index. This column maps to the COLUMN_ID column of the *_TAB_COLUMNS view.
    iot_prefix_length            PLS_INTEGER,        -- Number of columns in the prefix of the compression key
    iot_key_compression          VARCHAR2(8),        -- Indicates whether index compression is enabled (ENABLED) or not (DISABLED)
    overflow_table_name          VARCHAR2(30),       -- Name of overflow table
    overflow_tablespace          VARCHAR2(30),       -- Tablespace of overflow tablespace name
    overflow_physical_attr_rec   gt_physical_attr_rec, -- physical attributes
    overflow_logging             VARCHAR2(7),        -- Indicates whether or not changes to the table are logged; NULL for partitioned tables: YES NO
    mapping_table                VARCHAR2(1),        -- Y - yes, N - no
    partitioned                  VARCHAR2(3),        -- Indicates whether the table is partitioned (YES) or not (NO)
    partitioning_type            VARCHAR2(9),        -- Type of the partitioning method:
                                                     --     RANGE, HASH, SYSTEM, LIST, REFERENCE
    subpartitioning_type         VARCHAR2(7),        -- Type of the composite partitioning method:
                                                     --     NONE, RANGE, HASH, SYSTEM, LIST
    part_key_column_arr          arrays.gt_str_arr,  -- List of partition key columns
    subpart_key_column_arr       arrays.gt_str_arr,  -- List of subpartition key columns
    subpartition_template_arr    gt_subpartition_template_arr, -- List of subpartition templates       
    ref_ptn_constraint_name      VARCHAR2(30),       -- Name of the partitioning referential constraint for reference-partitioned tables
    interval                     VARCHAR2(1000),     -- String of the interval value
    temporary                    VARCHAR2(1),        -- Indicates whether the table is temporary (Y) or not (N)
    duration                     VARCHAR2(15),       -- Indicates the duration of a temporary table:
                                                     --   SYS$SESSION - Rows are preserved for the duration of the session
                                                     --   SYS$TRANSACTION - Rows are deleted after COMMIT
                                                     --   Null - Permanent table
    secondary                    VARCHAR2(1),        -- Indicates whether the table is a secondary object created by the ODCIIndexCreate method of the Oracle Data Cartridge (Y) or not (N)
    nested                       VARCHAR2(3),        -- Indicates whether the table is a nested table (YES) or not (NO)
    object_id_type               VARCHAR2(16),       -- Indicates whether the object ID (OID) is USER-DEFINED or SYSTEM GENERATED
    table_type_owner             VARCHAR2(30),       -- If an object table, owner of the type from which the table is created
    table_type                   VARCHAR2(30),       -- If an object table, type of the table
    row_movement                 VARCHAR2(8),        -- Indicates whether partitioned row movement is enabled (ENABLED) or disabled (DISABLED)
    dependencies                 VARCHAR2(8),        -- Indicates whether row-level dependency tracking is enabled (ENABLED) or disabled (DISABLED)
    physical_attr_rec            gt_physical_attr_rec, -- physical attributes
    compression_rec              gt_compression_rec, -- compression
    parallel_rec                 gt_parallel_rec,    -- parallel degree 
    logging                      VARCHAR2(7),        -- Indicates whether or not changes to the table are logged; NULL for partitioned tables: YES NO
    cache                        VARCHAR2(5),        -- Indicates whether the table is to be cached in the buffer cache (Y) or not (N)
    monitoring                   VARCHAR2(3),        -- Indicates whether the table has the MONITORING attribute set (YES) or not (NO)
    read_only                    VARCHAR2(3),        -- Indicates whether the table IS READ-ONLY (YES) or not (NO)
    result_cache                 VARCHAR2(7),        -- Result cache mode annotation for the table:
                                                     --   DEFAULT - Table has not been annotated, FORCE, MANUAL
    rename_rec                   gt_rename_rec,      -- record holding names for renaming
    column_arr                   gt_column_arr,      -- array of columns
    column_indx_arr              arrays.gt_int_indx, -- index by column name
    column_qualified_indx_arr    arrays.gt_int_indx, -- index by qualified column name
    constraint_arr               gt_constraint_arr,  -- array of constraints
    constraint_indx_arr          arrays.gt_int_indx, -- index by constraint name
    log_group_arr                gt_log_group_arr,   -- array of log groups
    log_group_indx_arr           arrays.gt_int_indx, -- index by log group name
    index_arr                    gt_index_arr,       -- array if indexes     
    index_indx_arr               arrays.gt_int_indx, -- index by "index_owner"."index_name"
    lob_arr                      gt_lob_arr,         -- arrays of lob columns indexed by LOB column index
    lob_indx_arr                 arrays.gt_int_indx, -- arrays of lob columns indexed by LOB column index
    xml_col_arr                  gt_xml_col_arr,     -- arrays of xml columns indexed by XML column index
    varray_arr                   gt_varray_arr,      -- arrays of varray columns indexed by VARRAY column indx
    partition_arr                gt_partition_arr,   -- array of partitions
    partition_indx_arr           arrays.gt_int_indx, -- index by partition name
    subpartition_arr             gt_partition_arr,   -- array of subpartitions
    subpartition_indx_arr        arrays.gt_int_indx, -- index by subpartition name
    ref_constraint_arr           gt_constraint_arr,  -- array of foreign keys referencing on the table from other tables
    ref_constraint_indx_arr      arrays.gt_int_indx, -- index by foreign key name
    join_index_arr               gt_index_arr,       -- array of join indexes on other tables joined with given table
    join_index_indx_arr          arrays.gt_int_indx, -- index by join indexes
    is_table_empty               BOOLEAN,            -- indicates is table empty or not
    privilege_arr                gt_privilege_arr,   -- array of table privileges
    trigger_arr                  gt_trigger_arr,     -- array of tables triggers
    trigger_indx_arr             arrays.gt_int_indx, -- index by triggers
    policy_arr                   gt_policy_arr,      -- array of tables policies
    tablespace_block_size_indx   arrays.gt_num_indx  -- index of block_size of tablespaces
  );

  TYPE gt_sequence_rec    IS RECORD(
    owner                        VARCHAR2(30),       -- Owner of the sequence
    sequence_name                VARCHAR2(30),       -- Name of the sequence
    min_value                    NUMBER,             -- Minimum value of the sequence
    max_value                    NUMBER,             -- Maximum value of the sequence
    increment_by                 NUMBER,             -- Value by which sequence is incremented
    cycle_flag                   VARCHAR2(1),        -- Indicates whether the sequence wraps around on reaching the limit (Y) or not (N)
    order_flag                   VARCHAR2(1),        -- Indicates whether sequence numbers are generated in order (Y) or not (N)
    cache_size                   NUMBER,             -- Number of sequence numbers to cache
    last_number                  NUMBER,             -- Last sequence number written to disk. If a sequence uses caching, the number written to disk is the last number placed in the sequence cache. This number is likely to be greater than the last sequence number that was used.
    rename_rec                   gt_rename_rec,      -- record holding names for renaming
    privilege_arr                gt_privilege_arr    -- array of table privileges
  );

  g_params                       cort_params_pkg.gt_params_rec;


  -- output debug text
  PROCEDURE debug(
    in_text  IN CLOB
  );

  -- format begin/end message
  FUNCTION format_message(
    in_template     IN VARCHAR2,
    in_object_type  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_object_name  IN VARCHAR2
  )
  RETURN VARCHAR2;

  -- raise application error wrapper
  PROCEDURE raise_error(
    in_msg  IN VARCHAR,
    in_code IN NUMBER DEFAULT -20000
  );
  
  -- executes DDL command
  PROCEDURE execute_immediate(
    in_sql   IN CLOB,
    in_echo  IN BOOLEAN DEFAULT TRUE,
    in_test  IN BOOLEAN DEFAULT FALSE
  );

  -- Returns TRUE if table exists otherwise returns FALSE
  FUNCTION object_exists(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2
  )
  RETURN BOOLEAN;

  -- Drops object if it exists
  PROCEDURE exec_drop_object(
    in_object_type  IN VARCHAR2,
    in_object_name  IN VARCHAR2,
    in_object_owner IN VARCHAR2,
    in_echo         IN BOOLEAN DEFAULT TRUE,
    in_test         IN BOOLEAN DEFAULT FALSE
  );

  -- wrapper for table
  PROCEDURE exec_drop_table(
    in_table_name   IN VARCHAR2,
    in_owner        IN VARCHAR2,
    in_echo         IN BOOLEAN DEFAULT TRUE,
    in_test         IN BOOLEAN DEFAULT FALSE
  );

  -- returns error stack
  FUNCTION get_error_stack 
  RETURN VARCHAR2;

  -- return column indx in table column_arr by name
  FUNCTION get_column_indx(
    in_table_rec   IN cort_exec_pkg.gt_table_rec,
    in_column_name IN VARCHAR2
  )
  RETURN PLS_INTEGER;

  PROCEDURE read_table(
    in_table_name      IN VARCHAR2,
    in_owner           IN VARCHAR2,
    out_table_rec      OUT NOCOPY gt_table_rec
  );
  
  PROCEDURE read_index(
    in_index_name  IN VARCHAR2,
    in_owner       IN VARCHAR2,
    out_index_rec  OUT NOCOPY gt_index_rec
  );

  PROCEDURE read_table_indexes(
    io_table_rec IN OUT NOCOPY gt_table_rec
  );

  -- read table properties and all attributes/dependant objects
  PROCEDURE read_table_cascade(
    in_table_name      IN VARCHAR2,
    in_owner           IN VARCHAR2,
    out_table_rec      OUT NOCOPY gt_table_rec
  );

  -- create or replace table
  PROCEDURE create_or_replace_table(
    in_sid            IN VARCHAR2,
    in_schema_name    IN VARCHAR2,
    in_table_name     IN VARCHAR2,
    in_owner          IN VARCHAR2,
    in_sql            IN CLOB
  );

  -- create temp table as copy of given table to create indexes
  PROCEDURE create_index_temp_table(
    in_sid            IN VARCHAR2,
    in_table_name     IN VARCHAR2,
    in_owner          IN VARCHAR2
  );
  
  -- create temp copy of index on temp table
  PROCEDURE create_or_replace_index(
    in_sid            IN VARCHAR2,
    in_schema_name    IN VARCHAR2,
    in_index_name     IN VARCHAR2,
    in_owner          IN VARCHAR2,
    in_sql            IN CLOB
  );

  -- indexes recreation on given table/cluster 
  PROCEDURE create_or_replace_indexes(
    in_sid            IN VARCHAR2,
    in_table_name     IN VARCHAR2,
    in_owner          IN VARCHAR2
  );

  -- Create or replace sequence
  PROCEDURE create_or_replace_sequence(
    in_sid            IN VARCHAR2,
    in_schema_name    IN VARCHAR2,
    in_sequence_name  IN VARCHAR2,
    in_owner          IN VARCHAR2,
    in_sql            IN CLOB  
  );

  -- Public: create or replace object
  PROCEDURE create_or_replace(
    in_sid           IN VARCHAR2,
    in_job_rec       IN cort_jobs%ROWTYPE,
    in_params_rec    IN cort_params_pkg.gt_params_rec
  ); 

  -- Add metadata of creating recreatable object
  PROCEDURE before_create_or_replace(
    in_sid           IN VARCHAR2,
    in_job_rec       IN cort_jobs%ROWTYPE,
    in_params_rec    IN cort_params_pkg.gt_params_rec
  );

  -- Rollback the latest change for given object
  PROCEDURE revert_change(
    in_metadata_rec   IN cort_objects%ROWTYPE,
    in_params_rec     IN cort_params_pkg.gt_params_rec 
  );
  
END cort_exec_pkg;
/