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
  Description: Schema standard installation script (without SQLPlus Extensions)
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.01   | Rustam Kafarov    | Main functionality
  14.01.1 | Rustam Kafarov    | Removed dependencies on SQLPlus Extensions 
  14.02   | Rustam Kafarov    | Added cort_builds, cort_config_params, plan_table tables. Made script 100% rerunnable (log data will be lost)
  ----------------------------------------------------------------------------------------------------------------------  
*/


PROMPT TABLE CORT_STAT

@DROP TABLE cort_stat

BEGIN
  dbms_stats.create_stat_table(
    ownname          => USER,
    stattab          => 'CORT_STAT',
    global_temporary => TRUE
  );
END;
/

GRANT SELECT, INSERT, UPDATE, DELETE ON cort_stat TO PUBLIC;

PROMPT TABLE CORT_JOBS

@DROP TABLE cort_jobs

CREATE TABLE cort_jobs(
  sid             VARCHAR2(24)    NOT NULL,
  action          VARCHAR2(30)    NOT NULL,
  status          VARCHAR2(30)    NOT NULL CHECK(status IN ('RUNNING','FAILED','COMPLETED')),
  object_type     VARCHAR2(30)    NOT NULL,
  object_owner    VARCHAR2(30)    NOT NULL,
  object_name     VARCHAR2(30)    NOT NULL,
  job_owner       VARCHAR2(30)    NOT NULL,
  job_name        VARCHAR2(30)    NOT NULL,
  job_time        TIMESTAMP,
  job_sid         VARCHAR2(24),
  sql_text        CLOB,
  schema_name     VARCHAR2(30),
  session_params  XMLTYPE,
  build           VARCHAR2(30),
  output          CLOB,
  error_code      NUMBER,
  error_message   VARCHAR2(4000),
  error_backtrace VARCHAR2(4000),
  cort_stack      VARCHAR2(4000),
  CONSTRAINT cort_jobs_sid_pk PRIMARY KEY (sid)
);

@DROP INDEX cort_jobs_object_name_uk

CREATE UNIQUE INDEX cort_jobs_object_name_uk ON cort_jobs(object_name, object_owner, DECODE(status,'RUNNING','0',sid));
  

PROMPT TABLE CORT_JOB_LOG

@DROP TABLE CORT_JOB_LOG

CREATE TABLE cort_job_log(
  job_log_time    TIMESTAMP(9)     NOT NULL,
  sid             VARCHAR2(30)     NOT NULL,
  action          VARCHAR2(30),
  status          VARCHAR2(30),
  object_type     VARCHAR2(30),
  object_owner    VARCHAR2(30),
  object_name     VARCHAR2(30),
  job_owner       VARCHAR2(30),
  job_name        VARCHAR2(30),
  job_time        TIMESTAMP,
  job_sid         VARCHAR2(24),
  sql_text        CLOB,
  schema_name     VARCHAR2(30),
  session_params  XMLTYPE,
  build           VARCHAR2(30),
  session_id      NUMBER,
  username        VARCHAR2(30),
  osuser          VARCHAR2(30),
  machine         VARCHAR2(64),
  terminal        VARCHAR2(16),
  module          VARCHAR2(48),
  output          CLOB,
  error_code      NUMBER,
  error_message   VARCHAR2(4000),
  error_backtrace VARCHAR2(4000),
  cort_stack      VARCHAR2(4000),
  CONSTRAINT cort_job_log_pk PRIMARY KEY(sid, job_log_time) USING INDEX COMPRESS 
);


PROMPT TABLE CORT_LOG

@DROP TABLE cort_log

CREATE TABLE cort_log(
  log_time       TIMESTAMP(9)    NOT NULL,
  sid            VARCHAR2(30)    NOT NULL,
  job_time       TIMESTAMP,
  action         VARCHAR2(30),
  object_type    VARCHAR2(30),
  object_owner   VARCHAR2(30),
  object_name    VARCHAR2(30),
  log_type       VARCHAR2(100),
  text           CLOB,
  package_name   VARCHAR2(30),
  line_number    NUMBER,
  execution_time NUMBER(18,9),
  error_msg      VARCHAR2(512),
  CONSTRAINT cort_log_pk PRIMARY KEY(sid, log_time) USING INDEX COMPRESS 
);


PROMPT TABLE CORT_OBJECTS

@DROP TABLE cort_objects

CREATE TABLE cort_objects(
  object_owner                VARCHAR2(30)  NOT NULL,
  object_name                 VARCHAR2(30)  NOT NULL,
  object_type                 VARCHAR2(30)  NOT NULL,
  exec_time                   TIMESTAMP(9)  NOT NULL,
  sid                         VARCHAR2(30)  NOT NULL,
  sql_text                    CLOB,
  last_ddl_time               DATE,
  application                 VARCHAR2(20),
  release                     VARCHAR2(20),
  build                       VARCHAR2(20),
  change_type                 NUMBER(2), 
  forward_ddl                 XMLTYPE,
  revert_ddl                  XMLTYPE,
  revert_name                 VARCHAR2(30),
  CONSTRAINT cort_objects_pk PRIMARY KEY (object_owner, object_name, object_type, exec_time)
    USING INDEX COMPRESS
);


PROMPT TABLE CORT_BUILDS

@DROP TABLE cort_builds

CREATE TABLE cort_builds(
  application                    VARCHAR2(20)  NOT NULL,
  build                          VARCHAR2(20)  NOT NULL,
  status                         VARCHAR2(10)  NOT NULL,
  release                        VARCHAR2(20),
  start_time                     DATE,
  end_time                       DATE,
  CONSTRAINT cort_builds_pk
    PRIMARY KEY (build)
);

CREATE UNIQUE INDEX cort_builds_uk ON cort_builds(application, DECODE(STATUS, 'ACTIVE', '0', 'STALE', '0', build));


PROMPT TABLE CORT_BUILD_SESSIONS

@DROP TABLE cort_build_sessions

CREATE TABLE cort_build_sessions(
  build                          VARCHAR2(20)  NOT NULL,
  session_id                     VARCHAR2(30)  NOT NULL,
  start_time                     DATE,
  end_time                       DATE,
  CONSTRAINT cort_builds_sessions_pk
    PRIMARY KEY (build, session_id),
  CONSTRAINT cort_builds_sessions_fk
    FOREIGN KEY (build) REFERENCES cort_builds(build)
);

PROMPT TABLE CORT_PARAMS

@DROP TABLE cort_params

CREATE TABLE cort_params (
  param_name      VARCHAR2(30)   NOT NULL,
  param_type      VARCHAR2(30)   NOT NULL,
  default_value   VARCHAR2(1000),  
  CONSTRAINT cort_params_pk PRIMARY KEY (param_name),
  CONSTRAINT cort_params_name_chk CHECK(param_name = UPPER(param_name)),
  CONSTRAINT cort_params_type_chk CHECK(param_type IN ('BOOLEAN','NUMBER','VARCHAR2'))
);


PROMPT TABLE CORT_HINTS

@DROP TABLE cort_hints

CREATE TABLE cort_hints (
  hint            VARCHAR2(30)   NOT NULL,
  param_name      VARCHAR2(30)   NOT NULL,
  param_value     VARCHAR2(1000),  
  expression_flag VARCHAR2(1)    NOT NULL,
  CONSTRAINT cort_hints_pk PRIMARY KEY (hint)
);


PROMPT TABLE CORT_CONFIG_PARAMS

@DROP TABLE cort_config_params

CREATE TABLE cort_config_params (
  param_name      VARCHAR2(30)   NOT NULL,
  param_value     VARCHAR2(1000),
  CONSTRAINT cort_config_params_pk PRIMARY KEY (param_name),
  CONSTRAINT cort_config_params_name_chk CHECK(param_name = UPPER(param_name))
);



PROMPT TABLE CORT_INDEX_TEMP_TABLES

@DROP TABLE cort_index_temp_tables

CREATE TABLE cort_index_temp_tables (
  table_owner       VARCHAR2(30) NOT NULL,
  table_name        VARCHAR2(30) NOT NULL,
  temp_table_name   VARCHAR2(30) NOT NULL,
  sid               VARCHAR2(24) NOT NULL,
  old_table_rec     XMLTYPE      NOT NULL,
  new_table_rec     XMLTYPE      NOT NULL,  
  CONSTRAINT cort_index_temp_tables_pk PRIMARY KEY(table_owner, table_name)
);


PROMPT TABLE PLAN_TABLE

@DROP TABLE PLAN_TABLE

CREATE GLOBAL TEMPORARY TABLE PLAN_TABLE(
  statement_id       VARCHAR2(30),
  plan_id            NUMBER,
  timestamp          DATE,
  remarks            VARCHAR2(4000),
  operation          VARCHAR2(4000),
  options            VARCHAR2(255),
  object_node        VARCHAR2(128),
  object_owner       VARCHAR2(30),
  object_name        VARCHAR2(30),
  object_alias       VARCHAR2(65),
  object_instance    INTEGER,
  object_type        VARCHAR2(30),
  optimizer          VARCHAR2(255),
  search_columns     NUMBER,
  id                 INTEGER,
  parent_id          INTEGER,
  depth              INTEGER,
  position           INTEGER,
  cost               INTEGER,
  cardinality        INTEGER,
  bytes              INTEGER,
  other_tag          VARCHAR2(255),
  partition_start    VARCHAR2(255),
  partition_stop     VARCHAR2(255),
  partition_id       INTEGER,
  other              LONG,
  other_xml          CLOB,
  distribution       VARCHAR2(30),
  cpu_cost           INTEGER,
  io_cost            INTEGER,
  temp_space         INTEGER,
  access_predicates  VARCHAR2(4000),
  filter_predicates  VARCHAR2(4000),
  projection         VARCHAR2(4000),
  time               INTEGER,
  qblock_name        VARCHAR2(30)
)
ON COMMIT PRESERVE ROWS
;

PROMPT CONTEXT cort_&_user

BEGIN
  EXECUTE IMMEDIATE 'CREATE OR REPLACE CONTEXT '||substr('cort_&_user', 1, 30)||' USING cort_trg_pkg';
END;
/  


@@cort_config_params.sql

@@cort_params.sql

@@cort_hints.sql
