CREATE OR REPLACE PACKAGE cort_options_pkg 
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
  Description: Package for conditional compilation constants     
  ----------------------------------------------------------------------------------------------------------------------     
  Release | Author(s)         | Comments
  ----------------------------------------------------------------------------------------------------------------------  
  14.02   | Rustam Kafarov    | To support CC for single schema/entire database modes or for explain plan option
  ----------------------------------------------------------------------------------------------------------------------  
*/

  gc_single_schema constant boolean := &is_single_schema;
  gc_explain_plan  constant boolean := &is_explain_plan;
  
END cort_options_pkg;
/