@echo off

REM   CORT - Oracle database deployment and continuous integration tool
REM 
REM   Copyright (C) 2013-2014  Softcraft Ltd - Rustam Kafarov
REM 
REM   This program is free software: you can redistribute it and/or modify
REM   it under the terms of the GNU General Public License as published by
REM   the Free Software Foundation, either version 3 of the License, or
REM   (at your option) any later version.
REM 
REM   This program is distributed in the hope that it will be useful,
REM   but WITHOUT ANY WARRANTY; without even the implied warranty of
REM   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
REM   GNU General Public License for more details.
REM 
REM   You should have received a copy of the GNU General Public License
REM   along with this program.  If not, see <http://www.gnu.org/licenses/>.

for /D %%I in (%0%) do (
  set root_path=%%~dpI
)

sqlplus /nolog @"%root_path%install.sql"

echo.
echo.
pause