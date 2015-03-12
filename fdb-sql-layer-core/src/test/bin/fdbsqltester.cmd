@echo off
setlocal
for %%f in (%~dp0..\..\..\target) do set TARGET=%%~dpnf
for %%f in (%TARGET%\fdb-sql-layer-*.*.*-SNAPSHOT.jar) do set BASEJAR=%%~dpnf
java -cp %BASEJAR%.jar;%BASEJAR%-tests.jar;%TARGET%\dependency\* com.foundationdb.sql.test.Tester %*
