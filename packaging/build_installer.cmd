@REM
@REM The MIT License (MIT)
@REM 
@REM Copyright (c) 2009-2015 FoundationDB, LLC
@REM 
@REM Permission is hereby granted, free of charge, to any person obtaining a copy
@REM of this software and associated documentation files (the "Software"), to deal
@REM in the Software without restriction, including without limitation the rights
@REM to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
@REM copies of the Software, and to permit persons to whom the Software is
@REM furnished to do so, subject to the following conditions:
@REM 
@REM The above copyright notice and this permission notice shall be included in
@REM all copies or substantial portions of the Software.
@REM 
@REM THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
@REM IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
@REM FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
@REM AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
@REM LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
@REM OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
@REM THE SOFTWARE.
@REM

@ECHO OFF

SETLOCAL

IF "%1"=="" (
  SET RELEASE=0
) ELSE IF "%1"=="-r" (
  SET RELEASE=1
) ELSE (
  ECHO Unexpected argument: %1
  EXIT /B 1
)


REM Program is named hd2u in msys-git 1.9 and dos2unix in version prior

WHERE hd2u >NUL 2>&1
IF "%ERRORLEVEL%"=="0" (
  SET DOS2UNIX=hd2u
)
WHERE dos2unix >NUL 2>&1
IF "%ERRORLEVEL%"=="0" (
  SET DOS2UNIX=dos2unix
)
IF "%DOS2UNIX%"=="" (
    ECHO No hd2u or dos2unix found
    EXIT /B 1
)


SET EXE_DIR=%~dp0\exe
SET TOP_DIR=%EXE_DIR%\..\..
CD %TOP_DIR%

FOR /F "usebackq" %%v IN (`powershell -Command "& {[xml]$p=Get-Content pom.xml ; $p.project.version}"`) DO SET LAYER_MVN_VERSION=%%v
FOR /F "usebackq" %%v IN (`git rev-parse --short HEAD`) DO SET GIT_HASH=%%v
SET LAYER_VERSION=%LAYER_MVN_VERSION:-SNAPSHOT=%
SET VERSION_TEXT=%LAYER_MVN_VERSION%.%RELEASE%-%GIT_HASH%
SET INSTALLER=fdb-sql-layer-%LAYER_VERSION%-%RELEASE%

IF NOT DEFINED CERT_FILE SET CERT_FILE=%EXE_DIR%\testcert\testcert.pfx
IF NOT DEFINED CERT_PASSWORD SET CERT_PASSWORD=test

ECHO "Building FoundationDB SQL Layer %LAYER_VERSION% Release %RELEASE%"

call mvn clean package -U -D"fdbsql.release=%RELEASE%" -D"skipTests=true"
IF ERRORLEVEL 1 GOTO EOF

IF NOT DEFINED TOOLS_LOC SET TOOLS_LOC="git@github.com:FoundationDB/sql-layer-client-tools.git"
IF NOT DEFINED TOOLS_REF SET TOOLS_REF="master"

CD fdb-sql-layer-core/target
git clone %TOOLS_LOC% client-tools
IF ERRORLEVEL 1 GOTO EOF
CD client-tools
git checkout -b scratch %TOOLS_REF%
IF ERRORLEVEL 1 GOTO EOF
call mvn clean package -U -D"fdbsql.release=%RELEASE%" -D"skipTests=true"
IF ERRORLEVEL 1 GOTO EOF
DEL target\*-sources.jar
CD ..
CD ..
CD ..

REM Common files
MD target
MD target\isstage

XCOPY /E %EXE_DIR% target\isstage
ECHO -tests.jar > target\xclude
ECHO -sources.jar >> target\xclude

REM SQL Layer component files
MD target\isstage\layer
MOVE target\isstage\conf target\isstage\layer
MD target\isstage\layer\bin
MD target\isstage\layer\lib
MD target\isstage\layer\lib\plugins
MD target\isstage\layer\lib\server
MD target\isstage\layer\lib\fdb-sql-layer-routinefw
MD target\isstage\layer\procrun

COPY %TOP_DIR%\LICENSE.txt target\isstage\layer\LICENSE-SQL_LAYER.txt
COPY %EXE_DIR%\..\conf\* target\isstage\layer\conf
DEL target\isstage\layer\conf\jvm.options
DEL target\isstage\layer\conf\sql-layer.policy
COPY %EXE_DIR%\conf\sql-layer.policy target\isstage\layer\conf\sql-layer.policy
COPY bin\*.cmd target\isstage\layer\bin
%DOS2UNIX% --verbose --u2d target\isstage\layer\conf\* target\isstage\layer\*.txt target\isstage\layer\bin\*.cmd
FOR %%f in (target\isstage\layer\conf\*) DO MOVE "%%f" "%%f.new"
XCOPY fdb-sql-layer-core\target\fdb-sql-layer-core*.jar target\isstage\layer\lib /EXCLUDE:target\xclude
XCOPY fdb-sql-layer-core\target\dependency\* target\isstage\layer\lib\server
XCOPY plugins\* target\isstage\layer\lib\plugins /E
XCOPY fdb-sql-layer-routinefw\target\fdb-sql-layer-routinefw*.jar target\isstage\layer\lib\fdb-sql-layer-routinefw\ /EXCLUDE:target\xclude

CD target\isstage\layer
curl -o procrun.zip -L http://archive.apache.org/dist/commons/daemon/binaries/windows/commons-daemon-1.0.15-bin-windows.zip

IF ERRORLEVEL 1 GOTO EOF
7z x -oprocrun procrun.zip
IF ERRORLEVEL 1 GOTO EOF
CD procrun
mt /nologo -manifest ..\..\prunsrv.manifest -outputresource:prunsrv.exe;1
IF ERRORLEVEL 1 GOTO EOF
mt /nologo -manifest ..\..\prunmgr.manifest -outputresource:prunmgr.exe;1
IF ERRORLEVEL 1 GOTO EOF
CD amd64
mt /nologo -manifest ..\..\..\prunsrv.manifest -outputresource:prunsrv.exe;1
IF ERRORLEVEL 1 GOTO EOF
CD ..\ia64
mt /nologo -manifest ..\..\..\prunsrv.manifest -outputresource:prunsrv.exe;1
IF ERRORLEVEL 1 GOTO EOF

cd %TOP_DIR%

REM Client Tools component files
MD target\isstage\client
MD target\isstage\client\bin
MD target\isstage\client\lib
MD target\isstage\client\lib\client

COPY fdb-sql-layer-core\target\client-tools\bin\*.cmd target\isstage\client\bin
XCOPY fdb-sql-layer-core\target\client-tools\target\fdb-sql-layer-client-tools-*.jar target\isstage\client\lib /EXCLUDE:target\xclude
COPY fdb-sql-layer-core\target\client-tools\target\dependency\* target\isstage\client\lib\client
COPY fdb-sql-layer-core\target\client-tools\LICENSE.txt target\isstage\client\LICENSE-SQL_LAYER_CLIENT_TOOLS.txt

REM Build the installer
CD target\isstage

iscc /S"standard=signtool sign /f $q%CERT_FILE%$q  /p $q%CERT_PASSWORD%$q /t http://tsa.starfieldtech.com/ $f" ^
     /O.. /F"%INSTALLER%" /dVERSION=%LAYER_VERSION% /dVERSIONTEXT=%VERSION_TEXT% /dRELEASE=%RELEASE% fdb-sql-layer.iss
IF ERRORLEVEL 1 GOTO EOF

CD ..\..

:EOF
ENDLOCAL
