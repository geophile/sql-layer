#!/bin/bash
set -x
DRIVERJAR=/usr/share/java/postgresql.jar:/usr/share/java/mysql-connector-java.jar
TARGET=$(ls -d $(dirname $0)/../../../target)
BASEJAR=$(ls ${TARGET}/fdb-sql-layer-*.*.*-SNAPSHOT.jar)
java -cp "target/test-classes:${BASEJAR}:${BASEJAR%.jar}-tests.jar:${TARGET}/dependency/*:$DRIVERJAR" com.foundationdb.sql.test.SQLClient "$@"
