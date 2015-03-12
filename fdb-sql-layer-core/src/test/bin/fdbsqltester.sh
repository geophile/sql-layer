#!/bin/bash
TARGET=$(ls -d $(dirname $0)/../../../target)
BASEJAR=$(ls ${TARGET}/fdb-sql-layer-*.*.*-SNAPSHOT.jar)
java -cp "${BASEJAR}:${BASEJAR%.jar}-tests.jar:${TARGET}/dependency/*" com.foundationdb.sql.test.Tester "$@"
