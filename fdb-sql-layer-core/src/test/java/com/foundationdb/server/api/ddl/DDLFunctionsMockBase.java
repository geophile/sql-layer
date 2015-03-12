/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2009-2015 FoundationDB, LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.foundationdb.server.api.ddl;

import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.View;
import com.foundationdb.ais.util.TableChange;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.NoSuchTableIdException;
import com.foundationdb.server.service.dxl.OnlineDDLMonitor;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.format.DummyStorageFormatRegistry;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.server.types.service.TestTypesRegistry;
import com.foundationdb.server.types.service.TypesRegistry;
import com.foundationdb.sql.server.ServerSession;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;

/**
 * Simple implementation that throws UnsupportedOperation for all methods.
 */
public class DDLFunctionsMockBase implements DDLFunctions {
    @Override
    public void createTable(Session session, Table table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(Session session, Table table, String queryExpression, QueryContext context, ServerSession server) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(Session session, TableName currentName, TableName newName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(Session session, TableName tableName) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public ChangeLevel alterTable(Session session, TableName tableName, Table newDefinition,
                           List<TableChange> columnChanges, List<TableChange> indexChanges, QueryContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterSequence(Session session, TableName sequenceName, Sequence newDefinition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropSchema(Session session, String schemaName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropNonSystemSchemas(Session session) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropGroup(Session session, TableName groupName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AkibanInformationSchema getAIS(Session session) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TypesRegistry getTypesRegistry() {
        return TestTypesRegistry.MCOMPAT;
    }

    @Override
    public TypesTranslator getTypesTranslator() {
        return MTypesTranslator.INSTANCE;
    }

    @Override
    public StorageFormatRegistry getStorageFormatRegistry() {
        return DummyStorageFormatRegistry.create();
    }

    @Override
    public AISCloner getAISCloner() {
        return new AISCloner(getTypesRegistry(), getStorageFormatRegistry());
    }

    @Override
    public TableName getTableName(Session session, int tableId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTableId(Session session, TableName tableName) throws NoSuchTableException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table getTable(Session session, int tableId) throws NoSuchTableIdException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table getTable(Session session, TableName tableName) throws NoSuchTableException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> getActiveGenerations() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createIndexes(Session session, Collection<? extends Index> indexesToAdd) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTableIndexes(Session session, TableName tableName, Collection<String> indexesToDrop) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropGroupIndexes(Session session, TableName groupName, Collection<String> indexesToDrop) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTableStatistics(Session session, TableName tableName, Collection<String> indexesToUpdate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(Session session, View view) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropView(Session session, TableName viewName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createSequence(Session session, Sequence sequence) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropSequence(Session session, TableName sequenceName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createRoutine(Session session, Routine routine, boolean replaceExisting) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropRoutine(Session session, TableName procedureName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createSQLJJar(Session session, SQLJJar sqljJar) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void replaceSQLJJar(Session session, SQLJJar sqljJar) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void dropSQLJJar(Session session, TableName jarName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOnlineDDLMonitor(OnlineDDLMonitor onlineDDLMonitor) {
        throw new UnsupportedOperationException();
    }
}
