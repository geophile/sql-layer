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
package com.foundationdb.server.test.it.dxl;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.NoSuchRowException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public final class CBasicIT extends ITBase {

    @Test
    public void dropTable() throws InvalidOperationException {
        createTable("testSchema", "customer", "id int not null primary key");
        ddl().dropTable(session(), tableName("testSchema", "customer"));

        AkibanInformationSchema ais = ddl().getAIS(session());
        assertNull("expected no table", ais.getTable("testSchema", "customer"));
        ddl().dropTable(session(), tableName("testSchema", "customer")); // should be no-op; testing it doesn't fail
    }

    @Test
    public void dropGroup() throws InvalidOperationException {
        createTable("test", "t", "id int not null primary key");
        final TableName groupName = ddl().getAIS(session()).getTable("test", "t").getGroup().getName();
        ddl().dropGroup(session(), groupName);

        AkibanInformationSchema ais = ddl().getAIS(session());
        assertNull("expected no table", ais.getTable("test", "t"));
        assertNull("expected no group", ais.getGroup(groupName));

        ddl().dropGroup(session(), groupName);
    }

    /*
     * Found from an actual case in the MTR test suite. Caused by recycled table IDs and undeleted table statuses.
     * Really testing that table statuses get deleted, but about as direct as we can get from this level.
     */
    @Test
    public void dropThenCreateTableIDRecycled() throws InvalidOperationException {
        NewAISBuilder builder = AISBBasedBuilder.create("test", ddl().getTypesTranslator());
        builder.table("t1").autoIncInt("id", 1).pk("id").colString("name", 255);
        ddl().createTable(session(), builder.ais().getTable("test", "t1"));
        final int tidV1 = tableId("test", "t1");

        writeRow(tidV1, 1, "hello world");
        expectRowCount(tidV1, 1);
        ddl().dropTable(session(), tableName(tidV1));

        // Easiest exception trigger was to toggle auto_inc column, failed when trying to update it
        final int tidV2 = createTable("test", "t2", "id int not null primary key, tag char(1), value decimal(10,2)");
        writeRow(tidV2, "1", "a", "49.95");
        expectRowCount(tidV2, 1);
        ddl().dropTable(session(), tableName(tidV2));
    }

    @Test
    public void updateNoChangeToHKey() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");

        expectRowCount(tableId, 0);
        writeRow(tableId, 0, "hello world");
        expectRowCount(tableId, 1);

        expectRows(tableId, row(tableId, 0, "hello world") );

        updateRow(row(tableId, 0, "hello world"), row(tableId, 0, "goodbye cruel world"));
        expectRowCount(tableId, 1);
        expectRows(tableId, row(tableId, 0, "goodbye cruel world") );
    }

    @Test
    public void updateOldOnlyById() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");

        expectRowCount(tableId, 0);
        writeRow(tableId, 0, "hello world");
        expectRowCount(tableId, 1);

        expectRows(tableId, row(tableId, 0, "hello world") );

        updateRow(row(tableId, 0, "hello world"), row(tableId, 1, "goodbye cruel world"));
        expectRowCount(tableId, 1);
        expectRows(tableId, row(tableId, 1, "goodbye cruel world") );
    }

    @Test(expected=NoRowsUpdatedException.class)
    public void updateOldNotById() throws InvalidOperationException {
        final int tableId;
        try {
            tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");

            expectRowCount(tableId, 0);
            writeRow(tableId, 0, "hello world");
            expectRowCount(tableId, 1);

            expectRows(tableId, row(tableId, 0, "hello world") );
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        Row badRow = row(tableId, 1, "goodbye cruel world");
        try {
            Row old = row(tableId, null, "hello world");
            updateRow(old, badRow);
        } catch (NoSuchRowException e) {
            expectRows(tableId, row(tableId, 0, "hello world"));
            throw new NoRowsUpdatedException();
        }
    }

    /**
     * We currently can't differentiate between null and unspecified, so not specifying a field is the same as
     * setting it null. Thus, by providing a truncated definition for both old and new rows, we're essentially
     * nulling some of the row as well as shortening it.
     * @throws InvalidOperationException if there's a failure
     */
    @Test
    public void updateRowPartially() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");

        expectRowCount(tableId, 0);
        writeRow(tableId, 0, "hello world");
        expectRowCount(tableId, 1);

        expectRows(tableId, row(tableId, 0, "hello world") );

        updateRow(row(tableId, 0, null), row(tableId, 1, null));
        expectRowCount(tableId, 1);
        expectRows(tableId, row(tableId, 1, null) );
    }

    @Test
    public void updateChangesHKey() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");

        expectRowCount(tableId, 0);
        writeRow(tableId, 0, "hello world");
        expectRowCount(tableId, 1);

        expectRows(tableId, row(tableId, 0, "hello world") );

        updateRow(row(tableId, 0, "hello world"), row(tableId, 1, "goodbye cruel world"));
        expectRowCount(tableId, 1);
        expectRows(tableId, row(tableId, 1, "goodbye cruel world") );
    }

    @Test
    public void deleteRows() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");

        expectRowCount(tableId, 0);
        writeRow(tableId, 0, "doomed row");
        expectRowCount(tableId, 1);
        writeRow(tableId, 1, "also doomed");
        expectRowCount(tableId, 2);

        expectRows(tableId,
                row(tableId, 0, "doomed row"),
                row(tableId, 1, "also doomed"));

        deleteRow(tableId, 0, "doomed row");
        expectRowCount(tableId, 1);
        expectRows(tableId,
                row(tableId, 1, "also doomed"));

        deleteRow(tableId, 1, "also doomed");
        expectRowCount(tableId, 0);
        expectRows(tableId);
    }

    @Test(expected=NoSuchRowException.class)
    public void deleteRowNotById() throws InvalidOperationException {
        final int tableId;
        try{
            tableId = createTable("theschema", "c", "id int not null primary key, name varchar(32)");

            expectRowCount(tableId, 0);
            writeRow(tableId, 0, "the customer's name");
            expectRowCount(tableId, 1);

            expectRows(tableId, row(tableId, 0, "the customer's name"));
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        try {
            Row deleteAttempt = row(tableId, null, "the customer's name");
            deleteRow(deleteAttempt);
        } catch (NoSuchRowException e) {
            expectRows(tableId, row(tableId, 0, "the customer's name"));
            throw e;
        }
    }

    @Test(expected=NoSuchRowException.class)
    public void deleteMissingRow()  throws InvalidOperationException {
        final int tableId;
        try{
            tableId = createTable("theschema", "c", "id int not null primary key, name varchar(32)");
        } catch (InvalidOperationException e) {
            throw unexpectedException(e);
        }

        try {
            deleteRow(tableId, 0, "this row doesn't exist");
        } catch (NoSuchRowException e) {
            expectRows(tableId);
            throw e;
        }
    }

    @Test
    public void aisGenerationIncrements() throws Exception {
        long firstGen = ddl().getAIS(session()).getGeneration();
        createTable("sch", "c1", "id int not null primary key");
        long secondGen = ddl().getAIS(session()).getGeneration();
        assertTrue(String.format("failed %d > %d", secondGen, firstGen), secondGen > firstGen);
    }

    @Test
    public void truncate() throws InvalidOperationException {
        final int tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");

        expectRowCount(tableId, 0);
        writeRow(tableId, 0, "hello world");
        expectRowCount(tableId, 1);
        dml().truncateTable(session(), tableId);
        expectRowCount(tableId, 0);
    }

    /**
     * bug1002359: Grouped tables, different schemas, same table name, same column name
     */
    @Test
    public void groupedTablesWithSameNameAndColumnNames() {
        createTable("s1", "t1", "id int not null primary key");
        createTable("s2", "t1", "some_id int not null primary key, id int, grouping foreign key(id) references s1.t1(id)");
        createTable("s3", "t1", "some_id int not null primary key, id int, grouping foreign key(id) references s2.t1(some_id)");
        AkibanInformationSchema ais = ddl().getAIS(session());
        Group group = ais.getGroup(new TableName("s1", "t1"));
        assertNotNull("Found group", group);
        List<TableName> tablesInGroup = new ArrayList<>();
        for(Table table : ais.getTables().values()) {
            if(table.getGroup() == group) {
                tablesInGroup.add(table.getName());
            }
        }
        assertEquals("Tables in group", "[s1.t1, s2.t1, s3.t1]", tablesInGroup.toString());
    }

    private static class NoRowsUpdatedException extends RuntimeException {
    }
}
