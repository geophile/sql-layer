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
package com.foundationdb.server.test.mt;

import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.mt.util.OnlineCreateTableAsBase;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import java.util.Arrays;


/** Interleaved DML during an online create table as select query */
public class OnlineCreateTableAsMT extends OnlineCreateTableAsBase {

    @Before
    public void createAndLoad() {
        CREATE_QUERY = " CREATE TABLE " + TO_TABLE + " AS SELECT * FROM " + FROM_TABLE + " WITH DATA ";
        ftID = createTable(SCHEMA, FROM_TABLE, "id INT NOT NULL PRIMARY KEY, x INT");

        fromTableRowType = SchemaCache.globalSchema(ais()).tableRowType(ftID);

        writeRows(row(ftID, -1, 10),
                row(ftID, 2, 20),
                row(ftID, 3, 30),
                row(ftID, 4, 40));

        fromGroupRows = runPlanTxn(groupScanCreator(ftID));
        columnNames = Arrays.asList("id", "x");
        DataTypeDescriptor d = new DataTypeDescriptor(TypeId.INTEGER_ID, false);
        fromDescriptors = Arrays.asList(d, d);
        server = new TestSession();
    }

    //
    // I/U/D pre-to-post METADATA
    //
    @Test
    public void insertPreToPostMetadata() {
        Row newRow = testRow(fromTableRowType, 5, 50);
        otherGroupRows = getGroupExpected();
        dmlPreToPostMetadata(insertCreator(ftID, newRow), getGroupExpected(), true, fromDescriptors, columnNames, server, true);
    }

    @Test
    public void updatePreToPostMetadata() {
        Row oldRow = testRow(fromTableRowType, 2, 20);
        Row newRow = testRow(fromTableRowType, 2, 21);
        otherGroupRows = getGroupExpected();
        dmlPreToPostMetadata(updateCreator(ftID, oldRow, newRow), getGroupExpected(), true, fromDescriptors, columnNames, server, true);
    }

    @Test
    public void deletePreToPostMetadata() {
        Row oldRow = fromGroupRows.get(0);
        otherGroupRows = getGroupExpected();
        dmlPreToPostMetadata(deleteCreator(ftID, oldRow), getGroupExpected(), true, fromDescriptors, columnNames, server, true);
    }

    //
    // I/U/D post METADATA to pre FINAL
    //
    @Test
    public void insertPostMetaToPreFinal() {
        Row newRow = testRow(fromTableRowType, 5, 50);
        otherGroupRows = combine(fromGroupRows, newRow);
        dmlPostMetaToPreFinal(insertCreator(ftID, newRow), combine(fromGroupRows, newRow), true, true, fromDescriptors, columnNames, server, true);
    }

    @Ignore
    public void updatePostMetaToPreFinal() {
        Row oldRow = testRow(fromTableRowType, 2, 20);
        Row newRow = testRow(fromTableRowType, 2, 21);
        otherGroupRows = replace(fromGroupRows, 1, newRow);
        dmlPostMetaToPreFinal(updateCreator(ftID, oldRow, newRow), replace(fromGroupRows, 1, newRow), true, true, fromDescriptors, columnNames, server, true);
    }

    @Ignore
    public void deletePostMetaToPreFinal() {
        Row oldRow = fromGroupRows.get(0);
        otherGroupRows = fromGroupRows.subList(1, fromGroupRows.size());
        dmlPostMetaToPreFinal(deleteCreator(ftID, oldRow), fromGroupRows.subList(1, fromGroupRows.size()), true, true, fromDescriptors, columnNames, server, true);
    }

    //
    // I/U/D pre-to-post FINAL
    //
    @Test
    public void insertPreToPostFinal() {
        Row newRow = testRow(fromTableRowType, 5, 50);
        otherGroupRows = getGroupExpected();
        dmlPreToPostFinal(insertCreator(ftID, newRow), getGroupExpected(), true, fromDescriptors, columnNames, server, true);
    }

    @Test
    public void updatePreToPostFinal() {
        Row oldRow = testRow(fromTableRowType, 2, 20);
        Row newRow = testRow(fromTableRowType, 2, 21);
        otherGroupRows = getGroupExpected();
        dmlPreToPostFinal(updateCreator(ftID, oldRow, newRow), getGroupExpected(), true, fromDescriptors, columnNames, server, true);
    }

    @Test
    public void deletePreToPostFinal() {
        Row oldRow = fromGroupRows.get(0);
        otherGroupRows = getGroupExpected();
        dmlPreToPostFinal(deleteCreator(ftID, oldRow), getGroupExpected(), true, fromDescriptors, columnNames, server, true);
    }
}