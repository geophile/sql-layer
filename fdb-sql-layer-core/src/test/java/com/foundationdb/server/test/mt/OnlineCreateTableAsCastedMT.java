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
import com.foundationdb.server.test.mt.util.OperatorCreator;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import java.util.Arrays;


/** Interleaved DML during an online create table as select query */
public class OnlineCreateTableAsCastedMT extends OnlineCreateTableAsBase {

    private String NEW_TABLE = "nt";

    @Before
    public void createAndLoad() {
        CREATE_QUERY = " CREATE TABLE " + NEW_TABLE + " AS SELECT CAST( ABS(id) AS DOUBLE ) , x IS NOT NULL FROM " + FROM_TABLE + " WITH DATA ";
        ftID = createTable(SCHEMA, FROM_TABLE, "id INT NOT NULL PRIMARY KEY, x INT");
        ttID = createTable(SCHEMA, TO_TABLE, "id INT NOT NULL PRIMARY KEY, x BOOLEAN");

        fromTableRowType = SchemaCache.globalSchema(ais()).tableRowType(ftID);
        toTableRowType = SchemaCache.globalSchema(ais()).tableRowType(ttID);

        writeRows(row(ftID, -1, 10),
                row(ftID, 2, 20),
                row(ftID, 3, 30),
                row(ftID, 4, 40),
                row(ttID, 1, true),
                row(ttID, 2, true),
                row(ttID, 3, true),
                row(ttID, 4, true));

        fromGroupRows = runPlanTxn(groupScanCreator(ftID));//runs given plan and returns output row
        toGroupRows = runPlanTxn(groupScanCreator(ttID));
        columnNames = Arrays.asList("id", "x");
        DataTypeDescriptor d = new DataTypeDescriptor(TypeId.INTEGER_ID, false);
        DataTypeDescriptor cd = new DataTypeDescriptor(TypeId.BOOLEAN_ID, false);
        toDescriptors = Arrays.asList(d, cd);
        server = new TestSession();
    }

    @Override
    protected OperatorCreator getOtherCreator() {
        int temp = ais().getTable(SCHEMA, NEW_TABLE).getTableId();
        return groupScanCreator(temp);
    }

    //
    // I/U/D pre-to-post METADATA
    //

    @Test
    public void InsertPreToPostMetadata() {
        Row newRow = testRow(fromTableRowType, 5, 50);
        otherGroupRows = getToExpected();
        dmlPreToPostMetadata(insertCreator(ftID, newRow), getGroupExpected(), true, toDescriptors, columnNames, server, true);
    }

    @Test
    public void UpdatePreToPostMetadata() {
        Row oldRow = testRow(fromTableRowType, 2, 20);
        Row newRow = testRow(fromTableRowType, 2, 21);
        otherGroupRows = getToExpected();
        dmlPreToPostMetadata(updateCreator(ftID, oldRow, newRow), getGroupExpected(), true, toDescriptors, columnNames, server, true);
    }

    @Test
    public void DeletePreToPostMetadata() {
        Row oldRow = fromGroupRows.get(0);
        otherGroupRows = getToExpected();
        dmlPreToPostMetadata(deleteCreator(ftID, oldRow), getGroupExpected(), true, toDescriptors, columnNames, server, true);
    }

    //
    // I/U/D post METADATA to pre FINAL
    //

    @Test
    public void InsertPostMetaToPreFinal() {
        Row newRow = testRow(fromTableRowType, 5, 50);
        Row newCastRow = testRow(toTableRowType, 5, true);
        otherGroupRows = combine(getToExpected(), newCastRow);
        dmlPostMetaToPreFinal(insertCreator(ftID, newRow), combine(fromGroupRows, newRow), true, true, toDescriptors, columnNames, server, true);
    }

    @Ignore
    public void UpdatePostMetaToPreFinal() {
        Row oldRow = testRow(fromTableRowType, 2, 20);
        Row newRow = testRow(fromTableRowType, 2, 21);
        otherGroupRows = getToExpected();
        dmlPostMetaToPreFinal(updateCreator(ftID, oldRow, newRow), replace(fromGroupRows, 1, newRow), true, true, toDescriptors, columnNames, server, true);
    }

    @Ignore
    public void DeletePostMetaToPreFinal() {
        Row oldRow = fromGroupRows.get(0);
        otherGroupRows = toGroupRows.subList(1, toGroupRows.size());
        dmlPostMetaToPreFinal(deleteCreator(ftID, oldRow), fromGroupRows.subList(1, fromGroupRows.size()), true, true, toDescriptors, columnNames, server, true);
    }
    //
    // I/U/D pre-to-post FINAL
    //

    @Test
    public void InsertPreToPostFinal() {
        Row newRow = testRow(fromTableRowType, 5, 50);
        otherGroupRows = getToExpected();
        dmlPreToPostFinal(insertCreator(ftID, newRow), getGroupExpected(), true, toDescriptors, columnNames, server, true);
    }

    @Test
    public void UpdatePreToPostFinal() {
        Row oldRow = testRow(fromTableRowType, 2, 20);
        Row newRow = testRow(fromTableRowType, 2, 21);
        otherGroupRows = getToExpected();
        dmlPreToPostFinal(updateCreator(ftID, oldRow, newRow), getGroupExpected(), true, toDescriptors, columnNames, server, true);
    }

    @Test
    public void DeletePreToPostFinal() {
        Row oldRow = fromGroupRows.get(0);
        otherGroupRows = getToExpected();
        dmlPreToPostFinal(deleteCreator(ftID, oldRow), getGroupExpected(), true, toDescriptors, columnNames, server, true);
    }
}