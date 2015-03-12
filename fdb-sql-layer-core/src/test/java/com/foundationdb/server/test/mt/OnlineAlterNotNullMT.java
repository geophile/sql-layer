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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/** Interleaved DML during an online column null to not-null change. */
public class OnlineAlterNotNullMT extends OnlineMTBase
{
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";
    private static final String COLUMN = "x";
    private static final String ALTER_NOT_NULL = "ALTER TABLE "+TABLE+" ALTER COLUMN "+COLUMN+" NOT NULL";

    private int tID;
    private TableRowType tableRowType;
    private List<Row> groupRows;
    private boolean expectedNullable;

    @Before
    public void createAndLoad() {
        tID = createTable(SCHEMA, TABLE, "id INT NOT NULL PRIMARY KEY, x INT");
        tableRowType = SchemaCache.globalSchema(ais()).tableRowType(tID);
        writeRows(row(tID, 2, 20),
                  row(tID, 4, 40));
        groupRows = runPlanTxn(groupScanCreator(tID));
        expectedNullable = false;
    }


    @Override
    protected String getDDL() {
        return ALTER_NOT_NULL;
    }

    @Override
    protected String getDDLSchema() {
        return SCHEMA;
    }

    @Override
    protected List<Row> getGroupExpected() {
        return groupRows;
    }

    @Override
    protected List<Row> getOtherExpected() {
        return null;
    }

    @Override
    protected OperatorCreator getGroupCreator() {
        return groupScanCreator(tID);
    }

    @Override
    protected OperatorCreator getOtherCreator() {
        return null;
    }

    @Override
    protected void postCheckAIS(AkibanInformationSchema ais) {
        Table table = ais.getTable(SCHEMA, TABLE);
        Column column = table.getColumn(COLUMN);
        assertEquals("column nullable", expectedNullable, column.getNullable());
    }

    //
    // I/U pre-to-post METADATA
    //

    @Test
    public void insertPreToPostMetadata() {
        Row newRow = testRow(tableRowType, 5, 50);
        dmlPreToPostMetadata(insertCreator(tID, newRow));
    }

    @Test
    public void updatePreToPostMetadata() {
        Row oldRow = testRow(tableRowType, 2, 20);
        Row newRow = testRow(tableRowType, 2, 21);
        dmlPreToPostMetadata(updateCreator(tID, oldRow, newRow));
    }

    //
    // I/U post METADATA to pre FINAL
    //

    @Test
    public void insertPostMetaToPreFinal() {
        Row newRow = testRow(tableRowType, 5, 50);
        dmlPostMetaToPreFinal(insertCreator(tID, newRow), combine(groupRows, newRow));
    }

    @Test
    public void updatePostMetaToPreFinal() {
        Row oldRow = testRow(tableRowType, 2, 20);
        Row newRow = testRow(tableRowType, 2, 21);
        dmlPostMetaToPreFinal(updateCreator(tID, oldRow, newRow), replace(groupRows, 0, newRow));
    }

    @Test
    public void insertViolationPostMetaToPreFinal() {
        expectedNullable = true;
        Row newRow = testRow(tableRowType, 5, null);
        dmlViolationPostMetaToPreFinal(insertCreator(tID, newRow), combine(groupRows, newRow));
    }

    @Test
    public void updateViolationPostMetaToPreFinal() {
        expectedNullable = true;
        Row oldRow = testRow(tableRowType, 2, 20);
        Row newRow = testRow(tableRowType, 2, null);
        dmlViolationPostMetaToPreFinal(updateCreator(tID, oldRow, newRow), replace(groupRows, 0, newRow));
    }

    //
    // I/U pre-to-post FINAL
    //

    @Test
    public void insertPreToPostFinal() {
        Row newRow = testRow(tableRowType, 5, 50);
        dmlPreToPostFinal(insertCreator(tID, newRow));
    }

    @Test
    public void updatePreToPostFinal() {
        Row oldRow = testRow(tableRowType, 2, 20);
        Row newRow = testRow(tableRowType, 2, 21);
        dmlPreToPostFinal(updateCreator(tID, oldRow, newRow));
    }
}
