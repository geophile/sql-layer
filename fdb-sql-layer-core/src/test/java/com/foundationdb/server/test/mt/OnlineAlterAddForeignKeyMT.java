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
import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Interleaved DML during an ALTER ADD FOREIGN KEY. */
public class OnlineAlterAddForeignKeyMT extends OnlineMTBase
{
    protected static final String SCHEMA = "test";
    protected static final String PARENT_TABLE = "p";
    protected static final String CHILD_TABLE = "c";
    protected static final String ALTER_ADD_FK = "ALTER TABLE "+CHILD_TABLE+" ADD CONSTRAINT fk1 FOREIGN KEY(pid) "+
                                                 "REFERENCES "+PARENT_TABLE+"(pid)";

    protected int pID, cID;
    protected TableRowType parentRowType, childRowType;
    protected List<Row> parentGroupRows, childGroupRows;
    protected boolean expectedFKPresent = true;

    @Before
    public void createAndLoad() {
        pID = createTable(SCHEMA, PARENT_TABLE, "pid INT NOT NULL PRIMARY KEY");
        cID = createTable(SCHEMA, CHILD_TABLE, "cid INT NOT NULL PRIMARY KEY, pid INT");
        parentRowType = SchemaCache.globalSchema(ais()).tableRowType(pID);
        childRowType = SchemaCache.globalSchema(ais()).tableRowType(cID);
        writeRows(row(pID, 1),
                  row(pID, 2));
        writeRows(row(cID, 10, 1),
                  row(cID, 20, 2));
        parentGroupRows = runPlanTxn(groupScanCreator(pID));
        childGroupRows = runPlanTxn(groupScanCreator(cID));
    }

    @Override
    protected String getDDL() {
        return ALTER_ADD_FK;
    }

    @Override
    protected String getDDLSchema() {
        return SCHEMA;
    }

    @Override
    protected List<Row> getGroupExpected() {
        return childGroupRows;
    }

    @Override
    protected List<Row> getOtherExpected() {
        return null;
    }

    @Override
    protected OperatorCreator getGroupCreator() {
        return groupScanCreator(cID);
    }

    @Override
    protected OperatorCreator getOtherCreator() {
        return null;
}

    @Override
    protected void postCheckAIS(AkibanInformationSchema ais) {
        Table table = ais.getTable(SCHEMA, CHILD_TABLE);
        List<String> fks = new ArrayList<>();
        for(ForeignKey f : table.getReferencingForeignKeys()) {
            fks.add(f.getConstraintName().getTableName());
        }
        assertEquals(expectedFKPresent ? "[fk1]" : "[]", fks.toString());
    }


    //
    // I/U pre-to-post METADATA
    //

    @Test
    public void insertPreToPostMetadata_Child() {
        Row newRow = testRow(childRowType, 100, 10);
        dmlPreToPostMetadata(insertCreator(cID, newRow));
    }

    @Test
    public void updatePreToPostMetadata_Child() {
        Row oldRow = testRow(childRowType, 20, 2);
        Row newRow = testRow(childRowType, 20, 20);
        dmlPreToPostMetadata(updateCreator(cID, oldRow, newRow));
    }

    @Test
    public void updatePreToPostMetadata_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        Row newRow = testRow(parentRowType, 3);
        dmlPreToPostMetadata(updateCreator(pID, oldRow, newRow));
    }

    @Test
    public void deletePreToPostMetadata_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        dmlPreToPostMetadata(deleteCreator(pID, oldRow));
    }

    //
    // I/U post METADATA to pre FINAL
    //

    @Test
    public void insertPostMetaToPreFinal_Child() {
        Row newRow = testRow(childRowType, 21, 2);
        dmlPostMetaToPreFinal(insertCreator(cID, newRow), combine(childGroupRows, newRow));
    }

    @Test
    public void updatePostMetaToPreFinal_Child() {
        Row oldRow = testRow(childRowType, 10, 1);
        Row newRow = testRow(childRowType, 10, 2);
        dmlPostMetaToPreFinal(updateCreator(cID, oldRow, newRow), replace(childGroupRows, 0, newRow));
    }

    @Test
    public void insertViolationPostMetaToPreFinal_Child() {
        Row newRow = testRow(childRowType, 100, 10);
        dmlViolationPostMetaToPreFinal(insertCreator(cID, newRow), parentGroupRows, combine(childGroupRows, newRow));
    }

    @Test
    public void updateViolationPostMetaToPreFinal_Child() {
        Row oldRow = testRow(childRowType, 20, 2);
        Row newRow = testRow(childRowType, 20, 20);
        dmlViolationPostMetaToPreFinal(updateCreator(cID, oldRow, newRow), parentGroupRows, replace(childGroupRows, 1, newRow));
    }

    @Test
    public void updateViolationPostMetaToPreFinal_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        Row newRow = testRow(parentRowType, 3);
        dmlViolationPostMetaToPreFinal(updateCreator(pID, oldRow, newRow), replace(parentGroupRows, 1, newRow), childGroupRows);
    }

    @Test
    public void deleteViolationPostMetaToPreFinal_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        dmlViolationPostMetaToPreFinal(deleteCreator(pID, oldRow), remove(parentGroupRows, 1), childGroupRows);
    }

    //
    // I/U pre-to-post FINAL
    //

    @Test
    public void insertPreToPostFinal_Child() {
        Row newRow = testRow(childRowType, 21, 2);
        dmlPreToPostFinal(insertCreator(cID, newRow));
    }

    @Test
    public void updatePreToPostFinal_Child() {
        Row oldRow = testRow(childRowType, 10, 1);
        Row newRow = testRow(childRowType, 10, 2);
        dmlPreToPostFinal(updateCreator(cID, oldRow, newRow));
    }

    @Test
    public void updatePreToPostFinal_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        Row newRow = testRow(parentRowType, 3);
        dmlPreToPostFinal(updateCreator(pID, oldRow, newRow));
    }

    @Test
    public void deletePreToPostFinal_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        dmlPreToPostFinal(deleteCreator(pID, oldRow));
    }

    protected void dmlViolationPostMetaToPreFinal(OperatorCreator dmlCreator,
                                                  List<Row> finalParentRows, List<Row> finalChildRows) {
        dmlViolationPostMetaToPreFinal(dmlCreator, finalParentRows, finalChildRows, false);
    }

    protected void dmlViolationPostMetaToPreFinal(OperatorCreator dmlCreator,
                                                  List<Row> finalParentRows, List<Row> finalChildRows,
                                                  boolean isDDLPassing) {
        this.expectedFKPresent = isDDLPassing;
        dmlPostMetaToPreFinal(dmlCreator, finalChildRows, true, isDDLPassing);
        checkExpectedRows(finalParentRows, groupScanCreator(pID));
    }
}
