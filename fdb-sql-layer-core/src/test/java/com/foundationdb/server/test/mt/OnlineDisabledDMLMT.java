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
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.NotAllowedByConfigException;
import com.foundationdb.server.service.config.TestConfigService;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

/** Interleaved DML during an online create index <i>when DML is disabled</i>. */
public class OnlineDisabledDMLMT extends OnlineMTBase
{
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";
    private static final String COLUMN = "x";
    private static final String INDEX = "x";
    private static final String CREATE_INDEX = "CREATE INDEX "+INDEX+" ON "+TABLE+"("+COLUMN+")";

    private int tID;
    private TableRowType tableRowType;
    private RowType indexRowType;
    private List<Row> groupRows;

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String, String> map = new HashMap<>(super.startupConfigProperties());
        map.put(TestConfigService.FEATURE_DDL_WITH_DML_KEY, "false");
        return map;
    }

    @Before
    public void createAndLoad() {
        tID = createTable(SCHEMA, TABLE, "id INT NOT NULL PRIMARY KEY, x INT");
        tableRowType = SchemaCache.globalSchema(ais()).tableRowType(tID);
        writeRows(row(tID, 2, 20),
                  row(tID, 4, 40));
        groupRows = runPlanTxn(groupScanCreator(tID));

        indexRowType = tableRowType.schema().newValuesType(tableRowType.typeAt(1), tableRowType.typeAt(0));
    }


    @Override
    protected String getDDL() {
        return CREATE_INDEX;
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
        return Arrays.asList(testRow(indexRowType, 20, 2), testRow(indexRowType, 40, 4));
    }

    @Override
    protected OperatorCreator getGroupCreator() {
        return groupScanCreator(tID);
    }

    @Override
    protected OperatorCreator getOtherCreator() {
        return indexScanCreator(tID, INDEX);
    }

    @Override
    protected void postCheckAIS(AkibanInformationSchema ais) {
        Table table = ais.getTable(SCHEMA, TABLE);
        assertNotNull("index present", table.getIndex(INDEX));
    }

    protected Class<? extends Exception> getFailingDMLExceptionClass() {
        return NotAllowedByConfigException.class;
    }


    //
    // I/U/D post METADATA to pre FINAL
    //

    @Test
    public void insertPostMetaToPreFinal() {
        Row newRow = testRow(tableRowType, 5, 50);
        dmlPostMetaToPreFinal(insertCreator(tID, newRow), groupRows, false, true);
    }

    @Test
    public void updatePostMetaToPreFinal() {
        Row oldRow = testRow(tableRowType, 2, 20);
        Row newRow = testRow(tableRowType, 2, 21);
        dmlPostMetaToPreFinal(updateCreator(tID, oldRow, newRow), groupRows, false, true);
    }

    @Test
    public void deletePostMetaToPreFinal() {
        Row oldRow = testRow(tableRowType, 2, 20);
        dmlPostMetaToPreFinal(deleteCreator(tID, oldRow), groupRows, false, true);
    }
}
