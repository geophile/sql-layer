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
package com.foundationdb.qp.loadableplan.std;

import com.foundationdb.qp.loadableplan.DirectObjectCursor;
import com.foundationdb.qp.loadableplan.DirectObjectPlan;
import com.foundationdb.qp.loadableplan.std.DumpGroupLoadablePlan;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.service.transaction.TransactionService.CloseableTransaction;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.sql.TestBase;
import com.foundationdb.sql.embedded.EmbeddedJDBCITBase;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import java.sql.Statement;
import java.io.File;
import java.util.Collection;
import java.util.List;

@RunWith(NamedParameterizedRunner.class)
public class DumpGroupLoadablePlanIT extends EmbeddedJDBCITBase
{
    public static final File RESOURCE_DIR = 
        new File("src/test/resources/"
                 + DumpGroupLoadablePlan.class.getPackage().getName().replace('.', '/'));
    public static final File SCHEMA_FILE = new File(RESOURCE_DIR, "schema.ddl");
    public static final String GROUP_NAME = "customers";

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        pb.add("single", new File(RESOURCE_DIR, GROUP_NAME + ".sql"), GROUP_NAME, false, -1);
        pb.add("single/commit", new File(RESOURCE_DIR, GROUP_NAME + ".sql"),GROUP_NAME, false, 1);
        pb.add("multiple", new File(RESOURCE_DIR, GROUP_NAME + "-m.sql"),GROUP_NAME, true, -1);
        pb.add("multiple/commit", new File(RESOURCE_DIR, GROUP_NAME + "-m.sql"),GROUP_NAME, true, 1);
        pb.add("values", new File(RESOURCE_DIR, "values.sql"), "values", true, 1);
        pb.add("guid", new File(RESOURCE_DIR, "guid.sql"), "guid_table", true, 1);
        pb.add("strings", new File(RESOURCE_DIR, "strings.sql"), "strings", true, 1);
        return pb.asList();
    }

    private File file;
    private boolean multiple;
    private int commitFreq;
    private String groupName;

    public DumpGroupLoadablePlanIT(File file, String groupName, boolean multiple, int commitFreq) {
        this.file = file;
        this.multiple = multiple;
        this.commitFreq = commitFreq;
        this.groupName = groupName;
    }

    @Before
    public void loadDatabase() throws Exception {
        loadSchemaFile(SCHEMA_NAME, SCHEMA_FILE);
    }

    @Test
    public void testDump() throws Exception {
        String expectedSQL = runSQL();
        try(CloseableTransaction txn = txnService().beginCloseableTransaction(session())) {
            runPlan(expectedSQL);
            txn.commit();
        }
    }

    private String runSQL() throws Exception {
        // Run the INSERTs via SQL.
        String sql = TestBase.fileContents(file);

        Statement stmt = getConnection().createStatement();
        for (String sqls : sql.split("\\;\\s*")) {
            stmt.execute(sqls);
        }
        stmt.close();
        return sql;
    }

    private void runPlan(String expectedSQL) throws Exception {
        DumpGroupLoadablePlan loadablePlan = new DumpGroupLoadablePlan();
        DirectObjectPlan plan = loadablePlan.plan();

        StoreAdapter adapter = newStoreAdapter();
        QueryContext queryContext = new SimpleQueryContext(adapter) {
                @Override
                public String getCurrentSchema() {
                    return SCHEMA_NAME;
                }
            };
        QueryBindings queryBindings = queryContext.createBindings();
        queryBindings.setValue(0, new Value(MString.varcharFor(SCHEMA_NAME), SCHEMA_NAME));
        queryBindings.setValue(1, new Value(MString.varcharFor(groupName), groupName));
        if (multiple)
            queryBindings.setValue(2, new Value(MNumeric.INT.instance(false), 10));
        if (commitFreq > 0)
            queryBindings.setValue(3, new Value(MNumeric.INT.instance(false), commitFreq));

        DirectObjectCursor cursor = plan.cursor(queryContext, queryBindings);
        
        StringBuilder actual = new StringBuilder();

        cursor.open();
        while(true) {
            List<?> columns = cursor.next();
            if (columns == null) {
                break;
            }
            else if (!columns.isEmpty()) {
                assertTrue(columns.size() == 1);
                if (actual.length() > 0)
                    actual.append("\n");
                actual.append(columns.get(0));
            }
        }
        cursor.close();

        assertEquals(expectedSQL, actual.toString());
    }

}
