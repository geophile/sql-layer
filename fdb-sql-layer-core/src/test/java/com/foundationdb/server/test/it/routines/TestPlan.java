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
package com.foundationdb.server.test.it.routines;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.loadableplan.LoadableOperator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.test.ExpressionGenerators;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;

import java.sql.Types;
import java.util.Arrays;

import static com.foundationdb.qp.operator.API.groupScan_Default;
import static com.foundationdb.qp.operator.API.project_Default;

/** A loadable operator plan.
 * <code><pre>
DROP TABLE test;
CREATE TABLE test(id INT PRIMARY KEY NOT NULL, value VARCHAR(10));
INSERT INTO test VALUES(1, 'aaa'), (2, 'bbb');
CALL sqlj.install_jar('target/fdb-sql-layer-x.y.z-tests.jar', 'testjar', 0);
CREATE PROCEDURE test(IN n BIGINT) LANGUAGE java PARAMETER STYLE foundationdb_loadable_plan EXTERNAL NAME 'testjar:com.foundationdb.server.test.it.routines.TestPlan';
CALL test(666);
 * </pre></code> 
 */
public class TestPlan extends LoadableOperator
{
    @Override
    public Operator plan()
    {
        // select id, value, $1 from test
        Group group = ais().getGroup(new TableName("test", "test"));
        Table testTable = ais().getTable("test", "test");
        RowType testRowType = schema().tableRowType(testTable);
        return
            project_Default(
                groupScan_Default(group),
                Arrays.asList(ExpressionGenerators.field(testRowType, 0),
                              ExpressionGenerators.field(testRowType, 1),
                              ExpressionGenerators.variable(MNumeric.INT.instance(true), 0)),
                testRowType);
    }

    @Override
    public int[] jdbcTypes()
    {
        return TYPES;
    }

    private static final int[] TYPES = new int[]{Types.INTEGER, Types.VARCHAR, Types.INTEGER};
}
