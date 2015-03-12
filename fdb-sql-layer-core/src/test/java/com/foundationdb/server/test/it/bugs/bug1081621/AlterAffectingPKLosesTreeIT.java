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
package com.foundationdb.server.test.it.bugs.bug1081621;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.util.TableChangeValidator;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

public class AlterAffectingPKLosesTreeIT extends ITBase {
    private final static String SCHEMA = "test";
    private final static TableName P_NAME = new TableName(SCHEMA, "p");
    private final static TableName C_NAME = new TableName(SCHEMA, "c");

    private void createTables() {
        createTable(P_NAME, "id int not null primary key, x int");
        createTable(C_NAME, "id int not null primary key, pid int, grouping foreign key(pid) references p(id)");
    }

    @Test
    public void test() throws Exception {
        createTables();

        runAlter(TableChangeValidator.ChangeLevel.GROUP, SCHEMA, "ALTER TABLE p DROP COLUMN id");
        runAlter(TableChangeValidator.ChangeLevel.GROUP, SCHEMA, "ALTER TABLE c DROP COLUMN id");

        ddl().dropTable(session(), P_NAME);
        ddl().dropTable(session(), C_NAME);

        safeRestartTestServices();

        createTables();
    }
}
