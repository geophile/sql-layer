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
package com.foundationdb.server.test.it.bugs.bug1033617;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

public final class DropTablesInNewSessionIT extends ITBase {
    @Test
    public void test() {
        int c = createTable("schema", "customers", "cid int not null primary key, name varchar(32)");
        int o = createTable("schema", "orders", "oid int not null primary key, cid int not null, placed date",
                akibanFK("cid", "customers", "cid"));
        TableName groupName = getTable(c).getGroup().getName();
        createLeftGroupIndex(groupName, "name_placed", "customers.name", "orders.placed");

        writeRow(c, 1, "bob");
        writeRow(o, 11, 1, "2012-01-01");

        Collection<String> indexesToUpdate = Collections.singleton("name_placed");
        ddl().updateTableStatistics(session(), TableName.create("schema", "customers"), indexesToUpdate);

        Session session = serviceManager().getSessionService().createSession();
        try {
            dropAllTables(session);
        }
        finally {
            session.close();
        }
    }
}
