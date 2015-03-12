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
package com.foundationdb.server.test.it.bugs.bug1017621;

import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class TableIDCollisionIT extends ITBase {
    private Table simpleISTable() {
        final TableName FAKE_TABLE = new TableName(TableName.INFORMATION_SCHEMA, "fake_table");
        NewAISBuilder builder = AISBBasedBuilder.create(ddl().getTypesTranslator());
        builder.table(FAKE_TABLE).colInt("id").pk("id");
        Table table = builder.ais().getTable(FAKE_TABLE);
        assertNotNull("Found table", table);
        return table;
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        // Something unique, since we are messing with IS tables
        return uniqueStartupConfigProperties(getClass());
    }

    @Test
    public void createRestartAndCreate() throws Exception {
        createTable("test", "t1", "id int");
        safeRestartTestServices();
        createTable("test", "t2", "id int");
        serviceManager().getSchemaManager().registerStoredInformationSchemaTable(simpleISTable(), 1);
        createTable("test", "t3", "id int");
    }
}
