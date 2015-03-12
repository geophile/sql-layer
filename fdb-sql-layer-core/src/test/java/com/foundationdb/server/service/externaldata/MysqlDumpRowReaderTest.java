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
package com.foundationdb.server.service.externaldata;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.SchemaFactory;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.server.types.value.ValueSources;

import org.junit.Test;

import static org.junit.Assert.*;

import java.io.*;
import java.util.*;

public final class MysqlDumpRowReaderTest {
    public static final File RESOURCE_DIR = 
        new File("src/test/resources/"
                 + MysqlDumpRowReaderTest.class.getPackage().getName().replace('.', '/'));
    public static final File DUMP_FILE = new File(RESOURCE_DIR, "t1.sql");
    
    public static final Object[][] ROWS = {
        { 2, null },
        { 3, "a 'xyz' b" },
        { 5, "abc\nxyz" },
        { 1, "foo" },
        { 6, "\u2603" }
    };

    public static final String DDL =
        "CREATE TABLE t1(id INT NOT NULL PRIMARY KEY, s VARCHAR(32))";

    @Test
    public void reader() throws Exception {
        SchemaFactory schemaFactory = new SchemaFactory("test");
        AkibanInformationSchema ais = schemaFactory.aisWithTableStatus(DDL);
        Table t1 = ais.getTable("test", "t1");
        InputStream istr = new FileInputStream(DUMP_FILE);
        MysqlDumpRowReader reader = new MysqlDumpRowReader(t1, t1.getColumns(), 
                                                           istr, "UTF-8",
                                                           null, MTypesTranslator.INSTANCE);
        List<Row> rows = new ArrayList<>();
        Row row;
        while ((row = reader.nextRow()) != null)
            rows.add(row);
        istr.close();
        assertEquals("number of rows", ROWS.length, rows.size());
        for (int i = 0; i < ROWS.length; i++) {
            Object[] orow = ROWS[i];
            row = rows.get(i);
            assertEquals("row " + i + " size", orow.length, row.rowType().nFields());
            for (int j = 0; j < orow.length; j++) {
                assertEquals("row " + i + " col " + j, orow[j], ValueSources.toObject(row.value(j)));
            }
        }
    }

}
