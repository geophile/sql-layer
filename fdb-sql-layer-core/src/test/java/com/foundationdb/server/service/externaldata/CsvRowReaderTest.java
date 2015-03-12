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
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.SchemaFactory;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.util.Strings;

import org.junit.Test;

import static org.junit.Assert.*;

import java.io.*;
import java.util.*;

public final class CsvRowReaderTest {
    public static final String[] CSV = {
        "id,s,t",
        "1,abc,2010-01-01",
        "2,xyz,",
        "3,,2010-04-01",
        "666,\"quoted \"\"string\"\" here\",2013-01-01 13:02:03"
    };

    public static final Object[][] ROWS = {
        { 1, 20100101000000L, 100, "abc" },
        { 2, null, 100, "xyz" },
        { 3, 20100401000000L, 100, null },
        { 666, 20130101130203L, 100, "quoted \"string\" here"}
    };

    public static final String DDL =
        "CREATE TABLE t1(id INT NOT NULL PRIMARY KEY, t TIMESTAMP, n INT DEFAULT 100, s VARCHAR(128))";

    @Test
    public void reader() throws Exception {
        SchemaFactory schemaFactory = new SchemaFactory("test");
        AkibanInformationSchema ais = schemaFactory.aisWithTableStatus(DDL);
        Table t1 = ais.getTable("test", "t1");
        InputStream istr = new ByteArrayInputStream(Strings.join(CSV).getBytes("UTF-8"));
        List<Column> columns = new ArrayList<>(3);
        for (String cname : CSV[0].split(","))
            columns.add(t1.getColumn(cname));
        CsvRowReader reader = new CsvRowReader(t1, columns,
                                               istr, new CsvFormat("UTF-8"), 
                                               null, MTypesTranslator.INSTANCE);
        reader.skipRows(1); // Header
        List<Row> rows = new ArrayList<>();
        Row row;
        while ((row = reader.nextRow()) != null)
            rows.add(row);
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
