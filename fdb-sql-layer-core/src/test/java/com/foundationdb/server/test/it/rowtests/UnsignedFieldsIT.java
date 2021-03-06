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
package com.foundationdb.server.test.it.rowtests;

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.types.value.ValueSources;
import org.junit.Assert;

import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class UnsignedFieldsIT extends ITBase {
    private final String SCHEMA = "test";
    private final String TABLE = "t";
    private final boolean IS_PK = false;
    private final boolean INDEXES = false;
    
    private void writeRows(int tableId, Object... values) {
        long id = 0;
        for(Object o : values) {
            writeRow(tableId, id++, o);
        }
    }

    private void compareRows(int tableId, Object... values) {
        List<Row> rows = scanAll(tableId);
        assertEquals("column count", 2, getTable(tableId).getColumns().size());
        Iterator<Row> rowIt = rows.iterator();
        Iterator<Object> expectedIt = Arrays.asList(values).iterator();
        while(rowIt.hasNext() && expectedIt.hasNext()) {
            Row row = rowIt.next();
            Number actual = (Number)ValueSources.toObject(row.value(1));
            if (actual instanceof Short) {
                actual = actual.longValue();
            } else if (actual instanceof Integer) {
                actual = actual.longValue();
            }
            Number expected = (Number)expectedIt.next();
            if (actual instanceof Long && expected instanceof BigInteger) {
                actual = BigInteger.valueOf(actual.longValue());
            }
            assertEquals("row id " + ValueSources.toObject(row.value(0)), expected, actual);
        }
        String extra = "";
        while(rowIt.hasNext()) {
            extra += rowIt.next() + ",";
        }
        if(!extra.isEmpty()) {
            Assert.fail("Extra rows from scan: " + extra);
        }
        while(expectedIt.hasNext()) {
            extra += expectedIt.next() + ",";
        }
        if(!extra.isEmpty()) {
            Assert.fail("Expected more rows from scan: " + extra);
        }
    }

    private void writeRowsAndCompareValues(int tableId, Object... values) {
        writeRows(tableId,  values);
        compareRows(tableId, values);
    }

    private Object[] getTestValues(int bitCount) {
        long signedMax = (1L << (bitCount - 1)) - 1;
        return array(0L, 1L, signedMax - 2, signedMax - 1, signedMax, signedMax + 1, signedMax + 2, signedMax*2 + 1);
    }

    
    @Test
    public void tinyIntUnsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ tinyint unsigned");
        Object[] values = getTestValues(8);
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void smallIntUnsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ smallint unsigned");
        Object[] values = getTestValues(16);
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void mediumIntUnsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ mediumint unsigned");
        Object[] values = getTestValues(24);
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void intUnsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ int unsigned");
        Object[] values = getTestValues(32);
        writeRowsAndCompareValues(tid, values);
    }

    @Ignore ("BigInt Unsigned doesn't handle 9223372036854775808 correctly")
    @Test
    public void bigIntUnsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ bigint unsigned");
        Object[] values = {new BigInteger("0"), new BigInteger("1"),
                           new BigInteger("9223372036854775805"), new BigInteger("9223372036854775806"),
                           new BigInteger("9223372036854775807"),
                           new BigInteger("9223372036854775808"), new BigInteger("9223372036854775809"),
                           new BigInteger("18446744073709551615")};
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void decimal52Unsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, new SimpleColumn("c1", "MCOMPAT_ decimal unsigned", 5L, 2L));
        Object[] values = array(new BigDecimal("0.00"), new BigDecimal("1.00"),
                                new BigDecimal("499.99"), new BigDecimal("500.00"), new BigDecimal("999.99"));
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void decimal2010Unsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, new SimpleColumn("c1", "MCOMPAT_ decimal unsigned", 20L, 10L));
        Object[] values = array(new BigDecimal("0.0000000000"), new BigDecimal("1.0000000000"),
                                new BigDecimal("4999999999.9999999999"), new BigDecimal("5000000000.0000000000"),
                                new BigDecimal("9999999999.9999999999"));
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void floatUnsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ float unsigned");
        Object[] values = array(0.0f, 1.0f, Float.MAX_VALUE);
        writeRowsAndCompareValues(tid, values);
    }
    
    @Test
    public void doubleUnsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ double unsigned");
        Object[] values = array(0.0d, 1.0d, Double.MAX_VALUE);
        writeRowsAndCompareValues(tid, values);
    }
}
