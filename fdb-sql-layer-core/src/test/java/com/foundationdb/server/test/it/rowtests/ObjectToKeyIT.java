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

import com.foundationdb.ais.model.Column;
import com.foundationdb.server.store.PersistitKeyAppender;
import com.foundationdb.server.test.it.ITBase;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public class ObjectToKeyIT extends ITBase {
    private final String SCHEMA = "test";
    private final String TABLE = "t";

     private void testObjectToKey(Column field, Object... testValues) throws PersistitException {
        Key key = store().createKey();
        PersistitKeyAppender appender = PersistitKeyAppender.create(key, null);
        for(Object inObj : testValues) {
            key.clear();
            appender.append(inObj, field);

            Object outObj = key.decode();
            if(outObj != null) {
                assertEquals(inObj.toString(), outObj.toString());
            }
            else {
                assertEquals(inObj, outObj);
            }
        }
    }

    @Test
    public void decimalField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, false, false, new SimpleColumn("c2", "MCOMPAT_ decimal", 5L, 2L));
        final Column column = getTable(tid).getColumn("c2");
        testObjectToKey(column,
                        null, BigDecimal.valueOf(-12345, 2), 578L, "999.99");
    }

    @Test
    public void decimalUnsignedField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, false, true, new SimpleColumn("c2", "MCOMPAT_ decimal", 5L, 2L));
        final Column column = getTable(tid).getColumn("c2");
        testObjectToKey(column,
                        null, BigDecimal.valueOf(0), BigDecimal.valueOf(4242, 2), 489L, "978.83");
    }
}
