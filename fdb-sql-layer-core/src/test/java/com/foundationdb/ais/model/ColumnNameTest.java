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
package com.foundationdb.ais.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ColumnNameTest
{
    @Test
    public void parseFull() {
        testParse(new ColumnName("a", "b", "c"), null, "a.b.c");
    }

    @Test
    public void parseNoSchema() {
        testParse(new ColumnName("def", "a", "b"), "def", "a.b");
    }

    @Test
    public void parseEmpty() {
        testParse(new ColumnName("def", "", ""), "def", "");
    }

    private static void testParse(ColumnName expected, String defSchema, String str) {
        ColumnName actual = ColumnName.parse(defSchema, str);
        assertEquals("schema", expected.getTableName().getSchemaName(), actual.getTableName().getSchemaName());
        assertEquals("table", expected.getTableName().getTableName(), actual.getTableName().getTableName());
        assertEquals("column", expected.getName(), actual.getName());
    }
}
