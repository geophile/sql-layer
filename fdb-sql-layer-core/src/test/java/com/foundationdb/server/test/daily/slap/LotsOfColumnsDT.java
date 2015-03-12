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
package com.foundationdb.server.test.daily.slap;

import org.junit.Test;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.test.daily.DailyBase;

public class LotsOfColumnsDT extends DailyBase {
    private final static int COLUMN_COUNT = 10000;

    @Test
    public void testIntColumns() {
        runTest("INT", "test_1");
    }
    
    @Test
    public void testCharColumns() {
        runTest("CHAR(1)", "table_2");
    }
    
    @Test
    public void testDoubleColumns() {
        runTest("DOUBLE", "table_3");
    }
    
    @Test
    public void testVarcharSmallColumns() {
        runTest("VARCHAR(5)", "table_4");
    }
    
    @Test
    public void testVarcharLargeColumns() {
        runTest("VARCHAR(300)", "table_5");
    }
    
    @Test
    public void testDecimalSmall() {
        runTest ("DECIMAL(3,2)", "table_6");
    }

    @Test
    public void testDecimalLarge() {
        runTest("DECIMAL(31, 21)", "table_7");
    }
    
    private void runTest (String type, String tableName) {
        StringBuilder query = new StringBuilder(COLUMN_COUNT * 32);
        
        generateColumnList (type, query);
        query.append("ID INT NOT NULL PRIMARY KEY");
        createTable("test", tableName, query.toString());

        ddl().dropTable(session(), new TableName("test", tableName));
    }

    private void generateColumnList (String type, StringBuilder query) {
        for (int i = 0; i < COLUMN_COUNT; i++) {
            query.append(String.format("COLUMN_%05d %s NOT NULL,", i, type));
        }
        
    }
}
