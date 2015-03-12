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
package com.foundationdb.server.types.mcompat.mtypes;

import com.foundationdb.server.types.common.types.TypesTranslatorTest;

import org.junit.Test;

public class MTypesTranslatorTest extends TypesTranslatorTest
{
    public MTypesTranslatorTest() {
        super(MTypesTranslator.INSTANCE);
    }

    @Test
    public void testTypesTranslator() throws Exception {
        testType("INTEGER", "MCOMPAT_ INT(11)");
        testType("TINYINT", "MCOMPAT_ TINYINT(5)");
        testType("SMALLINT", "MCOMPAT_ SMALLINT(7)");
        testType("MEDIUMINT", "MCOMPAT_ MEDIUMINT(9)");
        testType("BIGINT", "MCOMPAT_ BIGINT(21)");

        testType("INTEGER UNSIGNED", "MCOMPAT_ INT UNSIGNED(10)");
        testType("TINYINT UNSIGNED", "MCOMPAT_ TINYINT UNSIGNED(4)");
        testType("SMALLINT UNSIGNED", "MCOMPAT_ SMALLINT UNSIGNED(6)");
        testType("MEDIUMINT UNSIGNED", "MCOMPAT_ MEDIUMINT UNSIGNED(8)");
        testType("BIGINT UNSIGNED", "MCOMPAT_ BIGINT UNSIGNED(20)");

        testType("REAL", "MCOMPAT_ FLOAT(-1, -1)");
        testType("DOUBLE", "MCOMPAT_ DOUBLE(-1, -1)");
        testType("FLOAT", "MCOMPAT_ DOUBLE(-1, -1)");
        testType("FLOAT(10)", "MCOMPAT_ FLOAT(-1, -1)");
        testType("DOUBLE UNSIGNED", "MCOMPAT_ DOUBLE UNSIGNED(-1, -1)");

        testType("DECIMAL(4,2)", "MCOMPAT_ DECIMAL(4, 2)");
        testType("DECIMAL(8,0) UNSIGNED", "MCOMPAT_ DECIMAL UNSIGNED(8, 0)");

        testType("VARCHAR(16)", "MCOMPAT_ VARCHAR(16, UTF8, UCS_BINARY)");
        testType("VARCHAR(16) COLLATE EN_US_CI_CO", "MCOMPAT_ VARCHAR(16, UTF8, en_us_ci_co)");
        testType("CHAR(2)", "MCOMPAT_ CHAR(2, UTF8, UCS_BINARY)");

        testType("DATE", "MCOMPAT_ DATE");
        testType("TIME", "MCOMPAT_ TIME");
        testType("DATETIME", "MCOMPAT_ DATETIME");
        testType("TIMESTAMP", "MCOMPAT_ DATETIME");
        testType("YEAR", "MCOMPAT_ YEAR");

        testType("CLOB", "MCOMPAT_ LONGTEXT(2147483647, UTF8, UCS_BINARY)");
        testType("TEXT", "MCOMPAT_ TEXT(65535, UTF8, UCS_BINARY)");
        testType("TINYTEXT", "MCOMPAT_ TINYTEXT(255, UTF8, UCS_BINARY)");
        testType("MEDIUMTEXT", "MCOMPAT_ MEDIUMTEXT(16777215, UTF8, UCS_BINARY)");
        testType("LONGTEXT", "MCOMPAT_ LONGTEXT(2147483647, UTF8, UCS_BINARY)");

        testType("BOOLEAN", "AKSQL_ BOOLEAN");
    }
    
}
