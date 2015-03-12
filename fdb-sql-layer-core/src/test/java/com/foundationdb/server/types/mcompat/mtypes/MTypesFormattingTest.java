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

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TypeFormattingTestBase;
import com.foundationdb.server.types.mcompat.MBundle;
import com.foundationdb.server.types.value.ValueSource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber.*;
import static com.foundationdb.server.types.mcompat.mtypes.MBinary.*;
import static com.foundationdb.server.types.mcompat.mtypes.MDateAndTime.*;
import static com.foundationdb.server.types.mcompat.mtypes.MNumeric.*;
import static com.foundationdb.server.types.mcompat.mtypes.MString.*;

@RunWith(Parameterized.class)
public class MTypesFormattingTest extends TypeFormattingTestBase
{
    @Parameters(name="{0}")
    public static Collection<Object[]> types() throws Exception {
        List<Object[]> params = new ArrayList<>();
        for(TClass tClass : Arrays.asList(DECIMAL, DECIMAL_UNSIGNED)) {
            params.add(tCase(tClass, new BigDecimal("3.14"), "3.14", "\"3.14\"", "3.14"));
        }
        for(TClass tClass : Arrays.asList(TINYINT, TINYINT_UNSIGNED, SMALLINT, SMALLINT_UNSIGNED, MEDIUMINT, MEDIUMINT_UNSIGNED, INT, INT_UNSIGNED, BIGINT, BIGINT_UNSIGNED)) {
            params.add(tCase(tClass, 42, "42", "42", "42"));
        }
        for(TClass tClass : Arrays.asList(FLOAT, FLOAT_UNSIGNED, DOUBLE, DOUBLE_UNSIGNED)) {
            params.add(tCase(tClass, 3.14, "3.14", "3.14", "3.140000e+00"));
        }
        for(TClass tClass : Arrays.asList(CHAR, VARCHAR, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT)) {
            params.add(tCase(tClass, "hello", "hello", "\"hello\"", "'hello'"));
        }
        for(TClass tClass : Arrays.asList(BINARY, VARBINARY)) {
            params.add(tCase(tClass, new byte[]{ 0x41, 0x42 }, "AB", "\"\\x4142\"", "X'4142'"));
        }
        params.add(tCase(DATE, 1031372, "2014-06-12", "\"2014-06-12\"", "DATE '2014-06-12'"));
        params.add(tCase(DATETIME, 20140612174400L, "2014-06-12 17:44:00", "\"2014-06-12 17:44:00\"", "TIMESTAMP '2014-06-12 17:44:00'"));
        params.add(tCase(TIME, 10203, "01:02:03", "\"01:02:03\"", "TIME '01:02:03'"));
        params.add(tCase(TIMESTAMP, 1402595040, "2014-06-12 17:44:00", "\"2014-06-12 17:44:00\"", "TIMESTAMP '2014-06-12 17:44:00'"));
        params.add(tCase(YEAR, 100, "2000", "\"2000\"", "2000"));
        return checkParams(MBundle.INSTANCE.id(), params);
    }

    public MTypesFormattingTest(TClass tClass, ValueSource valueSource, String str, String json, String literal) {
        super(tClass, valueSource, str, json, literal);
    }
}
