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
package com.foundationdb.server.types.aksql.aktypes;

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TypeFormattingTestBase;
import com.foundationdb.server.types.aksql.AkBundle;
import com.foundationdb.server.types.value.ValueSource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class AkTypesFormattingTest extends TypeFormattingTestBase
{
    @Parameters(name="{0}")
    public static Collection<Object[]> types() throws Exception {
        List<Object[]> params = new ArrayList<>();

        params.add(tCase(AkBool.INSTANCE, true, "true", "true", "TRUE"));
        String guid = "10f11fbe-d9f2-11e3-b96e-7badf4bedd17";
        params.add(tCase(AkGUID.INSTANCE, guid, guid, '"' + guid + '"', "'" + guid + "'"));
        params.add(tCase(AkBlob.INSTANCE, new byte[]{ 0x01, 0x42 }, null, "\"\\x42\"", "X'0142'"));
        params.add(tCase(AkInterval.MONTHS, 13, "INTERVAL '1-1'", "13", "INTERVAL '1-01' YEAR TO MONTH"));
        params.add(tCase(AkInterval.SECONDS, 90061000001L, "INTERVAL '1 1:1:1.00001'", "90061.000001", "INTERVAL '1:01:01:01.000001' DAY TO SECOND"));
        return checkParams(AkBundle.INSTANCE.id(), params, AkResultSet.INSTANCE, AkGeometry.INSTANCE);
    }

    public AkTypesFormattingTest(TClass tClass, ValueSource valueSource, String str, String json, String literal) {
        super(tClass, valueSource, str, json, literal);
    }
}

