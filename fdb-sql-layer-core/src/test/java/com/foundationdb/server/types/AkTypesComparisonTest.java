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
package com.foundationdb.server.types;

import com.foundationdb.junit.SelectedParameterizedRunner;
import com.foundationdb.server.types.aksql.AkBundle;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;
import com.foundationdb.server.types.aksql.aktypes.AkBlob;
import com.foundationdb.server.types.aksql.aktypes.AkGeometry;
import com.foundationdb.server.types.aksql.aktypes.AkInterval;
import com.foundationdb.server.types.aksql.aktypes.AkResultSet;
import com.foundationdb.server.types.value.Value;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

@RunWith(SelectedParameterizedRunner.class)
public class AkTypesComparisonTest extends TypeComparisonTestBase
{
    @Parameters(name="{0}")
    public static Collection<Object[]> types() throws Exception {
        return makeParams(
            AkBundle.INSTANCE.id(),
            Arrays.asList(
                typeInfo(AkBool.INSTANCE, false, true),
                typeInfo(AkInterval.MONTHS, Long.MIN_VALUE, 0L, Long.MAX_VALUE),
                typeInfo(AkInterval.SECONDS, Long.MIN_VALUE, 0L, Long.MAX_VALUE)
            ),
            Arrays.asList(
                AkGeometry.INSTANCE,
                AkGUID.INSTANCE,
                AkResultSet.INSTANCE,
                AkBlob.INSTANCE    
            )
        );
    }

    public AkTypesComparisonTest(String name, Value a, Value b, int expected) throws Exception {
        super(name, a, b, expected);
    }
}
