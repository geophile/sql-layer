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
package com.foundationdb.server.types.mcompat.mcasts;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.server.types.ErrorHandlingMode;
import com.foundationdb.server.types.TExecutionContext;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class CastUtilsTest {
    
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        param(pb, "12.5", 13);
        param(pb, "-1299.5", -1300);
        param(pb, "2.0", 2);
        param(pb, "-2.0", -2);
        param(pb, "2.b", 2);
        param(pb, "2.4", 2);
        param(pb, "2.5", 3);
        param(pb, "-2.5", -3);
        param(pb, "", 0);
        param(pb, "a", 0);
        param(pb, "-", 0);
        param(pb, ".", 0);
        param(pb, "-.3", 0);
        param(pb, "-.6", -1);
        param(pb, "+.3", 0);
        param(pb, "+.6", 1);
        param(pb, ".6", 1);
        param(pb, ".6E4", 6000);
        param(pb, "123E4", 1230000);
        param(pb, "123E-4", 0);
        param(pb, "467E-3", 0);
        param(pb, "567E-3", 1);
        param(pb, "123.456E3", 123456);
        param(pb, "27474.83647e-4", 3);

        return pb.asList();
    }

    private static void param(ParameterizationBuilder pb, String input, long expected) {
        pb.add(input.length() == 0 ? "<empty>" : input, input, expected);
    }

    @Test
    public void testTruncate() {
        TExecutionContext context = new TExecutionContext(null, null, null, null,
                ErrorHandlingMode.IGNORE, ErrorHandlingMode.IGNORE, ErrorHandlingMode.IGNORE);
        long actual = CastUtils.parseInRange(input, Long.MAX_VALUE, Long.MIN_VALUE, context);
    
        assertEquals(input, expected, actual);
    }

    public CastUtilsTest(String input, long expected) {
        this.input = input;
        this.expected = expected;
    }

    private final String input;
    private final long expected;
}
