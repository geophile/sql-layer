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

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.mcompat.MParsers;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;

@RunWith(NamedParameterizedRunner.class)
public final class DecimalParserTest {

    @TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder builder = new ParameterizationBuilder();

        param(builder, "1234.56", 6, 2, "1234.56");
        param(builder, "-1234.56", 6, 2, "-1234.56");
        param(builder, "1234.56", 4, 2, "99.99");
        param(builder, "-1234.56", 4, 2, "-99.99");

        param(builder, "1234", 6, 2, "1234.00");
        param(builder, "12.3456", 4, 2, "12.35");
        param(builder, "1234.56", 6, 0, "1235");
        param(builder, "12.3Q", 4, 2, "12.30");

        return builder.asList();
    }

    private static void param(ParameterizationBuilder builder, 
                              String input, int precision, int scale, String expected) {
        builder.add(String.format("%s[%d,%d]", input, precision, scale),
                    input, precision, scale, expected);
    }

    public DecimalParserTest(String input, int precision, int scale, String expected) {
        this.input = input;
        this.precision = precision;
        this.scale = scale;
        this.expected = expected;
    }

    private String input, expected;
    private int precision, scale;

    @Test
    public void checkParse() {
        ValueSource source = new Value(MString.varcharFor(input), input);
        Value target = new Value(MNumeric.DECIMAL.instance(precision, scale, true));
        TExecutionContext context = 
            new TExecutionContext(null, Arrays.asList(source.getType()), target.getType(), null,
                                  ErrorHandlingMode.IGNORE, ErrorHandlingMode.IGNORE, ErrorHandlingMode.IGNORE);
        MParsers.DECIMAL.parse(context, source, target);
        BigDecimal actual = ((BigDecimalWrapper)target.getObject()).asBigDecimal();
        assertEquals(input, new BigDecimal(expected), actual);
    }
}
