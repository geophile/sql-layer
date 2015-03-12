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
import com.foundationdb.server.types.aksql.AkParsers;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class BooleanParserTest {

    @TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder builder = new ParameterizationBuilder();

        param(builder, "1", true);
        param(builder, "1-1", true);
        param(builder, "1.1", true);
        param(builder, "1.1.1.1.1", true);
        param(builder, ".1", true); // this is weird. ".1" as tinyint is 0, and booleans in mysql are tinyint. but
                                    // (false OR ".1") results in a tinyint 1 (ie, true). Gotta love MySQL.
        param(builder, "0.1", true);
        param(builder, "-1", true);
        param(builder, "-1.1-a", true);
        param(builder, ".-1", false);
        param(builder, "-.1", true);
        param(builder, "-..1", false);
        param(builder, "1a", true);
        param(builder, "a1", false); // MySQL doesn't believe in steak sauce
        param(builder, "0", false);
        param(builder, "0.0", false);

        param(builder, "false", false);
        param(builder, "f", false);
        // Following are not MySQL compatible, but required for ActiveRecord.
        param(builder, "true", true);
        param(builder, "t", true);

        return builder.asList();
    }

    private static void param(ParameterizationBuilder builder, String string, boolean expected) {
        builder.add(string, string, expected);
    }

    public BooleanParserTest(String string, boolean expected) {
        this.string = string;
        this.expected = expected;
    }

    private String string;
    private boolean expected;

    @Test
    public void checkParse() {
        ValueSource source = new Value(MString.varcharFor(string), string);
        Value target = new Value(AkBool.INSTANCE.instance(true));
        AkParsers.BOOLEAN.parse(null, source, target);
        Boolean actual = target.isNull() ? null : target.getBoolean();
        assertEquals(string, Boolean.valueOf(expected), actual);
    }
}
