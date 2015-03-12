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
package com.foundationdb.server.store;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.tuple.Tuple2;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(NamedParameterizedRunner.class)
public final class FDBHolderImplTest
{
    private static Parameterization c(String dirString, Tuple2 expected) {
        String name = String.valueOf(dirString);
        if(name.isEmpty()) {
            name = "empty";
        }
        return Parameterization.create(name, dirString, expected);
    }

    @TestParameters
    public static Collection<Parameterization> types() throws Exception {
        return Arrays.asList(
            c(null, null),
            c("", Tuple2.from()),
            c("sql", Tuple2.from("sql")),
            c("sql/", Tuple2.from("sql")),
            c("  pre", Tuple2.from("pre")),
            c("post  ", Tuple2.from("post")),
            c("  pre/post  ", Tuple2.from("pre", "post")),
            c("foo/bar/zap", Tuple2.from("foo", "bar", "zap")),
            c("alpha\\beta\\gamma", Tuple2.from("alpha", "beta", "gamma")),
            c("a\\b/c\\\\d//e", Tuple2.from("a", "b", "c", "d", "e"))
        );
    }


    private final String dirString;
    private final Tuple2 expected;

    public FDBHolderImplTest(String dirString, Tuple2 expected) {
        this.dirString = dirString;
        this.expected = expected;
    }

    @Test
    public void doCompare() {
        try {
            Tuple2 actual = Tuple2.fromList(FDBHolderImpl.parseDirString(dirString));
            if(expected.size() != actual.size()) {
                fail(String.format("Tuple size mismatch: [%s] vs [%s]", expected.getItems(), actual.getItems()));
            }
            for(int i = 0; i < expected.size(); ++i) {
                Object e = expected.get(i);
                Object a = actual.get(i);
                assertEquals(String.format("tuple(%d)", i), e, a);
            }
        } catch(IllegalArgumentException e) {
            if(dirString != null) {
                throw e;
            }
            // else: expected
        }
    }
}
