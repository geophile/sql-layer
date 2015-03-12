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

import com.foundationdb.server.types.service.ReflectiveInstanceFinder;
import com.foundationdb.server.types.value.ValueSource;

import com.foundationdb.util.AkibanAppender;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;

import static com.foundationdb.server.types.value.ValueSources.valuefromObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class TypeFormattingTestBase
{
    public static Object[] tCase(TClass tClass, Object o, String str, String json, String literal) {
        return new Object[] { tClass, valuefromObject(o, tClass.instance(true)), str, json, literal };
    }

    public static Collection<Object[]> checkParams(TBundleID bundle, Collection<Object[]> params, TClass... ignore) throws Exception {
        ReflectiveInstanceFinder finder = new ReflectiveInstanceFinder();
        Collection<? extends TClass> allTypes = finder.find(TClass.class);
        for(TClass tClass : allTypes) {
            if(tClass.name().bundleId() == bundle) {
                boolean found = false;
                for(Object[] tc : params) {
                    if(tc[0] == tClass) {
                        found = true;
                        break;
                    }
                }
                if(!found && !Arrays.asList(ignore).contains(tClass)) {
                    fail("no case for " + tClass.name());
                }
            }
        }
        return params;
    }

    private final ValueSource valueSource;
    private final String str;
    private final String json;
    private final String literal;

    public TypeFormattingTestBase(TClass tClass, ValueSource valueSource, String str, String json, String literal) {
        this.valueSource = valueSource;
        this.str = str;
        this.json = json;
        this.literal = literal;
    }

    private void checkNull(TClass tClass) {
        TInstance inst = tClass.instance(true);
        ValueSource source  = valuefromObject(null, inst);
        check(source, "NULL", "null", "NULL");
    }

    private static void check(ValueSource source, String formatted, String formattedJSON, String formattedLiteral) {
        FormatOptions FORMAT_OPTS = new FormatOptions();
        FORMAT_OPTS.set(FormatOptions.BinaryFormatOption.HEX);
        FORMAT_OPTS.set(FormatOptions.JsonBinaryFormatOption.HEX);
        String typeName = source.getType().typeClass().name().toString();
        StringBuilder sb = new StringBuilder();
        AkibanAppender out = AkibanAppender.of(sb);
        sb.setLength(0);
        if (formatted != null) {
            source.getType().format(source, out);
            assertEquals(typeName + " str", formatted, sb.toString());
        }
        sb.setLength(0);
        source.getType().formatAsJson(source, out, FORMAT_OPTS);
        assertEquals(typeName + " json", formattedJSON, sb.toString());
        sb.setLength(0);
        source.getType().formatAsLiteral(source, out);
        assertEquals(typeName + " literal", formattedLiteral, sb.toString());
    }


    @Test
    public void test() {
        DateTimeZone orig = DateTimeZone.getDefault();
        DateTimeZone.setDefault(DateTimeZone.UTC);
        try {
            checkNull(valueSource.getType().typeClass());
            check(valueSource, str, json, literal);
        } finally {
            DateTimeZone.setDefault(orig);
        }
    }

    @Test
    public void testFrenchCanadianLocale() {
        Locale orig = Locale.getDefault();
        Locale.setDefault(Locale.CANADA_FRENCH);
        try {
            test();
        } finally {
            Locale.setDefault(orig);
        }
    }
}
