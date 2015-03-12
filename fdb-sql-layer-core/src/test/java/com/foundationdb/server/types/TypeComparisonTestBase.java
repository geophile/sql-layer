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
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSources;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public abstract class TypeComparisonTestBase
{
    protected static class TypeInfo {
        public final TClass type;
        public final Object min;
        public final Object zero;
        public final Object max;

        public TypeInfo(TClass type, Object min, Object zero, Object max) {
            this.type = type;
            this.min = min;
            this.zero = zero;
            this.max = max;
        }
    }

    protected static TypeInfo typeInfo(TClass type, Object min, Object max) {
        return typeInfo(type, min, null, max);
    }

    protected static TypeInfo typeInfo(TClass type, Object min, Object zero, Object max) {
        return new TypeInfo(type, min, zero, max);
    }

    public static Collection<Object[]> makeParams(TBundleID bundle, Collection<TypeInfo> typeInfos, Collection<TClass> ignore) throws Exception {
        ReflectiveInstanceFinder finder = new ReflectiveInstanceFinder();
        for(TClass type : finder.find(TClass.class)) {
            if((type.name().bundleId() == bundle) && !ignore.contains(type)) {
                boolean found = false;
                for(TypeInfo info : typeInfos) {
                    if(info.type == type) {
                        found = true;
                        break;
                    }
                }
                if(!found) {
                    throw new AssertionError("No TypeInfo for " + type.name());
                }
            }
        }
        List<Object[]> params = new ArrayList<>();
        for(TypeInfo info : typeInfos) {
            String name = info.type.name().unqualifiedName();
            Value vnull = ValueSources.valuefromObject(null, info.type.instance(true));
            Value min = ValueSources.valuefromObject(info.min, info.type.instance(true));
            Value max = ValueSources.valuefromObject(info.max, info.type.instance(true));
            params.add(new Object[] { name + "_null_null", vnull, vnull, 0 });
            params.add(new Object[] { name + "_null_min", vnull, min, -1 });
            params.add(new Object[] { name + "_min_null", min, vnull, 1 });
            params.add(new Object[] { name + "_null_max", vnull, max, -1 });
            params.add(new Object[] { name + "_max_null", max, vnull, 1 });
            params.add(new Object[] { name + "_min_min", min, min, 0 });
            params.add(new Object[] { name + "_min_max", min, max, -1 });
            params.add(new Object[] { name + "_max_min", max, min, 1 });
            params.add(new Object[] { name + "_max_max", max, max, 0 });
            if(info.zero != null) {
                Value zero = ValueSources.valuefromObject(info.zero, info.type.instance(true));
                params.add(new Object[] { name + "_min_zero", min, zero, -1 });
                params.add(new Object[] { name + "_zero_min", zero, min, 1 });
                params.add(new Object[] { name + "_zero_zero", zero, zero, 0 });
                params.add(new Object[] { name + "_zero_max", zero, max, -1 });
                params.add(new Object[] { name + "_max_zero", max, zero, 1 });
            }
        }
        return params;
    }


    private final String name;
    private final Value a;
    private final Value b;
    private final int expected;

    public TypeComparisonTestBase(String name, Value a, Value b, int expected) throws Exception {
        this.name = name;
        this.a = a;
        this.b = b;
        this.expected = expected;
    }


    @Test
    public void testCompare() {
        String desc = String.format("%s compareTo %s ", a, b);
        int actual = TClass.compare(a,b);
        if(expected == 0) {
            assertThat(desc, actual, equalTo(0));
        } else if(expected < 0) {
            assertThat(desc, actual, lessThan(0));
        } else {
            assertThat(desc, actual, greaterThan(0));
        }
    }
}
