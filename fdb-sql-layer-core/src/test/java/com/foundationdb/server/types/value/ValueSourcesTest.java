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
package com.foundationdb.server.types.value;

import com.foundationdb.server.types.Attribute;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.BigDecimalWrapperImpl;
import com.foundationdb.server.types.common.types.DecimalAttribute;
import com.foundationdb.server.types.common.types.TBigDecimal;
import org.junit.Test;

import java.math.BigDecimal;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

import static com.foundationdb.server.types.value.ValueSources.*;

public class ValueSourcesTest
{
    private static void checkType(Class<? extends TClass> clazz,
                                  Attribute attr0, int value0,
                                  Attribute attr1, int value1,
                                  Value v) {
        TInstance type = v.getType();
        assertThat(type.typeClass(), is(instanceOf(clazz)));
        if(attr0 != null) {
            assertThat(attr0.name(), type.attribute(attr0), is(equalTo(value0)));
        }
        if(attr1 != null) {
            assertThat(attr1.name(), type.attribute(attr1), is(equalTo(value1)));
        }
    }

    private static void checkDecimal(int precision, int scale, Value v) {
        checkType(TBigDecimal.class, DecimalAttribute.PRECISION, precision, DecimalAttribute.SCALE, scale, v);
    }

    private static void checkDecimal(int precision, int scale, String decimalStr) {
        BigDecimalWrapper wrapper = new BigDecimalWrapperImpl(new BigDecimal(decimalStr));
        checkDecimal(precision, scale, fromObject(wrapper.asBigDecimal()));
        checkDecimal(precision, scale, fromObject(wrapper));
    }

    @Test
    public void fromObjectBigDecimals() {
        checkDecimal(6, 3, "-123.456");
        checkDecimal(2, 1, "-1.0");
        checkDecimal(2, 2, ".00");
        checkDecimal(1, 1, ".0");
        checkDecimal(1, 1, "0.0");
        checkDecimal(1, 0, "0.");
        checkDecimal(1, 0, "00.");
        checkDecimal(4, 4, "0.0005");
        checkDecimal(2, 1, "1.0");
        checkDecimal(6, 3, "123.456");
    }
}