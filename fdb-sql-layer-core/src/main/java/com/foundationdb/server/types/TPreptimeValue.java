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

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;

import java.util.Objects;

public final class TPreptimeValue {

    public void type(TInstance type) {
        assert mutable : "not mutable";
        this.type = type;
    }

    public boolean isNullable() {
        return type == null || type.nullability();
    }

    public TInstance type() {
        return type;
    }

    public void value(ValueSource value) {
        assert mutable : "not mutable";
        this.value = value;
    }

    public ValueSource value() {
        return value;
    }

    public TPreptimeValue() {
        this.mutable = true;
    }

    public TPreptimeValue(TInstance type) {
        this(type, null);
    }

    public TPreptimeValue(ValueSource value) {
        this(value.getType(), value);
    }

    public TPreptimeValue(TInstance type, ValueSource value) {
        this.type = type;
        this.value = value;
        this.mutable = false;
        //if (type == null)
        //    ArgumentValidation.isNull("value", value);
    }

    @Override
    public String toString() {
        if (type == null)
            return "<unknown>";
        String result = type.toString();
        if (value != null)
            result = result + '=' + value;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TPreptimeValue that = (TPreptimeValue) o;
        
        if (!Objects.deepEquals(type, that.type))
            return false;
        if (value == null)
            return that.value == null;
        return that.value != null && TClass.areEqual(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        AkCollator collator;
        if (type != null && type.typeClass() instanceof TString) {
            collator = AkCollatorFactory.getAkCollator(type.attribute(StringAttribute.COLLATION));
        }
        else {
            collator = null;
        }
        result = 31 * result + (value != null ? ValueSources.hash(value, collator) : 0);
        return result;
    }

    private TInstance type;
    private ValueSource value;
    private boolean mutable; // TODO ugh! should we next this, or create a hierarchy of TPV, MutableTPV?
}
