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
package com.foundationdb.server.types.common.types;

import com.foundationdb.server.types.*;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.sql.types.TypeId;

public class NoAttrTClass extends SimpleDtdTClass {

    @Override
    public TInstance instance(boolean nullable) {
        TInstance result;
        // These two blocks are obviously racy. However, the race will not create incorrectness, it'll just
        // allow there to be multiple copies of the TInstance floating around, each of which is correct, immutable
        // and equivalent.
        if (nullable) {
            result = nullableTInstance;
            if (result == null) {
                result = createInstanceNoArgs(true);
                nullableTInstance = result;
            }
        }
        else {
            result = notNullableTInstance;
            if (result == null) {
                result = createInstanceNoArgs(false);
                notNullableTInstance = result;
            }
        }
        return result;
    }

    @Override
    public boolean attributeIsPhysical(int attributeIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean attributeAlwaysDisplayed(int attributeIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected TInstance doPickInstance(TInstance left, TInstance right, boolean suggestedNullability) {
        return right; // doesn't matter which!
    }

    @Override
    protected void validate(TInstance type) {
    }

    public TClass widestComparable()
    {
        return this;
    }
    
    public NoAttrTClass(TBundleID bundle, String name, Enum<?> category, TClassFormatter formatter, int internalRepVersion,
                           int serializationVersion, int serializationSize, UnderlyingType underlyingType, TParser parser,
                           int defaultVarcharLen, TypeId typeId) {
        super(bundle, name, category, formatter, Attribute.NONE.class, internalRepVersion, serializationVersion, serializationSize,
                underlyingType, parser, defaultVarcharLen, typeId);
    }

    private volatile TInstance nullableTInstance;
    private volatile TInstance notNullableTInstance;
}
