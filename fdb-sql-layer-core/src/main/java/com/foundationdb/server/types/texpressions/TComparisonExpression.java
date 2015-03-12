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
package com.foundationdb.server.types.texpressions;

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.ValueSource;

public final class TComparisonExpression extends TComparisonExpressionBase {

    @Override
    protected int compare(TInstance leftInstance, ValueSource leftSource,
                          TInstance rightInstance, ValueSource rightSource)
    {
        TClass tClass = leftInstance.typeClass();
        if (collator != null) {
            assert tClass.underlyingType() == UnderlyingType.STRING : tClass + ": " + tClass.underlyingType();
            String leftString = leftSource.getString();
            String rightString = rightSource.getString();
            return collator.compare(leftString, rightString);
        }
        else {
            return TClass.compare(leftInstance, leftSource, rightInstance, rightSource);
        }
    }

    public TComparisonExpression(TPreparedExpression left, Comparison comparison, TPreparedExpression right) {
        this(left, comparison, right, null);
    }

    public TComparisonExpression(TPreparedExpression left, Comparison comparison, TPreparedExpression right,
                                 AkCollator collator) {
        super(left, comparison, right);
        TClass oneClass = left.resultType().typeClass();
        TClass twoClass = right.resultType().typeClass();
        if (!oneClass.compatibleForCompare(twoClass))
            throw new IllegalArgumentException("can't compare expressions of different types: " + left + " != " + right);
        if (collator != null && oneClass.underlyingType() != UnderlyingType.STRING) {
            throw new IllegalArgumentException("collator provided, but " + oneClass + " is not a string type");
        }
        this.collator = collator;
    }

    // Collator in advance saves mergeCollations() every eval as TClass.compare() would do
    private final AkCollator collator;
}
