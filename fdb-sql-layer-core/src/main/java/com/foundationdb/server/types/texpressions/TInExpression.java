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

import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TComparison;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TKeyComparable;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.util.ArrayList;
import java.util.List;

public final class TInExpression {

    public static TPreparedExpression prepare(TPreparedExpression lhs, List<? extends TPreparedExpression> rhs,
                                              QueryContext queryContext) {
        return prepare(lhs, rhs, null, null, queryContext);
    }

    /**
     * @param rhsInstance May be null if comparable is null
     * @param comparable If this is not null, rhsInstance must be assigned
     */
    public static TPreparedExpression prepare(TPreparedExpression lhs, List<? extends TPreparedExpression> rhs,
                                              TInstance rhsInstance, TKeyComparable comparable,
                                              QueryContext queryContext) {
        List<TPreparedExpression> all = new ArrayList<>(rhs.size() + 1);
        boolean nullable = lhs.resultType().nullability();
        all.add(lhs);
        for (TPreparedExpression r : rhs) {
            all.add(r);
            nullable |= r.resultType().nullability();
        }
        TValidatedScalar overload;        
        if (comparable == null)
            overload = noKey;
        else {
            TInstance lhsInstance = lhs.resultType();
            boolean reverse;
            TClass leftIn = lhsInstance.typeClass();
            TClass rightIn = rhsInstance.typeClass();
            TClass leftCmp = comparable.getLeftTClass();
            TClass rightCmp = comparable.getRightTClass();
            if (leftIn == leftCmp && rightIn == rightCmp) {
                reverse = false;
            }
            else if (rightIn == leftCmp && leftIn == rightCmp) {
                reverse = true;
            }
            else {
                throw new IllegalArgumentException("invalid comparisons: " + lhsInstance + " and " + rhsInstance + " against " + comparable);
            }
            overload = new TValidatedScalar(reverse ?
                                            new InKeyReversedScalar(comparable.getComparison()) :
                                            new InKeyScalar(comparable.getComparison()));
        }
        return new TPreparedFunction(overload, AkBool.INSTANCE.instance(nullable), all);
    }
    
    static abstract class InScalarBase extends TScalarBase {
        protected abstract int doCompare(TInstance lhsInstance, ValueSource lhsSource,
                                         TInstance rhsInstance, ValueSource rhsSource);

        @Override
        protected void buildInputSets(TInputSetBuilder builder) {
            builder.vararg(null, 0, 1);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
            ValueSource lhsSource = inputs.get(0);
            TInstance lhsInstance = lhsSource.getType();
            for (int i=1, nInputs = inputs.size(); i < nInputs; ++i) {
                ValueSource rhsSource = inputs.get(i);
                TInstance rhsInstance = rhsSource.getType();
                if (0 == doCompare(lhsInstance, lhsSource, rhsInstance, rhsSource)) {
                    output.putBool(true);
                    return;
                }
            }
            output.putBool(false);
        }

        @Override
        public String displayName() {
            return "in";
        }

        @Override
        public TOverloadResult resultType() {
            return TOverloadResult.fixed(AkBool.INSTANCE);
        }

        @Override
        protected boolean nullContaminates(int inputIndex) {
            return (inputIndex == 0);
        }
    }

    private static final TValidatedScalar noKey = new TValidatedScalar(new InScalarBase() {
        @Override
        protected int doCompare(TInstance lhsInstance, ValueSource lhsSource,
                                TInstance rhsInstance, ValueSource rhsSource) {
            return TClass.compare(lhsInstance, lhsSource, rhsInstance, rhsSource);
        }
    });

    static class InKeyScalar extends InScalarBase {
        protected final TComparison comparison;

        InKeyScalar(TComparison comparison) {
            this.comparison = comparison;
        }
        
        @Override
        protected int doCompare(TInstance lhsInstance, ValueSource lhsSource,
                                TInstance rhsInstance, ValueSource rhsSource) {
            return comparison.compare(lhsInstance, lhsSource, rhsInstance, rhsSource);
        }
    }

    static class InKeyReversedScalar extends InKeyScalar {
        InKeyReversedScalar(TComparison comparison) {
            super(comparison);
        }
        
        @Override
        protected int doCompare(TInstance lhsInstance, ValueSource lhsSource,
                                TInstance rhsInstance, ValueSource rhsSource) {
            return comparison.compare(rhsInstance, rhsSource, lhsInstance, lhsSource);
        }
    }
}
