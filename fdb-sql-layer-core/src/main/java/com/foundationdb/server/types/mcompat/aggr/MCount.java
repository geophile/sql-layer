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
package com.foundationdb.server.types.mcompat.aggr;

import com.foundationdb.server.types.TAggregator;
import com.foundationdb.server.types.TAggregatorBase;
import com.foundationdb.server.types.TInputSet;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.util.Collections;
import java.util.List;

public class MCount extends TAggregatorBase {

    public static final TAggregator[] INSTANCES = {
            new MCount("count(*)", true, true),
            new MCount("count(*)", true, false),
            new MCount("count", false, true),
            new MCount("count", false, false)
    };

    @Override
    public void input(TInstance type, ValueSource source, TInstance stateType, Value state, Object o) {
        if (countIfNull || (!source.isNull())) {
            long count = state.hasAnyValue() ? state.getInt64() : 0;
            ++count;
            state.putInt64(count);
        }
    }

    @Override
    public List<TInputSet> inputSets() {
        return claimNoInputs ? Collections.<TInputSet>emptyList() : super.inputSets();
    }

    @Override
    public void emptyValue(ValueTarget state) {
        state.putInt64(0L);
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MNumeric.BIGINT);
    }

    private MCount(String name, boolean countIfNull, boolean claimNoInputs) {
        super(name, null);
        this.countIfNull = countIfNull;
        this.claimNoInputs = claimNoInputs;
    }

    private final boolean countIfNull;
    /**
     * Whether the inputSets() list should be empty. The optimizer sometimes doesn't have an operand for COUNT or
     * COUNT(*), so we get around this by creating two copies of each overload, one which says it has no inputs.
     * By assemble time, both will actually have an input.
     */
    private final boolean claimNoInputs;
}
