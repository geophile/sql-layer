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
import com.foundationdb.server.types.TFixedTypeAggregator;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

public class MGroupConcat extends TFixedTypeAggregator
{
    public static final TAggregator INSTANCE = new MGroupConcat();

    private MGroupConcat() {
        super("group_concat", MString.TEXT);
    }

    @Override
    public void input(TInstance type, ValueSource source, TInstance stateType, Value state, Object del)
    {
        // skip all NULL rows
        if (source.isNull())
            return;

        // cache a StringBuilder instead?
        state.putString((state.hasAnyValue()
                            ? state.getString() + (String)del
                            : "") 
                            + source.getString(),
                        null);
    }

    @Override
    public void emptyValue(ValueTarget state)
    {
        state.putNull();
    }
}
