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
package com.foundationdb.server.types.common.funcs;

import com.foundationdb.ais.model.Sequence;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;

public class SequenceValue extends TScalarBase {

    public static TScalar[] create(TClass stringType, TClass longType) {
        return new TScalar[] {
            new SequenceValue(false, stringType, longType, 0),
            new SequenceValue(false, stringType, longType, 0, 1),
            new SequenceValue(true, stringType, longType, 0),
            new SequenceValue(true, stringType, longType, 0, 1)
        };
    }

    protected final boolean nextValue;
    protected final TClass inputType, outputType;
    protected final int[] covering;
    
    private static final Logger logger = LoggerFactory.getLogger(SequenceValue.class);

    private SequenceValue (boolean nextValue, TClass inputType, TClass outputType, 
                           int... covering) {
        this.nextValue = nextValue;
        this.inputType = inputType;
        this.outputType = outputType;
        this.covering = covering;
    }

    @Override
    public String displayName() {
        return nextValue ? "NEXTVAL" : "CURRVAL";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(outputType);
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(inputType, covering);
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return true;
    }

    @Override
    protected boolean neverConstant() {
        return true;
    }

    @Override
    protected void doEvaluate(TExecutionContext context,
            LazyList<? extends ValueSource> inputs, ValueTarget output) {
        String[] parts = { "", "" };
        if (covering.length > 1) {
            parts[0] = inputs.get(0).getString();
            parts[1] = inputs.get(1).getString();
        }
        else {
            TableName name = TableName.parse("", inputs.get(0).getString());
            parts[0] = name.getSchemaName();
            parts[1] = name.getTableName();
        }
        if (parts[0].isEmpty()) {
            parts[0] = context.getCurrentSchema();
        }

        TableName sequenceName = new TableName(parts[0], parts[1]);
        StoreAdapter store = context.getQueryContext().getStore();
        Sequence sequence = store.getSequence(sequenceName);
        long value = nextValue ?
            store.sequenceNextValue(sequence) :
            store.sequenceCurrentValue(sequence);

        logger.debug("Sequence loading : {} -> {}", sequenceName, value);

        output.putInt64(value);
    }
}
