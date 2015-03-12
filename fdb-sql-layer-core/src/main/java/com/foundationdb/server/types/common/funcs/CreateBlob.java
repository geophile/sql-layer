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


import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.server.store.*;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;
import com.foundationdb.server.types.aksql.aktypes.AkBlob;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import java.util.UUID;

public class CreateBlob extends TScalarBase {

    public static TScalar createEmptyBlob() {
        return new CreateBlob();
    }

    public static TScalar createBlob(final TClass binaryType) {
        return new CreateBlob() {
            @Override
            protected  void buildInputSets(TInputSetBuilder builder) {
                builder.covers(binaryType, 0);
            }
        };
    }


    private CreateBlob() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        // does nothing
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        BlobRef blob;
        String mode = context.getQueryContext().getStore().getConfig().getProperty(AkBlob.RETURN_UNWRAPPED);
        BlobRef.LeadingBitState state = BlobRef.LeadingBitState.NO;
        byte[] data = new byte[0];
        if (inputs.size() == 1) {
            data = inputs.get(0).getBytes();
        }
        
        if (mode.equalsIgnoreCase(AkBlob.UNWRAPPED)){
            blob = new BlobRef(data, state);
        }
        else {
            state = BlobRef.LeadingBitState.YES;
            if ((data.length < AkBlob.LOB_SWITCH_SIZE)) {
                byte[] tmp = new byte[data.length + 1];
                tmp[0] = BlobRef.SHORT_LOB;
                System.arraycopy(data, 0, tmp, 1, data.length);
                data = tmp;
            }
            else {
                UUID id = FDBStore.writeDataToNewBlob(context.getQueryContext().getSession(), data);
                byte[] tmp = new byte[17];
                tmp[0] = BlobRef.LONG_LOB;
                System.arraycopy(AkGUID.uuidToBytes(id), 0, tmp, 1, 16);
                data = tmp;
            }
            blob = new BlobRef(data, state);
        }
        output.putObject(blob);
    }

    @Override
    public String displayName() {
        return "CREATE_BLOB";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(AkBlob.INSTANCE);
    }
}

