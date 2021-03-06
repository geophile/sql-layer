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
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.aksql.aktypes.AkBlob;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.error.LobContentException;


public class CreateShortBlob extends TScalarBase {
    public static TScalar createEmptyShortBlob() {
        return new CreateShortBlob();
    }

    public static TScalar createShortBlob(final TClass binaryType) {
        return new CreateShortBlob() {
            @Override
            protected  void buildInputSets(TInputSetBuilder builder) {
                builder.covers(binaryType, 0);
            }
        };
    }


    private CreateShortBlob() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        // does nothing
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        byte[] data = new byte[0];
        if (inputs.size() == 1) {
            data = inputs.get(0).getBytes();
            if ( data.length > AkBlob.LOB_SWITCH_SIZE ) {
                throw new LobContentException("Lob size too large for small lob");
            }
        }

        String mode = context.getQueryContext().getStore().getConfig().getProperty(AkBlob.RETURN_UNWRAPPED);
        BlobRef.LeadingBitState state = BlobRef.LeadingBitState.NO;
        if (mode.equalsIgnoreCase(AkBlob.WRAPPED)) {
            state = BlobRef.LeadingBitState.YES;
            byte[] tmp = new byte[data.length + 1];
            tmp[0] = BlobRef.SHORT_LOB;
            System.arraycopy(data, 0, tmp, 1, data.length);
            data = tmp;
        }

        BlobRef blob = new BlobRef(data, state, BlobRef.LobType.UNKNOWN, BlobRef.LobType.SHORT_LOB);
        output.putObject(blob);
    }

    @Override
    public String displayName() {
        return "CREATE_SHORT_BLOB";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(AkBlob.INSTANCE);
    }

}
