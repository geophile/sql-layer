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


import com.foundationdb.server.error.InvalidArgumentTypeException;
import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.server.service.blob.LobService;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.aksql.aktypes.AkBlob;
import com.foundationdb.server.types.common.types.NoAttrTClass;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;


public class BlobSize extends TScalarBase {
    private NoAttrTClass blobClass;

    public static TScalar blobSize(final NoAttrTClass blob) {
        return new BlobSize(blob);
    }


    private BlobSize(NoAttrTClass blobClass) {
        this.blobClass = blobClass;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(this.blobClass, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        long size = 0L;
        BlobRef blobRef;
        if (inputs.get(0).hasAnyValue()) {
            Object o = inputs.get(0).getObject();
            if (o instanceof BlobRef) {
                blobRef = (BlobRef) o;
                String mode = context.getQueryContext().getStore().getConfig().getProperty(AkBlob.RETURN_UNWRAPPED);
                if (mode.equalsIgnoreCase(AkBlob.UNWRAPPED)) {
                    byte[] content = blobRef.getBytes();
                    if (content != null) {
                        size = (long)content.length;
                    }
                }
                else {
                    if (blobRef.isLongLob()) {
                        LobService lobService = context.getQueryContext().getServiceManager().getServiceByClass(LobService.class);
                        size = lobService.sizeBlob(context.getQueryContext().getSession(), blobRef.getId());
                    } else {
                        size = (long) (blobRef.getBytes().length);
                    }
                }
            } else {
                throw new InvalidArgumentTypeException("Should be a blob column");
            }
        }
        output.putInt64(size);
    }

    @Override
    public String displayName() {
        return "BLOB_SIZE";
    }

    @Override
    public String[] registeredNames() {
        return new String[] {displayName(), "octet_length"};
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MNumeric.BIGINT);
    }
}



