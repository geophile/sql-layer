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

import com.foundationdb.server.service.blob.LobService;
import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.aksql.aktypes.AkBlob;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.util.List;


public class UnwrapBlob extends TScalarBase {
    TBinary binaryType;
    
    public static TScalar unwrapBlob(final TBinary binaryType) {
        return new UnwrapBlob(binaryType);
    }


    private UnwrapBlob(final TBinary binaryType) {
        this.binaryType = binaryType;
    }

    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(AkBlob.INSTANCE, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        
        byte[] data = new byte[0];
        BlobRef blob;
        if (inputs.get(0).hasAnyValue()) {
            Object o = inputs.get(0).getObject();
            if (o instanceof BlobRef) {
                blob = (BlobRef) o;
                String mode = context.getQueryContext().getStore().getConfig().getProperty(AkBlob.RETURN_UNWRAPPED);
                if (mode.equalsIgnoreCase(AkBlob.UNWRAPPED)) {
                    data = blob.getBytes();
                } else {
                    if (blob.isShortLob()) {
                        data = blob.getBytes();
                    } else {
                        LobService ls = context.getQueryContext().getServiceManager().getServiceByClass(LobService.class);
                        data = ls.readBlob(context.getQueryContext().getSession(), blob.getId());
                    }
                }
            }
        }
        output.putBytes(data);
    }

    @Override
    public String displayName() {
        return "UNWRAP_BLOB";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {

            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                TPreptimeValue preptimeValue = inputs.get(0);
                return binaryType.instance(Integer.MAX_VALUE, preptimeValue.isNullable());
            }
        });
    }

}
