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
package com.foundationdb.server.types.mcompat.mcasts;

import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TCastBase;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.aksql.aktypes.AkBlob;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.common.types.NoAttrTClass;
import com.foundationdb.server.types.common.types.TBinary.Attrs;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;


public final class Cast_From_Binary {

    private Cast_From_Binary() {}

    public static final TCast BINARY_TO_VARBINARY = new BinaryToBinary(MBinary.BINARY, MBinary.VARBINARY);
    public static final TCast VARBINARY_TO_BINARY = new BinaryToBinary(MBinary.VARBINARY, MBinary.BINARY);
    public static final TCast VARBINARY_TO_AKBLOB = new BinaryToAkBlob(MBinary.VARBINARY, AkBlob.INSTANCE);
    public static final TCast BINARY_TO_AKBLOB = new BinaryToAkBlob(MBinary.BINARY, AkBlob.INSTANCE);
    
    private static class BinaryToBinary extends TCastBase {
        private BinaryToBinary(TBinary sourceClass, TBinary targetClass) {
            super(sourceClass, targetClass);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            byte[] in = source.getBytes();
            byte[] out = in;
            TInstance outputType = context.outputType();
            int maxLen = outputType.attribute(Attrs.LENGTH);
            if (in.length > maxLen) {
                out = new byte[maxLen];
                System.arraycopy(in, 0, out, 0, maxLen);
                context.reportTruncate("bytes of length " + in.length,  "bytes of length " + maxLen);
            }
            TBinary.putBytes(context, target, out);
        }

        @Override
        public TInstance preferredTarget(TPreptimeValue source) {
            TInstance sourceType =  source.type();
            return targetClass().instance(sourceType.attribute(Attrs.LENGTH),
                    source.isNullable());
        }
    }
    
    private static class  BinaryToAkBlob extends TCastBase {
        private BinaryToAkBlob(TBinary sourceClass, NoAttrTClass targetClass) {
            super(sourceClass, targetClass);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            byte[] in = source.getBytes();
            BlobRef blob = new BlobRef(in);
            target.putObject(blob);
        }

        @Override
        public TInstance preferredTarget(TPreptimeValue source) {
            return targetClass().instance(source.isNullable());
        }
    }
}
