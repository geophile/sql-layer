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
package com.foundationdb.server.types.aksql.aktypes;

import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.ValueIO;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.aksql.AkParsers;
import com.foundationdb.server.types.aksql.AkBundle;
import com.foundationdb.server.types.aksql.AkCategory;
import com.foundationdb.server.types.common.TFormatter;
import com.foundationdb.server.types.common.types.NoAttrTClass;
import com.foundationdb.server.types.value.ValueCacher;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.BasicValueTarget;
import com.foundationdb.server.types.value.BasicValueSource;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.value.ValueTargets;
import com.foundationdb.sql.types.TypeId;


public class AkBlob extends NoAttrTClass {

    
    public final static TypeId BLOBTYPE = TypeId.BLOB_ID;
    public final static ValueCacher CACHER = new BlobCacher();
    public final static int LOB_SWITCH_SIZE = 50000;
    public final static String RETURN_UNWRAPPED = "fdbsql.blob.return_unwrapped";
    public final static String WRAPPED = "false";
    public final static String UNWRAPPED = "true";
    public final static NoAttrTClass INSTANCE = new AkBlob();
    
    private AkBlob(){
        super(AkBundle.INSTANCE.id(), BLOBTYPE.getSQLTypeName(), AkCategory.STRING_BINARY, TFormatter.FORMAT.BLOB, 1,
                1, -1, UnderlyingType.BYTES,
                AkParsers.BLOB, BLOBTYPE.getMaximumMaximumWidth(), BLOBTYPE);
    }

    
    @Override
    public ValueCacher cacher() {
        return CACHER;
    }
    
    @Override
    public int variableSerializationSize(TInstance type, boolean average) {
        return LOB_SWITCH_SIZE;
    }


    private static class BlobCacher implements ValueCacher {

        @Override
        public void cacheToValue(Object bdw, TInstance type, BasicValueTarget target) {
            if (bdw instanceof BlobRef) {
                byte[] bb = ((BlobRef) bdw).getValue();
                target.putBytes(bb);
            } else {
                throw new InvalidParameterValueException("Object is not a blob instance");
            }
        }

        @Override
        public Object valueToCache(BasicValueSource value, TInstance type) {
            byte[] bb = value.getBytes();
            return new BlobRef(bb, BlobRef.LeadingBitState.YES);
        }

        @Override
        public Object sanitize(Object object) {
            return object;
        }

        @Override
        public boolean canConvertToValue(Object cached) {
            return true;
        }
    } 

    @Override
    protected ValueIO getValueIO() {
        return valueIO;
    }

    private static final ValueIO valueIO = new ValueIO() {

        protected void copy(ValueSource in, TInstance typeInstance, ValueTarget out) {
            ValueTargets.copyFrom(in, out);
        }

        @Override
        public void writeCollating(ValueSource in, TInstance typeInstance, ValueTarget out) {
            BlobRef blob = (BlobRef)in.getObject();
            out.putBytes(blob.getValue());
        }

        @Override
        public void readCollating(ValueSource in, TInstance typeInstance, ValueTarget out) {
            byte[] bb = in.getBytes();
            out.putObject(new BlobRef(bb));
        }

        @Override
        public void copyCanonical(ValueSource in, TInstance typeInstance, ValueTarget out) {
            copy(in, typeInstance, out);
        }
    };
    
    public static boolean isBlob(TClass clazz) {
        return clazz == AkBlob.INSTANCE;
    } 
    
    public static boolean containsBlob(RowType rowType) {
        for (int i = 0; i < rowType.nFields(); i ++) {
            if (isBlob(rowType.typeAt(i).typeClass())) {
                return true;
            }
        }
        return false;
    }
}
