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

import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.types.*;
import com.foundationdb.server.types.aksql.AkBundle;
import com.foundationdb.server.types.aksql.AkCategory;
import com.foundationdb.server.types.aksql.AkParsers;
import com.foundationdb.server.types.common.TFormatter;
import com.foundationdb.server.types.common.types.NoAttrTClass;
import com.foundationdb.server.types.value.*;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.server.AkServerUtil;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import static com.foundationdb.sql.types.TypeId.getUserDefinedTypeId;


public class AkGUID extends NoAttrTClass
    {
        public final static TypeId GUIDTYPE = TypeId.GUID_ID;
        public final static NoAttrTClass INSTANCE = new AkGUID();
        public final static ValueCacher CACHER = new GuidCacher();
        
        private AkGUID(){
           super(AkBundle.INSTANCE.id(), GUIDTYPE.getSQLTypeName(), AkCategory.STRING_BINARY, TFormatter.FORMAT.GUID, 1,
                   1, 16, UnderlyingType.BYTES,
                   AkParsers.GUID, GUIDTYPE.getMaximumMaximumWidth(), GUIDTYPE);
        }
        
        @Override
        public ValueCacher cacher() {
            return CACHER;
        }

        private static class GuidCacher implements ValueCacher {
            
            @Override
            public void cacheToValue(Object bdw, TInstance type, BasicValueTarget target) {
                if (bdw instanceof UUID) {
                    byte[] bb = uuidToBytes((UUID)bdw);
                    target.putBytes(bb);                    
                } else {
                    throw new InvalidParameterValueException("cannot perform UUID cast on Object");
                }
            }

            @Override
            public Object valueToCache(BasicValueSource value, TInstance type) {
                byte[] bb = value.getBytes();
                return bytesToUUID(bb,0);
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
                UUID guid = (UUID)in.getObject();
                out.putBytes(uuidToBytes(guid));
            }

            @Override
            public void readCollating(ValueSource in, TInstance typeInstance, ValueTarget out) {
                byte[] bb = in.getBytes();
                out.putObject(bytesToUUID(bb, 0));
            }

            @Override
            public void copyCanonical(ValueSource in, TInstance typeInstance, ValueTarget out) {
                copy(in, typeInstance, out);
            }
        };
        
        
        public static byte[] uuidToBytes(UUID guid) {
            ByteBuffer bb = ByteBuffer.allocate(16);
            bb.putLong(0, guid.getMostSignificantBits());
            bb.putLong(8, guid.getLeastSignificantBits());
            return bb.array();
        }

        public static UUID bytesToUUID(byte[] byteAr, int offset) {
            ByteBuffer bb = ByteBuffer.allocate(16);
            bb.put(Arrays.copyOfRange(byteAr, offset, offset+16));
            return new UUID(bb.getLong(0), bb.getLong(8));
        }
    }
        
