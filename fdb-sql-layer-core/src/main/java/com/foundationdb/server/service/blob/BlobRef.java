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
package com.foundationdb.server.service.blob;

import com.foundationdb.server.error.LobContentException;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;

import java.util.UUID;
import java.util.Arrays;

public class BlobRef {
    public enum LeadingBitState { NO, YES, UNKNOWN }
    public enum LobType { SHORT_LOB, LONG_LOB, UNKNOWN }
    public static final byte SHORT_LOB = 0x01;
    public static final byte LONG_LOB = 0x02;
    private UUID id;
    private byte[] data;
    private byte storeTypeBit;
    private LeadingBitState leadingBitState = LeadingBitState.UNKNOWN;
    private LobType lobType = LobType.UNKNOWN;
    private LobType requestedType = LobType.UNKNOWN;
    private Boolean returnedBlobInUnwrappedMode = false;
    
    
    public BlobRef(byte[] value) {
        this(value, LeadingBitState.UNKNOWN);
    }
    public BlobRef(byte[] value, LeadingBitState state) {
        this(value, state, LobType.UNKNOWN, LobType.UNKNOWN);
    }
    

    public BlobRef(byte[] value, LeadingBitState state, LobType definedType, LobType requestedType) {
        this.leadingBitState = state;
        this.lobType = definedType;
        this.requestedType = requestedType;
        
        if (leadingBitState == LeadingBitState.YES) {
            storeTypeBit = value[0];
            if (storeTypeBit == SHORT_LOB) {
                lobType = LobType.SHORT_LOB;
            } else if (storeTypeBit == LONG_LOB) {
                lobType = LobType.LONG_LOB;
            } else {
                throw new LobContentException("Invalid leading bit -");                
            }
            
            if (isShortLob()) {
                data = Arrays.copyOfRange(value, 1, value.length);
            } else if (isLongLob()) {
                if (value.length != 17){
                    throw new LobContentException("invalid id length");
                }
                id = AkGUID.bytesToUUID(value, 1);
            } else {
                throw new LobContentException("Invalid store type");
            }
        } else {
            data = value;
        }
    }
    
    public byte[] getValue() {
        // always returns data with the correct leading bit if applicable
        byte[] res;
        if (leadingBitState == LeadingBitState.YES) {
            if (isShortLob()) {
                res = new byte[data.length + 1];
                System.arraycopy(data, 0, res, 1, data.length);
                res[0] = storeTypeBit;
            }
            else {
                res = new byte[17];
                System.arraycopy(AkGUID.uuidToBytes(id), 0, res, 1, 16);
                res[0] = storeTypeBit;
            }
        } else {
            res = data;
        }
        return res;
    }
    
    public boolean isShortLob() {
        return lobType == LobType.SHORT_LOB;
    }

    public boolean isLongLob() {
        return lobType == LobType.LONG_LOB;
    }
    
    public UUID getId() {
        return id;
    }
    
    public void setId(UUID id) {
        this.id = id;
    }
    
    public byte[] getBytes() {
        return data;
    }
    
    public LobType getLobType() {
        return lobType;
    }
    
    public void setLobType(LobType lobType) {
        this.lobType = lobType;
    }
    
    public LobType getRequestedLobType() { return requestedType; }
    
    public Boolean isReturnedBlobInUnwrappedMode() {
        return returnedBlobInUnwrappedMode;
    }
    
    public void setIsReturnedBlobInUnwrappedMode(Boolean value) {
        returnedBlobInUnwrappedMode = value;
    }
}
