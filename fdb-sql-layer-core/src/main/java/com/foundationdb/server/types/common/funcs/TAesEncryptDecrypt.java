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

import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.persistit.exception.InvalidKeyException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.Arrays;
import java.util.List;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public class TAesEncryptDecrypt extends TScalarBase
{
    private static final int ENCRYPT_RATIO = 4;
    private static final int DECRYPT_RATIO = 3;

    public static TScalar[] create (TClass stringType, TClass varbinType, int keyLength)
    {
        return new TScalar[]
        {
            new TAesEncryptDecrypt(stringType, varbinType, "AES_ENCRYPT",
                                   Cipher.ENCRYPT_MODE, ENCRYPT_RATIO,  keyLength),
            new TAesEncryptDecrypt(stringType, varbinType, "AES_DECRYPT",
                                   Cipher.DECRYPT_MODE, DECRYPT_RATIO, keyLength)
        };
    }

    private final TClass stringType;
    private final TClass varbinType;
    private final String name;
    private final int mode;
    private final int ratio;
    private final int keyLength;
    
    private TAesEncryptDecrypt(TClass stringType, TClass varbinType, String name, 
            int mode, int ratio, int len)
    {
        this.stringType = stringType;
        this.varbinType = varbinType;
        this.name = name;
        this.mode = mode;
        this.ratio = ratio;
        keyLength = len;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(varbinType, 0).covers(stringType, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        try
        {
            output.putBytes(aes_decrypt_encrypt(inputs.get(0).getBytes(),
                                                inputs.get(1).getString(),
                                                keyLength,
                                                mode));
        }
        catch (Exception e)
        {
            context.warnClient(new InvalidParameterValueException(e.getMessage()));
            output.putNull();
        }
    }

    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                ValueSource text = inputs.get(0).value();
                
                // if input is not literal
                // the return type is same as its type
                if (text == null)
                    return inputs.get(0).type();
                int len = text.isNull() ? 0 : (text.getBytes().length * ratio);
                return varbinType.instance(len, anyContaminatingNulls(inputs));
            }   
        });
    }
    
    private static byte[] aes_decrypt_encrypt(byte text[], String key, int keyLength, int mode)
            throws NoSuchAlgorithmException, NoSuchPaddingException, 
                    IllegalBlockSizeException, BadPaddingException, 
                    UnsupportedEncodingException, NoSuchProviderException, 
                    InvalidKeyException, 
                    java.security.InvalidKeyException
    {
        SecretKey sKey = new SecretKeySpec(adjustKey(key, keyLength), "AES");
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        cipher.init(mode, sKey);
        
        switch(mode)
        {
            case Cipher.ENCRYPT_MODE:
            case Cipher.DECRYPT_MODE:
                return cipher.doFinal(text); // TODO: does VARBINARY have byteLength and offset?
            default:
                throw new IllegalArgumentException("Unexpected MODE: " + mode);
        }
    }
    
    /**
     * adjust the key into a byte array of [length] bytes.
     * If key.length() is >=  length, then it wraps around
     *
     * (This is MySQL's compatible)
     * @param key
     * @return the key in byte array
     * @throws UnsupportedEncodingException 
     */
    private static byte[] adjustKey(String key, int length) throws UnsupportedEncodingException
    {
        byte keyBytes[] = new byte[length];
        Arrays.fill(keyBytes, (byte) 0);
        byte realKey[] = key.getBytes();

        int n = 0;
        for (byte b : realKey)
            keyBytes[n++ % length] ^= b;

        return keyBytes;
    }
}
