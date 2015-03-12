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
package com.foundationdb.tuple;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.Test;

import com.foundationdb.tuple.TupleUtil.DecodeResult;

import static org.junit.Assert.*;

public class TupleUtilsTest {

    @Test
    public void doubleEncodingTest() {
        Double initDouble = 4.5;
        byte[] bytes = TupleFloatingUtil.encode(initDouble);
        DecodeResult result = TupleFloatingUtil.decodeDouble(bytes, 1);
        assertEquals(initDouble, (Double) result.o);
        
        initDouble = -4.5;
        bytes = TupleFloatingUtil.encode(initDouble);
        result = TupleFloatingUtil.decodeDouble(bytes, 1);
        assertEquals(initDouble, (Double) result.o);
        
        initDouble = 0.0;
        bytes = TupleFloatingUtil.encode(initDouble);
        result = TupleFloatingUtil.decodeDouble(bytes, 1);
        assertEquals(initDouble, (Double) result.o);
    }
    
    @Test
    public void floatEncodingTest() {
        Float initFloat = (float) 4.5;
        byte[] bytes = TupleFloatingUtil.encode(initFloat);
        DecodeResult result = TupleFloatingUtil.decodeFloat(bytes, 1);
        assertEquals(initFloat, (Float) result.o);
        
        initFloat = (float) -4.5;
        bytes = TupleFloatingUtil.encode(initFloat);
        result = TupleFloatingUtil.decodeFloat(bytes, 1);
        assertEquals(initFloat, (Float) result.o);
        
        initFloat = (float) 0.0;
        bytes = TupleFloatingUtil.encode(initFloat);
        result = TupleFloatingUtil.decodeFloat(bytes, 1);
        assertEquals(initFloat, (Float) result.o);
        
        bytes = TupleFloatingUtil.floatingPointToByteArray((float) -42);
        bytes = TupleFloatingUtil.floatingPointCoding(bytes, true);
        assertEquals(ByteArrayUtil.printable(bytes), "=\\xd7\\xff\\xff");
    }
    
    @Test
    public void bigIntEncodingTest() {
        BigInteger bigInteger = new BigInteger("12345678912345");
        byte[] bytes = TupleFloatingUtil.encode(bigInteger);
        DecodeResult result = TupleFloatingUtil.decodeBigInt(bytes, 1);
        assertEquals(bigInteger, (BigInteger) result.o);
        
        bigInteger = new BigInteger("-12345678912345");
        bytes = TupleFloatingUtil.encode(bigInteger);
        result = TupleFloatingUtil.decodeBigInt(bytes, 1);
        assertEquals(bigInteger, (BigInteger) result.o);
    }
    
    @Test
    public void bigDecEncodingTest() {
        BigDecimal bigDecimal = new BigDecimal("123456789.123456789");
        byte[] bytes = TupleFloatingUtil.encode(bigDecimal);
        DecodeResult result = TupleFloatingUtil.decodeBigDecimal(bytes, 1);
        assertEquals(bigDecimal, (BigDecimal) result.o);
        
        bigDecimal = new BigDecimal("-123456789.123456789");
        bytes = TupleFloatingUtil.encode(bigDecimal);
        result = TupleFloatingUtil.decodeBigDecimal(bytes, 1);
        assertEquals(bigDecimal, (BigDecimal) result.o);
    }

    @Test
    public void booleanEncodingTest() {
        Boolean bool = new Boolean(true);
        byte[] bytes = TupleFloatingUtil.encode(bool);
        DecodeResult result = TupleFloatingUtil.decode(bytes, 0, 1);
        assertEquals(bool, (Boolean) result.o);

        bool = new Boolean(false);
        bytes = TupleFloatingUtil.encode(bool);
        result = TupleFloatingUtil.decode(bytes, 0, 1);
        assertEquals(bool, (Boolean) result.o);
    }
}
