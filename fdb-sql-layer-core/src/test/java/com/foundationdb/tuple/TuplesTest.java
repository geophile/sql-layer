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

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.UUID;



public class TuplesTest {

    @Test
    public void tuplesTest() {
        
        Tuple2 t = new Tuple2();
        t = t.add(Long.MAX_VALUE);
        t = t.add(1);
        t = t.add(0);
        t = t.add(-1);
        t = t.add(Long.MIN_VALUE);
        t = t.add("foo");
        t = t.add(4.5);
        t = t.add((Float) (float) 4.5);
        t = t.add((Float) (float) -4.5);
        t = t.add(new Boolean(true));
        t = t.add(new BigInteger("123456789123456789"));
        t = t.add(new BigDecimal("123456789.123456789"));
        t = t.add(new BigDecimal("-12345678912345.1234567891234"));
        t = t.add(new Boolean(false));
        UUID uuid = UUID.randomUUID(); 
        t = t.add(uuid);
        byte[] bytes = t.pack();
        List<Object> items = Tuple2.fromBytes(bytes).getItems();
        
        assertEquals((Long) items.get(0), (Long) Long.MAX_VALUE);
        assertEquals((Long) items.get(1), (Long) ((long) 1));
        assertEquals((String) items.get(5), "foo");
        assertEquals((Float) items.get(8), (Float) ((float) -4.5));
        assertEquals((Boolean) items.get(9), new Boolean(true));
        assertEquals((BigInteger) items.get(10), new BigInteger("123456789123456789"));
        assertEquals((BigDecimal) items.get(12), new BigDecimal("-12345678912345.1234567891234"));
        assertEquals((Boolean) items.get(13), new Boolean(false));
        assertEquals((UUID) items.get(14), uuid);
    }

    @Test
    public void compareIntsAndBigInts() {
        // negative ints should always be less than positive BigInts
        Tuple2 tInt = new Tuple2();
        tInt = tInt.add(-1);
        Tuple2 tBigInt = new Tuple2();
        tBigInt = tBigInt.add(new BigInteger("1"));
        assertEquals(-1, tInt.compareTo(tBigInt));

        // positive ints should always be greater than negative BigInts
        tInt = new Tuple2();
        tInt = tInt.add(1);
        tBigInt = new Tuple2();
        tBigInt = tBigInt.add(new BigInteger("-1"));
        assertEquals(1, tInt.compareTo(tBigInt));
    }

    @Test
    public void bigDecOrdering() {
        Tuple2 t1 = new Tuple2();
        t1 = t1.add(new BigDecimal("-1.29"));
        Tuple2 t2 = new Tuple2();
        t2 = t2.add(new BigDecimal("-1.28"));
        assertEquals(-1, t1.compareTo(t2));

        t1 = new Tuple2();
        t1 = t1.add(new BigDecimal("1.28"));
        t2 = new Tuple2();
        t2 = t2.add(new BigDecimal("1.27"));
        assertEquals(1, t1.compareTo(t2));

        t1 = new Tuple2();
        t1 = t1.add(new BigDecimal("1.27"));
        t2 = new Tuple2();
        t2 = t2.add(new BigDecimal("-1.29"));
        assertEquals(1, t1.compareTo(t2));

        t1 = new Tuple2();
        t1 = t1.add(new BigDecimal("1.28"));
        t2 = new Tuple2();
        t2 = t2.add(new BigDecimal("-1.27"));
        assertEquals(1, t1.compareTo(t2));
    }

    @Test
    public void bigIntOrdering() {
        Tuple2 t1 = new Tuple2();
        t1 = t1.add(new BigInteger("-129"));
        Tuple2 t2 = new Tuple2();
        t2 = t2.add(new BigInteger("-128"));
        assertEquals(-1, t1.compareTo(t2));

        t1 = new Tuple2();
        t1 = t1.add(new BigInteger("128"));
        t2 = new Tuple2();
        t2 = t2.add(new BigInteger("127"));
        assertEquals(1, t1.compareTo(t2));

        t1 = new Tuple2();
        t1 = t1.add(new BigInteger("127"));
        t2 = new Tuple2();
        t2 = t2.add(new BigInteger("-129"));
        assertEquals(1, t1.compareTo(t2));

        t1 = new Tuple2();
        t1 = t1.add(new BigInteger("128"));
        t2 = new Tuple2();
        t2 = t2.add(new BigInteger("-128"));
        assertEquals(1, t1.compareTo(t2));

        byte[] bytes = new byte[255];
        bytes[0] = Byte.MAX_VALUE;
        for (int i = 1; i < 255; i++) {
            bytes[i] = (byte) 0xff;
        }
        BigInteger bigInteger = new BigInteger(bytes);
        
        bytes = new byte[255];
        bytes[0] = Byte.MIN_VALUE;
        for (int i = 1; i < 255; i++) {
            bytes[i] = (byte) 0xff;
        }
        BigInteger negBigInteger = new BigInteger(bytes);
        
        t1 = new Tuple2();
        t1 = t1.add(bigInteger);
        
        t2 = new Tuple2();
        t2 = t2.add(negBigInteger);
        assertEquals(1, t1.compareTo(t2));
    }

    @Test
    public void bigBigIntegers() {
        byte[] bytes = new byte[255];
        bytes[0] = Byte.MAX_VALUE;
        for (int i = 1; i < 255; i++) {
            bytes[i] = (byte) 0xff;
        }
        BigInteger d87 = new BigInteger(bytes);
        BigInteger d86 = d87.subtract(BigInteger.ONE);
        BigInteger d88 = d87.add(BigInteger.ONE);

        Tuple2 t86 = Tuple2.from(d86);
        Tuple2 t87 = Tuple2.from(d87);
        Tuple2 t88 = Tuple2.from(d88);

        assertEquals(1, t87.compareTo(t86));
        assertEquals(-1, t87.compareTo(t88));
    }

    @Test
    public void bigBigDecimals() {
        // ugly way to do the same check as in bigBigIntegers()
        // BigDecimal constructor can't use a byte array
        BigDecimal d87 = new BigDecimal("6.311915248302931113420874353255849992"
                + "274238802678805475025458091313409206810134940077578400688069"
                + "035876702726742558206932445226396580258026384404762978180296"
                + "998218235800975799169960498122978927108605007496888196929060"
                + "980203636671125359002800483627045035477705475840828688979666"
                + "316614415743662577953892653422248893240169598129040034138000"
                + "892479464096881899672276968321417838091053263371155107472381"
                + "418784593110535860101262081515155927959433915215703847190084"
                + "626412349047985295082072211944746431041274115171590347784511"
                + "315438671341475195046526469759060436979598359792076802657157"
                + "2887653525297164440538776584100773887");

        BigDecimal d88 = new BigDecimal("6.311915248302931113420874353255849992"
                + "274238802678805475025458091313409206810134940077578400688069"
                + "035876702726742558206932445226396580258026384404762978180296"
                + "998218235800975799169960498122978927108605007496888196929060"
                + "980203636671125359002800483627045035477705475840828688979666"
                + "316614415743662577953892653422248893240169598129040034138000"
                + "892479464096881899672276968321417838091053263371155107472381"
                + "418784593110535860101262081515155927959433915215703847190084"
                + "626412349047985295082072211944746431041274115171590347784511"
                + "315438671341475195046526469759060436979598359792076802657157"
                + "2887653525297164440538776584100773888");

        BigDecimal d86 = new BigDecimal("6.311915248302931113420874353255849992"
                + "274238802678805475025458091313409206810134940077578400688069"
                + "035876702726742558206932445226396580258026384404762978180296"
                + "998218235800975799169960498122978927108605007496888196929060"
                + "980203636671125359002800483627045035477705475840828688979666"
                + "316614415743662577953892653422248893240169598129040034138000"
                + "892479464096881899672276968321417838091053263371155107472381"
                + "418784593110535860101262081515155927959433915215703847190084"
                + "626412349047985295082072211944746431041274115171590347784511"
                + "315438671341475195046526469759060436979598359792076802657157"
                + "2887653525297164440538776584100773886");

        Tuple2 t86 = Tuple2.from(d86);
        Tuple2 t87 = Tuple2.from(d87);
        Tuple2 t88 = Tuple2.from(d88);

        assertEquals(1, t87.compareTo(t86));
        assertEquals(-1, t87.compareTo(t88));
    }
}
