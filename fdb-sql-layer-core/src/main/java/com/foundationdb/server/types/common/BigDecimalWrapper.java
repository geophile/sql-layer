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
package com.foundationdb.server.types.common;

import com.foundationdb.server.types.DeepCopiable;
import java.math.BigDecimal;

public interface BigDecimalWrapper extends Comparable<BigDecimalWrapper>, DeepCopiable<BigDecimalWrapper>
{
    
    BigDecimalWrapper set(BigDecimalWrapper value);
    BigDecimalWrapper add(BigDecimalWrapper addend);
    BigDecimalWrapper subtract(BigDecimalWrapper subtrahend);
    BigDecimalWrapper multiply(BigDecimalWrapper multiplicand);
    BigDecimalWrapper divide(BigDecimalWrapper divisor);
    BigDecimalWrapper floor();
    BigDecimalWrapper ceil();
    BigDecimalWrapper truncate(int scale);
    BigDecimalWrapper round(int scale);
    BigDecimalWrapper divideToIntegralValue(BigDecimalWrapper divisor);
    BigDecimalWrapper divide(BigDecimalWrapper divisor, int scale);
    BigDecimalWrapper parseString(String num);
    BigDecimalWrapper negate();
    BigDecimalWrapper abs();
    BigDecimalWrapper mod(BigDecimalWrapper num);
    BigDecimalWrapper deepCopy();
    
    int compareTo (BigDecimalWrapper o);
    int getScale();
    int getPrecision();
    int getSign();
    boolean isZero();
    void reset();

    BigDecimal asBigDecimal();
}
