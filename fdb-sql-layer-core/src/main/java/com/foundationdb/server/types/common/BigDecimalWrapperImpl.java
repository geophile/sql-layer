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

import java.math.BigDecimal;
import java.math.RoundingMode;

public class BigDecimalWrapperImpl implements BigDecimalWrapper {

    public static final BigDecimalWrapperImpl ZERO = new BigDecimalWrapperImpl(BigDecimal.ZERO);

    public static int sqlPrecision(BigDecimal bd) {
        int precision = bd.precision();
        int scale = bd.scale();
        if (precision < scale) {
            // BigDecimal interprets something like "0.01" as having a scale of 2 and precision of 1.
            precision = scale;
        }
        return precision;
    }

    public static int sqlScale(BigDecimal bd) {
        return bd.scale();
    }


    private BigDecimal value;

    public BigDecimalWrapperImpl(BigDecimal value) {
        this.value = value;
    }

    public BigDecimalWrapperImpl(String num)
    {
        value = new BigDecimal(num);
    }

    public BigDecimalWrapperImpl(long val)
    {
        value = BigDecimal.valueOf(val);
    }

    public BigDecimalWrapperImpl()
    {
        value = BigDecimal.ZERO;
    }

    @Override
    public void reset() {
        value = BigDecimal.ZERO;
    }
            
    @Override
    public BigDecimalWrapper set(BigDecimalWrapper other) {
        value = other.asBigDecimal();
        return this;
    }
            
    @Override
    public BigDecimalWrapper add(BigDecimalWrapper other) {
        value = value.add(other.asBigDecimal());
        return this;
    }

    @Override
    public BigDecimalWrapper subtract(BigDecimalWrapper other) {
        value = value.subtract(other.asBigDecimal());
        return this;
    }

    @Override
    public BigDecimalWrapper multiply(BigDecimalWrapper other) {
        value = value.multiply(other.asBigDecimal());
        return this;
    }

    @Override
    public BigDecimalWrapper divide(BigDecimalWrapper other) {
        value = value.divide(other.asBigDecimal());
        return this;
    }

    @Override
    public BigDecimalWrapper ceil() {
        value = value.setScale(0, RoundingMode.CEILING);
        return this;
    }
    
    @Override
    public BigDecimalWrapper floor() {
        value = value.setScale(0, RoundingMode.FLOOR);
        return this;
    }
    
    @Override
    public BigDecimalWrapper truncate(int scale) {
        value = value.setScale(scale, RoundingMode.DOWN);
        return this;
    }
    
    @Override
    public BigDecimalWrapper round(int scale) {
        value = value.setScale(scale, RoundingMode.HALF_UP);
        return this;
    }
    
    @Override
    public int getSign() {
        return value.signum();
    }
    
    @Override
    public BigDecimalWrapper divide(BigDecimalWrapper divisor, int scale)
    {
        value = value.divide(divisor.asBigDecimal(),
                scale,
                RoundingMode.HALF_UP);
        return this;
    }

    @Override
    public BigDecimalWrapper divideToIntegralValue(BigDecimalWrapper divisor)
    {
        value = value.divideToIntegralValue(divisor.asBigDecimal());
        return this;
    }

    @Override
    public BigDecimalWrapper abs()
    {
        value = value.abs();
        return this;
    }
    
    @Override
    public int getScale()
    {
        return sqlScale(value);
    }

    @Override
    public int getPrecision()
    {
        return sqlPrecision(value);
    }

    @Override
    public BigDecimalWrapper parseString(String num)
    {
        value = new BigDecimal (num);
        return this;
    }

    @Override
    public int compareTo(BigDecimalWrapper o)
    {
        return value.compareTo(o.asBigDecimal());
    }

    @Override
    public BigDecimalWrapper negate()
    {
        value = value.negate();
        return this;
    }

    @Override
    public BigDecimal asBigDecimal() {
        return value;
    }

    @Override
    public boolean isZero()
    {
        return value.signum() == 0;
    }

    @Override
    public BigDecimalWrapper mod(BigDecimalWrapper num)
    {
        value = value.remainder(num.asBigDecimal());
        return this;
    }

    @Override
    public String toString() {
        return value == null ? "UNSET" : value.toString();
    }

    @Override
    public BigDecimalWrapper deepCopy()
    {
        return new BigDecimalWrapperImpl(value);
    }
}

