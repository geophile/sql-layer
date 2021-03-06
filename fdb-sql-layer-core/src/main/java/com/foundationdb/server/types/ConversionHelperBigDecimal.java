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
package com.foundationdb.server.types;

import com.foundationdb.server.types.common.BigDecimalWrapperImpl;
import com.foundationdb.util.AkibanAppender;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class ConversionHelperBigDecimal {

    public static byte[] bytesFromObject(BigDecimal value, int declPrec, int declScale) {
        final int declIntSize = calcBinSize(declPrec - declScale);
        final int declFracSize = calcBinSize(declScale);

        int size = declIntSize + declFracSize;
        byte[] results = new byte[size];
        fromObject(value, results, 0, declPrec, declScale);
        return results;
    }

    private static int fromObject(BigDecimal value, byte[] dest, int offset, int declPrec, int declScale) {
        final String from = normalizeToString(value, declPrec, declScale);

        final int mask = (from.charAt(0) == '-') ? -1 : 0;
        int fromOff = 0;

        if (mask != 0)
            ++fromOff;

        int signSize = mask == 0 ? 0 : 1;
        int periodIndex = from.indexOf('.');
        final int intCnt;
        final int fracCnt;

        if (periodIndex == -1) {
            intCnt = from.length() - signSize;
            fracCnt = 0;
        }
        else {
            intCnt = periodIndex - signSize;
            fracCnt = from.length() - intCnt - 1 - signSize;
        }

        final int intFull = intCnt / DECIMAL_DIGIT_PER;
        final int intPart = intCnt % DECIMAL_DIGIT_PER;
        final int fracFull = fracCnt / DECIMAL_DIGIT_PER;
        final int fracPart = fracCnt % DECIMAL_DIGIT_PER;
        final int intSize = calcBinSize(intCnt);

        final int declIntSize = calcBinSize(declPrec - declScale);
        final int declFracSize = calcBinSize(declScale);

        int toItOff = offset;
        int toEndOff = offset + declIntSize + declFracSize;

        for (int i = 0; (intSize + i) < declIntSize; ++i)
            dest[toItOff++] = (byte) mask;

        int sum = 0;

        // Partial integer
        if (intPart != 0) {
            for (int i = 0; i < intPart; ++i) {
                sum *= 10;
                sum += (from.charAt(fromOff + i) - '0');
            }

            int count = DECIMAL_BYTE_DIGITS[intPart];
            packIntegerByWidth(count, sum ^ mask, dest, toItOff);

            toItOff += count;
            fromOff += intPart;
        }

        // Full integers
        for (int i = 0; i < intFull; ++i) {
            sum = 0;

            for (int j = 0; j < DECIMAL_DIGIT_PER; ++j) {
                sum *= 10;
                sum += (from.charAt(fromOff + j) - '0');
            }

            int count = DECIMAL_TYPE_SIZE;
            packIntegerByWidth(count, sum ^ mask, dest, toItOff);

            toItOff += count;
            fromOff += DECIMAL_DIGIT_PER;
        }

        // Move past decimal point (or to end)
        ++fromOff;

        // Full fractions
        for (int i = 0; i < fracFull; ++i) {
            sum = 0;

            for (int j = 0; j < DECIMAL_DIGIT_PER; ++j) {
                sum *= 10;
                sum += (from.charAt(fromOff + j) - '0');
            }

            int count = DECIMAL_TYPE_SIZE;
            packIntegerByWidth(count, sum ^ mask, dest, toItOff);

            toItOff += count;
            fromOff += DECIMAL_DIGIT_PER;
        }

        // Fraction left over
        if (fracPart != 0) {
            sum = 0;

            for (int i = 0; i < fracPart; ++i) {
                sum *= 10;
                sum += (from.charAt(fromOff + i) - '0');
            }

            int count = DECIMAL_BYTE_DIGITS[fracPart];
            packIntegerByWidth(count, sum ^ mask, dest, toItOff);

            toItOff += count;
        }

        while (toItOff < toEndOff)
            dest[toItOff++] = (byte) mask;

        dest[offset] ^= 0x80;

        return declIntSize + declFracSize;
    }

    public static String normalizeToString(BigDecimal value, int declPrec, int declScale) {
        // First, we have to turn the value into one that fits the Column's constraints.
        int valuePrec = BigDecimalWrapperImpl.sqlPrecision(value);
        int valueScale = BigDecimalWrapperImpl.sqlScale(value);
        assert valueScale >= 0 : value;
        int valueIntDigits = valuePrec - valueScale;
        int declIntDigits = declPrec - declScale;
        if (valueIntDigits > declIntDigits) {
            // A value that does not fit must have more digits, but
            // they still might be zero.
            boolean overflow = false;
            switch (value.signum()) {
            case 0:
                break;
            case +1:
                overflow = value.compareTo(BigDecimal.ONE.scaleByPowerOfTen(declIntDigits)) >= 0;
                break;
            case -1:
                overflow = value.compareTo(BigDecimal.valueOf(-1).scaleByPowerOfTen(declIntDigits)) <= 0;
                break;
            }
            if (overflow) {
                // truncate to something like "99.999"
                StringBuilder sb = new StringBuilder(declPrec+2); // one for minus sign, one for period
                if (value.signum() < 0)
                    sb.append('-');
                for (int i = declPrec; i > 0; --i) {
                    if (i == declScale)
                        sb.append('.');
                    sb.append('9');
                }
                return sb.toString();
            }
        }
        String from;
        if (valueScale != declScale) {
            // just truncate
            BigDecimal rounded = value.setScale(declScale, RoundingMode.HALF_UP);
            from = rounded.toPlainString();
        }
        else {
            from = value.toPlainString();
        }
        if (declIntDigits == 0) {
            if (value.signum() < 0) {
                assert ((from.length() > 3) &&
                        (from.charAt(0) == '-') &&
                        (from.charAt(1) == '0') &&
                        (from.charAt(2) == '.')) :
                       from;
                from = "-" + from.substring(2);
            }
            else {
                assert ((from.length() > 2) &&
                        (from.charAt(0) == '0') &&
                        (from.charAt(1) == '.')) :
                       from;
                from = from.substring(1);
            }
        }
        return from;
    }

    // for use within this package (for testing)

    /**
     * Decodes bytes representing the decimal value into the given AkibanAppender.
     * @param from the bytes to parse
     * @param location the starting offset within the "from" array
     * @param precision the decimal's precision
     * @param scale the decimal's scale
     * @param appender the StringBuilder to write to
     * @throws NullPointerException if from or appender are null
     * @throws NumberFormatException if the parse failed; the exception's message will be the String that we
     * tried to parse
     */
    public static void decodeToString(byte[] from, int location, int precision, int scale, AkibanAppender appender) {
        final int intCount = precision - scale;
        final int intFull = intCount / DECIMAL_DIGIT_PER;
        final int intPartial = intCount % DECIMAL_DIGIT_PER;
        final int fracFull = scale / DECIMAL_DIGIT_PER;
        final int fracPartial = scale % DECIMAL_DIGIT_PER;

        int curOff = location;

        final int mask = (from[curOff] & 0x80) != 0 ? 0 : -1;

        // Flip high bit during processing
        from[curOff] ^= 0x80;

        if (mask != 0)
            appender.append('-');

        boolean hadOutput = false;
        if (intPartial != 0) {
            int count = DECIMAL_BYTE_DIGITS[intPartial];
            int x = unpackIntegerByWidth(count, from, curOff) ^ mask;
            curOff += count;
            if (x != 0) {
                hadOutput = true;
                appender.append(x);
            }
        }

        for (int i = 0; i < intFull; ++i) {
            int x = unpackIntegerByWidth(DECIMAL_TYPE_SIZE, from, curOff) ^ mask;
            curOff += DECIMAL_TYPE_SIZE;

            if (hadOutput) {
                appender.append(String.format("%09d", x));
            } else if (x != 0) {
                hadOutput = true;
                appender.append(x);
            }
        }

        if (fracFull + fracPartial > 0) {
            if (hadOutput) {
                appender.append('.');
            }
            else {
                appender.append("0.");
            }
        }
        else if(!hadOutput)
            appender.append('0');

        for (int i = 0; i < fracFull; ++i) {
            int x = unpackIntegerByWidth(DECIMAL_TYPE_SIZE, from, curOff) ^ mask;
            curOff += DECIMAL_TYPE_SIZE;
            appender.append(String.format("%09d", x));
        }

        if (fracPartial != 0) {
            int count = DECIMAL_BYTE_DIGITS[fracPartial];
            int x = unpackIntegerByWidth(count, from, curOff) ^ mask;
            int width = scale - (fracFull * DECIMAL_DIGIT_PER);
            appender.append(String.format("%0" + width + "d", x));
        }

        // Restore high bit
        from[location] ^= 0x80;
    }

    // private methods


    private static int calcBinSize(int digits) {
        int full = digits / DECIMAL_DIGIT_PER;
        int partial = digits % DECIMAL_DIGIT_PER;
        return (full * DECIMAL_TYPE_SIZE) + DECIMAL_BYTE_DIGITS[partial];
    }

    /**
     * Pack an integer, of a given length, in big endian order into a byte array.
     * @param len length of integer
     * @param val value to store in the buffer
     * @param buf destination array to put bytes in
     * @param offset position to start at in buf
     */
    private static void packIntegerByWidth(int len, int val, byte[] buf, int offset) {
        if (len == 1) {
            buf[offset] = (byte) (val);
        } else if (len == 2) {
            buf[offset + 1] = (byte) (val);
            buf[offset]     = (byte) (val >> 8);
        } else if (len == 3) {
            buf[offset + 2] = (byte) (val);
            buf[offset + 1] = (byte) (val >> 8);
            buf[offset]     = (byte) (val >> 16);
        } else if (len == 4) {
            buf[offset + 3] = (byte) (val);
            buf[offset + 2] = (byte) (val >> 8);
            buf[offset + 1] = (byte) (val >> 16);
            buf[offset]     = (byte) (val >> 24);
        } else {
            throw new IllegalArgumentException("Unexpected length " + len);
        }
    }

    /**
     * Unpack a big endian integer, of a given length, from a byte array.
     * @param len length of integer to pull out of buffer
     * @param buf source array to get bytes from
     * @param offset position to start at in buf
     * @return The unpacked integer
     */
    private static int unpackIntegerByWidth(int len, byte[] buf, int offset) {
        if (len == 1) {
            return buf[offset];
        } else if (len == 2) {
            return (buf[offset] << 24
                    | (buf[offset+1] & 0xFF) << 16) >> 16;
        } else if (len == 3) {
            return (buf[offset] << 24
                    | (buf[offset+1] & 0xFF) << 16
                    | (buf[offset+2] & 0xFF) << 8) >> 8;
        } else if (len == 4) {
            return buf[offset] << 24
                    | (buf[offset+1] & 0xFF) << 16
                    | (buf[offset+2] & 0xFF) << 8
                    | (buf[offset+3] & 0xFF);
        }

        throw new IllegalArgumentException("Unexpected length " + len);
    }

    // hidden ctor
    private ConversionHelperBigDecimal() {}

    // consts

    //
    // DECIMAL related defines as specified at:
    // http://dev.mysql.com/doc/refman/5.4/en/storage-requirements.html
    // In short, up to 9 digits get packed into a 4 bytes.
    //
    private static final int DECIMAL_TYPE_SIZE = 4;
    private static final int DECIMAL_DIGIT_PER = 9;
    private static final int DECIMAL_BYTE_DIGITS[] = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };
}
