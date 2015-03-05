/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.rowdata;

import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public final class ConversionHelperBigDecimalTest {

    @Test
    public void normalizeTruncateNoInt() {
        checkNormalizeToString("1", 4, 4, ".9999");
    }

    @Test
    public void normalizeTruncateNoIntNegative() {
        checkNormalizeToString("-1", 4, 4, "-.9999");
    }

    @Test
    public void normalizeTruncateOnlyInt() {
        checkNormalizeToString("1000000", 4, 0, "9999");
    }

    @Test
    public void normalizeTruncateMixed() {
        checkNormalizeToString("1000000", 4, 2, "99.99");
    }

    @Test
    public void normalizeTruncateFractional() {
        checkNormalizeToString("1.234567", 4, 2, "1.23");
    }

    @Test
    public void normalizeAddPrecision() {
        checkNormalizeToString("2.5", 5, 2, "2.50");
    }

    private void checkNormalizeToString(String in, int precision, int scale, String expected) {
        BigDecimal bigDecimal = new BigDecimal(in);
        String actual = ConversionHelperBigDecimal.normalizeToString(bigDecimal, precision, scale);
        assertEquals(String.format("%s (%d,%d)", in, precision, scale), expected, actual);
    }
}
