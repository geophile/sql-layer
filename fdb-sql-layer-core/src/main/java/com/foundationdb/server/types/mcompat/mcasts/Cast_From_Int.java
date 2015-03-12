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
package com.foundationdb.server.types.mcompat.mcasts;

import com.foundationdb.server.types.TCast;
import static com.foundationdb.server.types.mcompat.mcasts.MNumericCastBase.*;

import com.foundationdb.server.types.TCastPath;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.texpressions.Constantness;

public class Cast_From_Int
{
    /**
     * TODO:
     * 
     * DATE
     * DATETIME
     * TIME
     * TIMESTAMP
     * YEAR
     * 
     * BIT
     * CHAR
     * BINARY
     * VARBINARY
     * TINYBLOG
     * TINYTEXT
     * TEXT
     * MEDIUMBLOB
     * MEDIUMTEXT
     * LONGBLOG
     * LONTTEXT
     * 
     */

    public static final TCast TO_TINYINT = new FromInt32ToInt8(MNumeric.INT, MNumeric.TINYINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_TINYINT = new FromInt32ToUnsignedInt8(MNumeric.INT, MNumeric.TINYINT_UNSIGNED, false, Constantness.UNKNOWN);

    public static final TCast TO_SMALLINT = new FromInt32ToInt16(MNumeric.INT, MNumeric.SMALLINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_SMALLINT = new FromInt32ToUnsignedInt16(MNumeric.INT, MNumeric.SMALLINT_UNSIGNED, true, Constantness.UNKNOWN);
    
    public static final TCast TO_MEDIUMINT = new FromInt32ToInt32(MNumeric.INT, MNumeric.MEDIUMINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_MEDIUMINT = new FromInt32ToUnsignedInt32(MNumeric.INT, MNumeric.MEDIUMINT_UNSIGNED, true, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_INT = new FromInt32ToUnsignedInt32(MNumeric.INT, MNumeric.INT_UNSIGNED, true, Constantness.UNKNOWN);
    
    public static final TCast TO_BIGINT = new FromInt32ToInt64(MNumeric.INT, MNumeric.BIGINT, true, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_BIGINT = new FromInt32ToInt64(MNumeric.INT, MNumeric.BIGINT_UNSIGNED, true, Constantness.UNKNOWN);

    public static final TCast TO_DOUBLE = new FromInt32ToDouble(MNumeric.INT, MApproximateNumber.DOUBLE, true, Constantness.UNKNOWN);

    public static final TCast TO_DECIMAL = new FromInt32ToDecimal(MNumeric.INT, MNumeric.DECIMAL, false, Constantness.UNKNOWN);

    public static final TCastPath TO_DATE = TCastPath.create(MNumeric.INT, MNumeric.BIGINT, MDateAndTime.DATE);

    public static final TCastPath TO_DATETIME = TCastPath.create(MNumeric.INT, MNumeric.BIGINT, MDateAndTime.DATETIME);

    public static final TCastPath TO_TIMESTAMP = TCastPath.create(MNumeric.INT, MNumeric.BIGINT, MDateAndTime.TIMESTAMP);

    public static final TCastPath TO_TIME = TCastPath.create(MNumeric.INT, MNumeric.BIGINT, MDateAndTime.TIME);
}
