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
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.texpressions.Constantness;

import static com.foundationdb.server.types.mcompat.mcasts.MNumericCastBase.*;

public class Cast_From_Tinyint
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
    public static final TCast TO_UNSIGNED_TINYINT = new FromInt8ToUnsignedInt8(MNumeric.TINYINT, MNumeric.TINYINT_UNSIGNED, true, Constantness.UNKNOWN);
   
    public static final TCast TO_SMALLINT = new FromInt8ToInt16(MNumeric.TINYINT, MNumeric.SMALLINT, true, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_SMALLINT = new FromInt8ToUnsignedInt16(MNumeric.TINYINT, MNumeric.SMALLINT_UNSIGNED, true, Constantness.UNKNOWN);

    public static final TCast TO_MEDIUMINT = new FromInt8ToInt32(MNumeric.TINYINT, MNumeric.MEDIUMINT, true, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_MEDIUMINT = new FromInt8ToUnsignedInt32(MNumeric.TINYINT, MNumeric.MEDIUMINT_UNSIGNED, true, Constantness.UNKNOWN);

    public static final TCast TO_INT = new FromInt8ToInt32(MNumeric.TINYINT, MNumeric.INT, true, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_INT = new FromInt8ToUnsignedInt32(MNumeric.TINYINT, MNumeric.INT_UNSIGNED, true, Constantness.UNKNOWN);

    public static final TCast TO_BIGINT = new FromInt8ToInt64(MNumeric.TINYINT, MNumeric.BIGINT, true, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_BIGINT = new FromInt8ToInt64(MNumeric.TINYINT, MNumeric.BIGINT_UNSIGNED, true, Constantness.UNKNOWN);
    
    public static final TCast TO_DECIMAL = new FromInt8ToDecimal(MNumeric.TINYINT, MNumeric.DECIMAL, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DOUBLE = new FromInt8ToDouble(MNumeric.TINYINT, MApproximateNumber.DOUBLE, true, Constantness.UNKNOWN);
}
