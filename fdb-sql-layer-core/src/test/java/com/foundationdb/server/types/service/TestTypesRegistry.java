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
package com.foundationdb.server.types.service;

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.aksql.aktypes.*;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;

import java.util.Arrays;

public class TestTypesRegistry extends TypesRegistry
{
    public TestTypesRegistry(TClass... tclasses) {
        super(Arrays.asList(tclasses));
    }

    // TODO: Distinguish tests that want MCOMPAT from those that just want normal types.
    public static final TypesRegistry MCOMPAT = 
        // These are the types used by unit tests before services.
        new TestTypesRegistry(MNumeric.INT, MNumeric.BIGINT, MNumeric.SMALLINT, MNumeric.TINYINT,
                              MNumeric.DECIMAL, MNumeric.DECIMAL_UNSIGNED,
                              MApproximateNumber.DOUBLE, MApproximateNumber.FLOAT,
                              MDateAndTime.DATE, MDateAndTime.DATETIME, MDateAndTime.TIMESTAMP,
                              MDateAndTime.YEAR,
                              MString.CHAR, MString.VARCHAR, MString.TEXT,
                              MBinary.VARBINARY, AkBlob.INSTANCE);
}
