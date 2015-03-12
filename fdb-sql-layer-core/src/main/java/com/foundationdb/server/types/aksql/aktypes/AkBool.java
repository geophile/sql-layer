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
package com.foundationdb.server.types.aksql.aktypes;

import com.foundationdb.server.types.aksql.AkParsers;
import com.foundationdb.server.types.aksql.AkCategory;
import com.foundationdb.server.types.common.TFormatter;
import com.foundationdb.server.types.aksql.AkBundle;
import com.foundationdb.server.types.common.types.NoAttrTClass;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.sql.types.TypeId;

/**
 * 
 * Implement Main's bool type which is a Java's primitive boolean
 */
public class AkBool
{
    public static final NoAttrTClass INSTANCE 
            = new NoAttrTClass(AkBundle.INSTANCE.id(), "boolean", AkCategory.LOGIC, TFormatter.FORMAT.BOOL, 1, 1, 1,
                               UnderlyingType.BOOL, AkParsers.BOOLEAN, 5, TypeId.BOOLEAN_ID);
}
