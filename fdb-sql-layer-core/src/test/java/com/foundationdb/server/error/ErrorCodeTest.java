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
package com.foundationdb.server.error;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

public final class ErrorCodeTest {

    @Test
    public void errorCodesAllUnique() {
        final Map<String,ErrorCode> map = new HashMap<>(ErrorCode.values().length);
        for (ErrorCode errorCode : ErrorCode.values()) {
            ErrorCode oldCode = map.put(errorCode.getFormattedValue(), errorCode);
            if (oldCode != null) {
                fail(String.format("Conflict between codes %s and %s; both equal %s",
                        oldCode, errorCode, errorCode.getFormattedValue()));
            }
        }
    }
    
    @Test
    public void errorExceptionsUnique() {
        final Map<Class<? extends InvalidOperationException>, ErrorCode> map = new HashMap<>(ErrorCode.values().length);
        
        for (ErrorCode errorCode : ErrorCode.values()) {
            // don't check the null ones. 
            if (errorCode.associatedExceptionClass() == null) { continue; } 
            ErrorCode oldCode = map.put (errorCode.associatedExceptionClass(), errorCode);
            if (oldCode != null) {
               fail (String.format("Duplicate Exception between %s and %s, both with %s exception" , 
                       oldCode, errorCode, errorCode.associatedExceptionClass()));
           }
        }
    }

    @Test
    public void errorHasMessage() {
         for (ErrorCode errorCode : ErrorCode.values()) {
             assertNotNull (errorCode.getMessage());
         }
    }

    @Test
    public void noExtraMessages() {
        Set<String> enumNames = new TreeSet<>();
        for(ErrorCode code : ErrorCode.values()) {
            enumNames.add(code.name());
        }
        Set<String> msgNames = new TreeSet<>(ErrorCode.resourceBundle.keySet());
        msgNames.removeAll(enumNames);
        assertEquals("[]", msgNames.toString());
    }
}
