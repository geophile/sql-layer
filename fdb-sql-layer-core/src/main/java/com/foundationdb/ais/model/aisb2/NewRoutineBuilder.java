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
package com.foundationdb.ais.model.aisb2;

import static com.foundationdb.ais.model.Routine.*;

public interface NewRoutineBuilder {
    NewRoutineBuilder language(String language, CallingConvention callingConvention);

    NewRoutineBuilder returnBoolean(String name);

    NewRoutineBuilder returnLong(String name);

    NewRoutineBuilder returnString(String name, int length);
    
    NewRoutineBuilder returnVarBinary(String name, int length);
    
    NewRoutineBuilder paramBooleanIn(String name);

    NewRoutineBuilder paramLongIn(String name);
    
    NewRoutineBuilder paramIntegerIn(String name);

    NewRoutineBuilder paramStringIn(String name, int length);

    NewRoutineBuilder paramVarBinaryIn(String name, int length);
    
    NewRoutineBuilder paramDoubleIn(String name);

    NewRoutineBuilder paramLongOut(String name);

    NewRoutineBuilder paramStringOut(String name, int length);
    
    NewRoutineBuilder paramVarBinaryOut(String name, int length);

    NewRoutineBuilder paramDoubleOut(String name);
    
    NewRoutineBuilder externalName(String className);

    NewRoutineBuilder externalName(String className, String methodName);

    NewRoutineBuilder externalName(String jarName, String className, String methodName);

    NewRoutineBuilder externalName(String jarSchema, String jarName, 
                                   String className, String methodName);

    NewRoutineBuilder procDef(String definition);

    NewRoutineBuilder sqlAllowed(SQLAllowed sqlAllowed);

    NewRoutineBuilder dynamicResultSets(int dynamicResultSets);

    NewRoutineBuilder deterministic(boolean deterministic);

    NewRoutineBuilder calledOnNullInput(boolean calledOnNullInput);
}
