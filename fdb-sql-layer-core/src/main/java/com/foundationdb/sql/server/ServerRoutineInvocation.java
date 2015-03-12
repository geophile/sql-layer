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
package com.foundationdb.sql.server;

import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.types.TInstance;

public abstract class ServerRoutineInvocation
{
    private final Routine routine;

    protected ServerRoutineInvocation(Routine routine) {
        this.routine = routine;
    }

    public int size() {
        return routine.getParameters().size();
    }

    public Routine getRoutine() {
        return routine;
    }

    public Routine.CallingConvention getCallingConvention() {
        return routine.getCallingConvention();
    }

    public TableName getRoutineName() {
        return routine.getName();
    }

    public Parameter getRoutineParameter(int index) {
        if (index == ServerJavaValues.RETURN_VALUE_INDEX)
            return routine.getReturnValue();
        else
            return routine.getParameters().get(index);
    }

    protected TInstance getType(int index) {
        return getRoutineParameter(index).getType();
    }

    public abstract ServerJavaValues asValues(ServerQueryContext queryContext, QueryBindings bindings);
    
}
