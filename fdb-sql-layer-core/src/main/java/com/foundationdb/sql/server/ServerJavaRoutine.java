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

import java.sql.ResultSet;
import java.util.Queue;

import com.foundationdb.ais.model.Parameter;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.error.ProtectedObjectException;
import com.foundationdb.server.explain.Explainable;
import com.foundationdb.server.service.routines.*;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.sql.routinefw.ShieldedInvokable;
import com.foundationdb.sql.routinefw.RoutineFirewall;
import java.security.AccessController;
import java.security.PrivilegedAction;

/** A Routine that uses Java native data types in its invocation API. */
public abstract class ServerJavaRoutine implements Explainable, ShieldedInvokable
{
    private ServerQueryContext context;
    private QueryBindings bindings;
    private ServerRoutineInvocation invocation;

    protected ServerJavaRoutine(ServerQueryContext context,
                                QueryBindings bindings,
                                ServerRoutineInvocation invocation) {
        this.context = context;
        this.bindings = bindings;
        this.invocation = invocation;
    }

    public ServerQueryContext getContext() {
        return context;
    }

    public ServerRoutineInvocation getInvocation() {
        return invocation;
    }

    public abstract void setInParameter(Parameter parameter, ServerJavaValues values, int index);
    public abstract void invokeShielded();
    public abstract Object getOutParameter(Parameter parameter, int index);
    public abstract Queue<ResultSet> getDynamicResultSets();

    public void push() {
        ServerCallContextStack.get().push(context, invocation);
    }

    public void pop(boolean success) {
        ServerCallContextStack.get().pop(context, invocation, success);
    }

    public void invoke() {
        SecurityService ss = context.getServiceManager().getServiceByClass(SecurityService.class);
        if (ss != null && !ss.isAccessible(context.getSession(), invocation.getRoutineName().getSchemaName()))  {
            throw new ProtectedObjectException("routine", invocation.getRoutineName().getTableName(), invocation.getRoutineName().getSchemaName());
        }
        if (invocation.getRoutine().isSystemRoutine()) {
            invokeShielded();
        }
        else {
            ScriptEngineManagerProvider semp = context.getServiceManager().getServiceByClass(ScriptEngineManagerProvider.class);
            ClassLoader origCl = getContextClassLoader();
            setContextClassLoader(semp.getSafeClassLoader());
            try {
                RoutineFirewall.callInvoke(this);
            }
            finally {
                setContextClassLoader(origCl);
            }
        }
    }

    private ClassLoader getContextClassLoader() {
        return AccessController.doPrivileged(
                new PrivilegedAction<ClassLoader>() {
                    public ClassLoader run() {
                        return Thread.currentThread().getContextClassLoader();
                    }
                }
        );
    }
    
    private void setContextClassLoader(final ClassLoader cl) {
        AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    public Void run() {
                        Thread.currentThread().setContextClassLoader(cl);
                        return null;
                    }
                }
        );
    }
    
    public void setInputs() {
        int nargs = invocation.size();
        ServerJavaValues values = invocation.asValues(context, bindings);
        for (int i = 0; i < nargs; i++) {
            Parameter parameter = invocation.getRoutineParameter(i);
            switch (parameter.getDirection()) {
            case IN:
            case INOUT:
                setInParameter(parameter, values, i);
                break;
            }
        }
    }

    public void getOutputs() {
        int nargs = invocation.size();
        ServerJavaValues values = invocation.asValues(context, bindings);
        for (int i = 0; i < nargs; i++) {
            Parameter parameter = invocation.getRoutineParameter(i);
            switch (parameter.getDirection()) {
            case INOUT:
            case OUT:
            case RETURN:
                values.setObject(i, getOutParameter(parameter, i));
                break;
            }
        }
        Parameter parameter = invocation.getRoutineParameter(ServerJavaValues.RETURN_VALUE_INDEX);
        if (parameter != null) {
            values.setObject(ServerJavaValues.RETURN_VALUE_INDEX, getOutParameter(parameter, ServerJavaValues.RETURN_VALUE_INDEX));
        }
    }
}
