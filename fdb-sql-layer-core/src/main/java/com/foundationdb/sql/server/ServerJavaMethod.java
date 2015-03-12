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
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.error.ExternalRoutineInvocationException;
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.ArrayDeque;
import java.util.Queue;

public class ServerJavaMethod extends ServerJavaRoutine
{
    private Method method;
    private Class<?>[] parameterTypes;
    private Object[] methodArgs;
    private Object methodResult;
    
    public ServerJavaMethod(ServerQueryContext context,
                            QueryBindings bindings,
                            ServerRoutineInvocation invocation,
                            Method method) {
        super(context, bindings, invocation);
        this.method = method;
        parameterTypes = method.getParameterTypes();
    }

    @Override
    public void push() {
        super.push();
        methodArgs = methodArgs(parameterTypes);
    }

    protected static Object[] methodArgs(Class<?>[] parameterTypes) {
        Object[] result = new Object[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> outputType = parameterTypes[i].getComponentType();
            if (outputType != null) {
                result[i] = Array.newInstance(outputType, 1);
            }
        }
        return result;
    }

    @Override
    public void setInParameter(Parameter parameter, ServerJavaValues values, int index) {
        Class<?> clazz = parameterTypes[index];
        if (clazz.isArray() && clazz != byte[].class) {
            Array.set(methodArgs[index], 0, 
                      values.getObject(index, clazz.getComponentType()));
        }
        else {
            methodArgs[index] = values.getObject(index, clazz);
        }
    }

    @Override
    public void invokeShielded() {
        try {
            methodResult = method.invoke(null, methodArgs);
        }
        catch (IllegalAccessException ex) {
            throw new ExternalRoutineInvocationException(getInvocation().getRoutineName(), ex);
        }
        catch (InvocationTargetException ex) {
            throw new ExternalRoutineInvocationException(getInvocation().getRoutineName(), ex.getTargetException());
        }
    }

    @Override
    public Object getOutParameter(Parameter parameter, int index) {
        if (parameter.getDirection() == Parameter.Direction.RETURN) {
            return methodResult;
        }
        else {
            return Array.get(methodArgs[index], 0);
        }
    }
    
    @Override
    public Queue<ResultSet> getDynamicResultSets() {
        Queue<ResultSet> result = new ArrayDeque<>();
        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> outputType = parameterTypes[i].getComponentType();
            if ((outputType != null) && ResultSet.class.isAssignableFrom(outputType)) {
                ResultSet rs = (ResultSet)Array.get(methodArgs[i], 0);
                if (rs != null)
                    result.add(rs);
            }
        }
        return result;
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes atts = new Attributes();
        atts.put(Label.PROCEDURE_IMPLEMENTATION, 
                 PrimitiveExplainer.getInstance(method.toString()));
        return new ServerCallExplainer(getInvocation(), atts, context);
    }

}
