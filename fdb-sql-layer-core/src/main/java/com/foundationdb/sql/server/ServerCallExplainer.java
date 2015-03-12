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

import com.foundationdb.server.explain.*;

public class ServerCallExplainer extends CompoundExplainer
{
    public ServerCallExplainer(ServerRoutineInvocation invocation, Attributes atts, ExplainContext context) {
        super(Type.PROCEDURE, addAttributes(atts, (ServerCallInvocation)invocation, context));
    }

    private static Attributes addAttributes(Attributes atts, 
                                            ServerCallInvocation invocation,
                                            ExplainContext context) {
        atts.put(Label.TABLE_SCHEMA, PrimitiveExplainer.getInstance(invocation.getRoutineName().getSchemaName()));
        atts.put(Label.TABLE_NAME, PrimitiveExplainer.getInstance(invocation.getRoutineName().getTableName()));
        atts.put(Label.PROCEDURE_CALLING_CONVENTION, PrimitiveExplainer.getInstance(invocation.getCallingConvention().name()));
        for (int i = 0; i < invocation.size(); i++) {
            int paramNumber = invocation.getParameterNumber(i);
            CompoundExplainer opex;
            if (paramNumber < 0) {
                opex = new CompoundExplainer(Type.LITERAL);
                opex.addAttribute(Label.OPERAND, PrimitiveExplainer.getInstance(String.valueOf(invocation.getConstantValue(i))));
            }
            else {
                opex = new CompoundExplainer(Type.VARIABLE);
                opex.addAttribute(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(paramNumber));
            }
            atts.put(Label.OPERAND, opex);
        }
        return atts;
    }

}
