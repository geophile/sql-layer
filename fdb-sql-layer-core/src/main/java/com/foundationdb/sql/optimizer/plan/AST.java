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
package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.unparser.NodeToString;

import com.foundationdb.server.types.TInstance;

import java.util.Arrays;
import java.util.Objects;
import java.util.List;

/** A parsed (and type-bound, normalized, etc.) SQL query.
 */
public class AST extends BasePlanNode
{
    private DMLStatementNode statement;
    private List<ParameterNode> parameters;

    public AST(DMLStatementNode statement, List<ParameterNode> parameters) {
        this.statement = statement;
        this.parameters = parameters;
    }
    
    public DMLStatementNode getStatement() {
        return statement;
    }

    public List<ParameterNode> getParameters() {
        return parameters;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        return v.visit(this);
    }
    
    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append("(");
        try {
            str.append(new NodeToString().toString(statement));
        }
        catch (StandardException ex) {
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        // Do not copy AST.
    }

    public String formatParameterTypes() {
        if ((parameters == null) || parameters.isEmpty())
            return "";
        String[] types = new String[parameters.size()];
        int len = 0;
        for (ParameterNode parameter : parameters) {
            int pos = parameter.getParameterNumber();
            if (len < pos + 1)
                len = pos + 1;
            TInstance type = (TInstance)parameter.getUserData();
            if (type != null) {
                types[pos] = type.toStringConcise(true);
            }
            else {
                types[pos] = Objects.toString(parameter.getType());
            }
        }
        return Arrays.toString(Arrays.copyOf(types, len)) + "\n";
    }

}
