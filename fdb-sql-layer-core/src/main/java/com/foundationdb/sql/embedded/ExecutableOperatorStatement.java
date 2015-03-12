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
package com.foundationdb.sql.embedded;

import com.foundationdb.qp.operator.Operator;

abstract class ExecutableOperatorStatement extends ExecutableStatement
{
    protected Operator resultOperator;
    protected long aisGeneration;
    protected JDBCResultSetMetaData resultSetMetaData;
    protected JDBCParameterMetaData parameterMetaData;
    
    protected ExecutableOperatorStatement(Operator resultOperator,
                                          long aisGeneration,
                                          JDBCResultSetMetaData resultSetMetaData,
                                          JDBCParameterMetaData parameterMetaData) {
        this.resultOperator = resultOperator;
        this.aisGeneration = aisGeneration;
        this.resultSetMetaData = resultSetMetaData;
        this.parameterMetaData = parameterMetaData;
    }

    public Operator getResultOperator() {
        return resultOperator;
    }

    @Override
    public JDBCResultSetMetaData getResultSetMetaData() {
        return resultSetMetaData;
    }

    @Override
    public JDBCParameterMetaData getParameterMetaData() {
        return parameterMetaData;
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.READ;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.NOT_ALLOWED;
    }

    @Override
    public long getAISGeneration() {
        return aisGeneration;
    }

}
