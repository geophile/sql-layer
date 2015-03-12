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
package com.foundationdb.sql.pg;

import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.sql.server.ServerJavaRoutine;
import com.foundationdb.sql.server.ServerJavaValues;

import java.util.List;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PostgresJavaRoutineResultsOutputter extends PostgresOutputter<ServerJavaRoutine>
{
    public PostgresJavaRoutineResultsOutputter(PostgresQueryContext context,
                                               PostgresJavaRoutine statement) {
        super(context, statement);
    }

    @Override
    public void output(ServerJavaRoutine javaRoutine) throws IOException {
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(ncols);
        int fieldIndex = 0;
        Routine routine = javaRoutine.getInvocation().getRoutine();
        List<Parameter> params = routine.getParameters();
        for (int i = 0; i < params.size(); i++) {
            Parameter param = params.get(i);
            if (param.getDirection() == Parameter.Direction.IN) continue;
            output(javaRoutine, param, i, fieldIndex++);
        }
        if (routine.getReturnValue() != null) {
            output(javaRoutine, routine.getReturnValue(), ServerJavaValues.RETURN_VALUE_INDEX, fieldIndex++);
        }
        messenger.sendMessage();
    }

    protected void output(ServerJavaRoutine javaRoutine, Parameter param, int i, int fieldIndex) throws IOException {
        Object field = javaRoutine.getOutParameter(param, i);
        PostgresType type = columnTypes.get(fieldIndex);
        boolean binary = context.isColumnBinary(fieldIndex);
        ByteArrayOutputStream bytes = encoder.encodePObject(field, type, binary);
        if (bytes == null) {
            messenger.writeInt(-1);
        }
        else {
            messenger.writeInt(bytes.size());
            messenger.writeByteStream(bytes);
        }
    }

}
