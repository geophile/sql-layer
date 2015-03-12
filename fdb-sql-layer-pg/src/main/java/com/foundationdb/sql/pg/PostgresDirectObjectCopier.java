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

import java.util.List;
import java.io.IOException;

/** Output object rows using the COPY protocol, which has the
 * advantage of being asynchronous. Useful when the loadable plan does
 * something that takes a while and produces output as it goes.
 */
public class PostgresDirectObjectCopier extends PostgresOutputter<List<?>>
{
    private boolean withNewline;

    public PostgresDirectObjectCopier(PostgresQueryContext context,
                                      PostgresDMLStatement statement,
                                      boolean withNewline) {
        super(context, statement);
        this.withNewline = withNewline;
    }

    @Override
    public void output(List<?> row) throws IOException {
        messenger.beginMessage(PostgresMessages.COPY_DATA_TYPE.code());
        encoder.reset();
        for (int i = 0; i < ncols; i++) {
            if (i > 0) encoder.appendString("|");
            Object field = row.get(i);
            PostgresType type = columnTypes.get(i);
            if (field != null)
                encoder.appendPObject(field, type, false);
        }
        if (withNewline)
            encoder.appendString("\n");
        messenger.writeByteStream(encoder.getByteStream());
        messenger.sendMessage();
    }

    public void respond() throws IOException {
        messenger.beginMessage(PostgresMessages.COPY_OUT_RESPONSE_TYPE.code());
        messenger.write(0);
        messenger.writeShort(ncols);
        for (int i = 0; i < ncols; i++) {
            assert !context.isColumnBinary(i);
            messenger.writeShort(0);
        }
        messenger.sendMessage();
    }

    public void done() throws IOException {
        messenger.beginMessage(PostgresMessages.COPY_DONE_TYPE.code());
        messenger.sendMessage();
    }

}
