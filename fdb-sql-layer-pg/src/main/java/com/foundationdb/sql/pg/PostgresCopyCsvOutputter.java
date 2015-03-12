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

import com.foundationdb.sql.server.ServerValueEncoder;

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.service.externaldata.CsvFormat;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.IOException;

public class PostgresCopyCsvOutputter extends PostgresOutputter<Row>
{
    private CsvFormat format;

    public PostgresCopyCsvOutputter(PostgresQueryContext context,
                                    PostgresDMLStatement statement,
                                    CsvFormat format) {
        super(context, statement);
        this.format = format;
        encoder = new ServerValueEncoder(context.getTypesTranslator(),
                                         format.getEncoding(),
                                         new QuotingByteArrayOutputStream(),
                                         context.getServer().getFormatOptions());
    }

    @Override
    public void output(Row row) throws IOException {
        messenger.beginMessage(PostgresMessages.COPY_DATA_TYPE.code());
        output(row, messenger.getRawOutput());
        messenger.sendMessage();
    }

    @Override
    public void beforeData() throws IOException {
        messenger.beginMessage(PostgresMessages.COPY_OUT_RESPONSE_TYPE.code());
        messenger.write(0);
        messenger.writeShort(ncols);
        for (int i = 0; i < ncols; i++) {
            assert !context.isColumnBinary(i);
            messenger.writeShort(0);
        }
        messenger.sendMessage();
        if (format.getHeadings() != null) {
            messenger.beginMessage(PostgresMessages.COPY_DATA_TYPE.code());
            outputHeadings(messenger.getRawOutput());
            messenger.sendMessage();
        }
    }

    @Override
    public void afterData() throws IOException {
        messenger.beginMessage(PostgresMessages.COPY_DONE_TYPE.code());
        messenger.sendMessage();
    }

    public void output(Row row, OutputStream outputStream) 
            throws IOException {
        for (int i = 0; i < ncols; i++) {
            if (i > 0) outputStream.write(format.getDelimiterByte());
            PostgresType type = columnTypes.get(i);
            boolean binary = context.isColumnBinary(i);
            QuotingByteArrayOutputStream bytes = (QuotingByteArrayOutputStream)encoder.encodeValue(row.value(i), type, binary);
            if (bytes != null) {
                bytes.quote(format);
                bytes.writeTo(outputStream);
            }
            else {
                outputStream.write(format.getNullBytes());
            }
        }
        outputStream.write(format.getRecordEndBytes());
    }

    public void outputHeadings(OutputStream outputStream) throws IOException {
        for (int i = 0; i < ncols; i++) {
            if (i > 0) outputStream.write(format.getDelimiterByte());
            outputStream.write(format.getHeadingBytes(i));
        }
        outputStream.write(format.getRecordEndBytes());
    }

    static class QuotingByteArrayOutputStream extends ByteArrayOutputStream {
        public void quote(CsvFormat format) {
            if (needsQuoting(format.getRequiresQuoting())) {
                for (int i = 0; i < count; i++) {
                    int b = buf[i] & 0xFF;
                    if ((b == format.getQuoteByte()) ||
                        (b == format.getEscapeByte())) {
                        insert(i, format.getEscapeByte());
                        i++;
                    }
                }
                insert(0, format.getQuoteByte());
                insert(count, format.getQuoteByte());
            }
        }

        private boolean needsQuoting(byte[] quotable) {
            for (int i = 0; i < count; i++) {
                byte b = buf[i];
                for (int j = 0; j < quotable.length; j++) {
                    if (b == quotable[j]) {
                        return true;
                    }
                }
            }
            return false;
        }

        private void insert(int pos, int b) {
            write(b);           // for private ensureCapacity.
            System.arraycopy(buf, pos, buf, pos+1, count-pos-1);
            buf[pos] = (byte)b;
        }
    }
 
}
