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

import java.io.InputStream;
import java.io.IOException;

public class PostgresCopyInputStream extends InputStream
{
    private PostgresMessenger messenger;
    private byte[] message;
    private int length, pos;
    private boolean eof;
    
    public PostgresCopyInputStream(PostgresMessenger messenger, int ncols) 
            throws IOException {
        this.messenger = messenger;

        messenger.beginMessage(PostgresMessages.COPY_IN_RESPONSE_TYPE.code());
        messenger.writeByte(0); // textual
        messenger.writeShort((short)ncols);
        for (int i = 0; i < ncols; i++) {
            messenger.writeShort(0); // text
        }
        messenger.sendMessage(true);
    }

    @Override
    public int read() throws IOException {
        while (true) {
            if (pos < length)
                return message[pos++];
            if (!nextMessage())
                return -1;
        }
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (len == 0) 
            return 0;
        while (true) {
            int nb = length - pos;
            if (nb > 0) {
                if (nb > len) nb = len;
                System.arraycopy(message, pos, b, off, nb);
                pos += nb;
                return nb;
            }
            if (!nextMessage())
                return -1;
        }
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0) return 0;
        long skipped = 0;
        while (true) {
            int nb = length - pos;
            if (nb > 0) {
                if (nb > n) nb = (int)n;
                pos += nb;
                skipped += nb;
                n -= nb;
                if (n <= 0) break;
            }
            if (!nextMessage())
                break;
        }
        return skipped;
    }

    @Override
    public int available() throws IOException {
        return (length - pos);
    }

    private boolean nextMessage() throws IOException {
        while (true) {
            switch (messenger.readMessage()) {
            case COPY_DATA_TYPE:
                message = messenger.getRawMessage();
                pos = 0;
                length = message.length;
                return true;
            case COPY_DONE_TYPE:
                return false;
            case COPY_FAIL_TYPE:
                throw new IOException("Copy failed: " + messenger.readString());
            case FLUSH_TYPE:
            case SYNC_TYPE:
                break;
            default:
                throw new IOException("Unexpected message type");
            }
        }
    }

}
