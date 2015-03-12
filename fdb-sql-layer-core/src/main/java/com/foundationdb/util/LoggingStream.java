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
package com.foundationdb.util;

import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class LoggingStream extends OutputStream
{
    private static final String NL = System.getProperty("line.separator");

    private final Logger log;
    private final boolean isError;
    private ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    private LoggingStream(Logger log, boolean isError) {
        this.log = log;
        this.isError = isError;
    }

    public static LoggingStream forError(Logger log) {
        return new LoggingStream(log, true);
    }

    public static LoggingStream forInfo(Logger log) {
        return new LoggingStream(log, false);
    }


    @Override
    public void write(int b) throws IOException {
        checkOpen();
        buffer.write(b);
    }

    @Override
    public void flush() throws IOException {
        checkOpen();
        if(buffer.size() == 0) {
            return;
        }
        String msg = buffer.toString();
        buffer.reset();
        if(NL.equals(msg)) {
            return;
        }
        if(isError) {
            log.error(msg);
        } else {
            log.info(msg);
        }
    }

    @Override
    public void close() throws IOException {
        buffer = null;
    }

    private void checkOpen() throws IOException {
        if(buffer == null) {
            throw new IOException("closed");
        }
    }
}
