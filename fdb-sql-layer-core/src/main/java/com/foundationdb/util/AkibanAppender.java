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

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;

public abstract class AkibanAppender {
    public abstract void append(Object o);
    public abstract void append(char c);
    public abstract void append(long l);
    public abstract void append(String s);
    public abstract Appendable getAppendable();

    public boolean canAppendBytes() {
        return false;
    }

    public Charset appendBytesAs() {
        throw new UnsupportedOperationException();
    }

    public void appendBytes(byte[] bytes, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    public void appendBytes(ByteSource byteSource) {
        appendBytes(byteSource.byteArray(), byteSource.byteArrayOffset(), byteSource.byteArrayLength());
    }

    public static AkibanAppender of(StringBuilder stringBuilder) {
        return new AkibanAppenderSB(stringBuilder);
    }

    public static AkibanAppender of(PrintWriter printWriter) {
        return new AkibanAppenderPW(printWriter);
    }

    public static AkibanAppender of(OutputStream outputStream, PrintWriter printWriter, String charset) {
        return new AkibanAppenderOS(outputStream, printWriter, charset);
    }

    private static class AkibanAppenderPW extends AkibanAppender
    {
        private final PrintWriter pr;

        public AkibanAppenderPW(PrintWriter pr) {
            this.pr = pr;
        }

        @Override
        public void append(Object o) {
            pr.print(o);
        }

        @Override
        public void append(char c) {
            pr.print(c);
        }

        @Override
        public void append(long l) {
            pr.print(l);
        }

        @Override
        public void append(String s) {
            pr.print(s);
        }

        @Override
        public Appendable getAppendable() {
            return pr;
        }

        protected void flush() {
            pr.flush();
        }
    }

    private static class AkibanAppenderOS extends AkibanAppenderPW {
        private final OutputStream os;
        private final Charset charset;

        private AkibanAppenderOS(OutputStream os, PrintWriter printWriter, String charset) {
            super(printWriter);
            this.os = os;
            this.charset = Charset.forName(charset);
        }

        @Override
        public void appendBytes(byte[] bytes, int offset, int length) {
            try {
                super.flush();
                os.write(bytes, offset, length);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean canAppendBytes() {
            return true;
        }

        @Override
        public Charset appendBytesAs() {
            return charset;
        }
    }

    private static class AkibanAppenderSB extends AkibanAppender
    {
        private final StringBuilder sb;

        public AkibanAppenderSB(StringBuilder sb) {
            this.sb = sb;
        }

        @Override
        public void append(Object o) {
            sb.append(o);
        }

        @Override
        public void append(char c) {
            sb.append(c);
        }

        @Override
        public void append(long l) {
            sb.append(l);
        }

        @Override
        public void append(String s) {
            sb.append(s);
        }

        @Override
        public Appendable getAppendable() {
            return sb;
        }

        @Override
        public String toString() {
            return sb.toString();
        }
    }
}
