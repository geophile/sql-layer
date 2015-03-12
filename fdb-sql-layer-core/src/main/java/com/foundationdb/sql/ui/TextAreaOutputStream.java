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
package com.foundationdb.sql.ui;

import javax.swing.*;

import java.io.IOException;
import java.io.OutputStream;

public class TextAreaOutputStream extends OutputStream
{
    private JTextArea textArea;
    private final StringBuilder buffer = new StringBuilder();
    private final OutputStream oldStream;
    private int writeOldFrom;
    
    // Initialize in deferred mode.
    public TextAreaOutputStream() {
        this.oldStream = System.out;
    }

    public TextAreaOutputStream(JTextArea textArea) {
        this.textArea = textArea;
        this.oldStream = null;
    }

    public synchronized void write(int b) {
        buffer.append((char)b);
    }

    public synchronized void write(byte[] b) {
        buffer.append(new String(b));
    }

    public synchronized void write(byte[] b, int off, int len) {
        buffer.append(new String(b, off, len));
    }

    public synchronized void flush() throws IOException {
        final JTextArea textArea = this.textArea;
        if (textArea != null) {
            final String string = buffer.toString();
            buffer.setLength(0);
            writeOldFrom = 0;
            SwingUtilities.invokeLater(new Runnable() {
                    public void run() {
                        textArea.append(string);
                    }
                });
        }
        else if (oldStream != null) {
            // Keep buffering in hopes that the console will appear, but still output
            // to stdout just in case.
            int end = buffer.length();
            oldStream.write(buffer.subSequence(writeOldFrom, end).toString().getBytes());
            writeOldFrom = end;
        }
    }

    // Change stream to outputting to text area.
    public synchronized boolean setTextAreaIfUnbound(JTextArea textArea) {
        if ((this.textArea == null) && (oldStream != null)) {
            this.textArea = textArea;
            return true;
        }
        return false;
    }

    // Change stream back to original output for any messages after frame is closed.
    public synchronized boolean clearTextAreaIfBound(JTextArea textArea) {
        if (this.textArea == textArea) {
            this.textArea = null;
            return true;
        }
        return false;
    }

}
