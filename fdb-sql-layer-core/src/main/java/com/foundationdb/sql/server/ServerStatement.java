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

/**
 * An executable statement. Base only handles transactionality, not
 * actual execution and returning of values.
 */
public interface ServerStatement
{
    /** What transaction mode(s) does this statement use? */
    public enum TransactionMode { 
        ALLOWED,                // Does not matter.
        NONE,                   // Must not have a transaction; none created.
        NEW,                    // Must not have a transaction: read only created.
        NEW_WRITE,              // Must not have a transaction: read write created.
        READ,                   // New read only or existing allowed.
        WRITE,                  // New or existing read write allowed.
        REQUIRED,               // Must have transaction: read only okay.
        REQUIRED_WRITE,         // Must have read write transaction.
        IMPLICIT_COMMIT,        // Automatically commit an open transaction
        IMPLICIT_COMMIT_AND_NEW;
    }

    public enum TransactionAbortedMode {
        ALLOWED,                // Statement always allowed
        NOT_ALLOWED,            // Statement never allowed
    }

    public enum AISGenerationMode {
        ALLOWED,               // Statement can be used under any generation
        NOT_ALLOWED            // Statement can only be used under one generation
    }

    public TransactionMode getTransactionMode();
    public TransactionAbortedMode getTransactionAbortedMode();
    public AISGenerationMode getAISGenerationMode();
}
