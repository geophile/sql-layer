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
package com.foundationdb.server.store;

import java.util.Iterator;

/**
 * A storage iterator is an <code>Iterator<Void></code> is stored in the
 * <code>iterator</code> field of a <code>FDBStoreData</code>. When advanced, it modifies
 * the state of the <code>FDBStoreData</code> so that {@link FDBStoreDataHelper#unpackKey}
 * can be called to fill the <code>persistitKey</code> field and, in the case of
 * Group iterators, {@link FDBStore#expandRow} can be called.
 * Normally, this is just a matter of copying the key/value pair of byte arrays into
 * the <code>rawKey</code> and <code>rawValue</code> fields.
 */
public abstract class FDBStoreDataIterator implements Iterator<Void> {
    protected final FDBStoreData storeData;

    public FDBStoreDataIterator(FDBStoreData storeData) {
        this.storeData = storeData;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public abstract void close();
}
