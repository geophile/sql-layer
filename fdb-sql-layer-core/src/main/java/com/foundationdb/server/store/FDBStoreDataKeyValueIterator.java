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

import static com.foundationdb.qp.operator.Operator.OPERATOR_TAP;

import com.foundationdb.KeyValue;
import com.foundationdb.async.AsyncIterator;
import com.foundationdb.qp.storeadapter.FDBAdapter;
import com.foundationdb.util.Debug;
import com.foundationdb.util.tap.InOutTap;

/**
 * Only takes care of the immediate copy of the key-value pair
 * into the <code>raw</code> fields.  Caller must deal with these
 * further, using unpackKey, unpackValue or expandRow.
 */
public class
    FDBStoreDataKeyValueIterator extends FDBStoreDataIterator
{
    private final AsyncIterator<KeyValue> underlying;
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: FDB Value Iterator");
    private static final InOutTap TAP_HAS_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: FDB Value hasNext"); 
    private static final boolean TAP_NEXT_ENABLED = Debug.isOn("tap_next");;

    public FDBStoreDataKeyValueIterator(FDBStoreData storeData,
                                        AsyncIterator<KeyValue> underlying) {
        super(storeData);
        this.underlying = underlying;
    }

    @Override
    public boolean hasNext() {
        if (TAP_NEXT_ENABLED) {
            TAP_HAS_NEXT.in();
        }
        try {
            return underlying.hasNext();
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(storeData.session, e);
        } finally {
            if (TAP_NEXT_ENABLED) {
                TAP_HAS_NEXT.out();
            }
        }
    }

    @Override
    public Void next() {
        if (TAP_NEXT_ENABLED) {
            TAP_NEXT.in();
        }
        try {
            KeyValue kv = underlying.next();
            storeData.rawKey = kv.getKey();
            storeData.rawValue = kv.getValue();
            return null;
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(storeData.session, e);
        } finally {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.out();
            }
        }
    }

    @Override
    public void close() {
        underlying.dispose();
    }
}
