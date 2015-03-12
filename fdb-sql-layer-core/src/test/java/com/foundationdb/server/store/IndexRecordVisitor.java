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

import com.persistit.Key;
import com.persistit.Value;

import java.util.ArrayList;
import java.util.List;

public abstract class IndexRecordVisitor extends IndexVisitor<Key,Value> {

    protected abstract void visit(List<?> key, Object value);

    @Override
    protected final void visit(Key key, Value value) {
        List<?> keyList = key(key, value);
        Object valueObj = value.isDefined() ? value.get() : null;
        visit(keyList, valueObj);
    }

    private List<?> key(Key key, Value value)
    {
        // Key traversal
        key.indexTo(0);
        List<Object> keyList = new ArrayList<>();
        extractKeySegments(key, keyList);
        // Value traversal. If the value is defined, then it contains more fields encoded like a key.
        // TODO: What about group indexes?
        if (!groupIndex() && value.isDefined()) {
            Key buffer = new Key(key);
            buffer.clear();
            value.getByteArray(buffer.getEncodedBytes(), 0, 0, value.getArrayLength());
            buffer.setEncodedSize(value.getArrayLength());
            extractKeySegments(buffer, keyList);
        }
        return keyList;
    }

    private void extractKeySegments(Key key, List<Object> list)
    {
        int segments = key.getDepth();
        for (int s = 0; s < segments; s++) {
            list.add(key.decode());
        }
    }
}
