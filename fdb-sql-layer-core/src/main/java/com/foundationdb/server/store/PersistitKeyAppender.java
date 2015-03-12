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

import com.foundationdb.ais.model.Column;
import com.foundationdb.server.PersistitKeyValueTarget;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.persistit.Key;

public abstract class PersistitKeyAppender {

    public final void append(int value) {
        key.append(value);
    }

    public final void append(long value) {
        key.append(value);
    }

    public final void appendNull() {
        key.append(null);
    }

    public final Key key() {
        return key;
    }

    public final void clear()
    {
        key().clear();
    }

    public abstract void append(Object object, Column column);

    public abstract void append(ValueSource source, Column column);

    public static PersistitKeyAppender create(Key key, Object descForError) {
        return new New(key, descForError);
    }

    protected PersistitKeyAppender(Key key) {
        this.key = key;
    }

    protected final Key key;

    // Inner classes

    private static class New extends PersistitKeyAppender
    {
        public New(Key key, Object descForError) {
            super(key);
            target = new PersistitKeyValueTarget(descForError);
            target.attach(this.key);
        }

        public void append(Object object, Column column) {
            column.getType().writeCollating(ValueSources.valuefromObject(object, column.getType()), target);
        }

        public void append(ValueSource source, Column column) {
            column.getType().writeCollating(source, target);
        }

        private final PersistitKeyValueTarget target;
    }
}
