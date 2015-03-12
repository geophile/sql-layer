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
package com.foundationdb.server.test.it.store;

import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.test.it.FDBITBase;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class FDBDeferredUniquenessIT extends FDBITBase
{
    int tid;

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String,String> props = new HashMap<>();
        props.putAll(super.startupConfigProperties());

        props.put("fdbsql.fdb.defer_uniqueness_checks", "true");
        return props;
    }

    @Before
    public void setup() {
        tid = createTable("test", "t1", 
                          "id int not null primary key",
                          "s varchar(10)");
    }

    @Test
    public void unique() {
        writeRow(tid, 1L, "fred");
        writeRow(tid, 2L, "wilma");
    }

    @Test(expected=DuplicateKeyException.class)
    public void notunique() {
        writeRow(tid, 1L, "fred");
        writeRow(tid, 1L, "barney");
    }

}
