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
package com.foundationdb.qp.util;

// Caches HKeys. The caching isn't to cache the values -- operators take care of that. The purpose of this
// class is to maximize reuse of HKey objects.

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.util.SparseArray;

public class HKeyCache<HKEY extends HKey>
{
    @SuppressWarnings("unchecked")
    public HKEY hKey(Table table)
    {
        HKEY hKey;
        int ordinal = table.getOrdinal();
        if (ordinalToHKey.isDefined(ordinal)) {
            hKey = ordinalToHKey.get(ordinal);
        } else {
            hKey = (HKEY) adapter.newHKey(table.hKey());
            ordinalToHKey.set(ordinal, hKey);
        }
        return hKey;
    }

    public HKeyCache(KeyCreator adapter)
    {
        this.adapter = adapter;
    }

    private final KeyCreator adapter;
    private final SparseArray<HKEY> ordinalToHKey = new SparseArray<>();
}
