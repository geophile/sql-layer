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
package com.foundationdb.ais.model;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DefaultIndexNameGenerator implements IndexNameGenerator {
    private final Set<String> indexNames = new HashSet<>();

    public DefaultIndexNameGenerator(Collection<String> initialIndexNames) {
        indexNames.addAll(initialIndexNames);
    }

    public static DefaultIndexNameGenerator forTable(Table table) {
        Set<String> indexNames = new HashSet<>();
        for(Index index : table.getIndexesIncludingInternal()) {
            indexNames.add(index.getIndexName().getName());
        }
        return new DefaultIndexNameGenerator(indexNames);
    }

    @Override
    public String generateIndexName(String indexName, String columnName) {
        if((indexName != null) && !indexNames.contains(indexName)) {
            indexNames.add(indexName);
            return indexName;
        }
        String name = columnName;
        for(int suffixNum = 2; indexNames.contains(name); ++suffixNum) {
            name = String.format("%s_%d", columnName, suffixNum);
        }
        indexNames.add(name);
        return name;
    }
}
