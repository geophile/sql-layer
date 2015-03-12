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

import java.util.List;
import java.util.Set;

/** NB: Used concurrently, synchronize implementations as appropriate. */
public interface NameGenerator
{
    // Generation
    int generateTableID(TableName name);
    int generateIndexID(int rootTableID);

    /** Generated named will be unique within the given {@code ais}. */
    TableName generateIdentitySequenceName(AkibanInformationSchema ais, TableName table, String column);

    String generateJoinName(TableName parentTable, TableName childTable, String[] pkColNames, String[] fkColNames);
    String generateJoinName(TableName parentTable, TableName childTable, List<JoinColumn> joinIndex);
    String generateJoinName(TableName parentTable, TableName childTable, List<String> pkColNames, List<String> fkColNames);
    String generateFullTextIndexPath(FullTextIndex index);
    TableName generateFKConstraintName(String schemaName, String tableName);
    TableName generatePKConstraintName(String schemaName, String tableName);
    TableName generateUniqueConstraintName(String schemaName, String tableName);
    
    // Bulk add
    void mergeAIS(AkibanInformationSchema ais);

    // Removal
    void removeTableID(int tableID);

    // View only (debug/testing)
    Set<String> getStorageNames();
}
