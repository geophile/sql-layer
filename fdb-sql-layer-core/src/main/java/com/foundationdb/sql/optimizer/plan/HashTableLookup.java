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
package com.foundationdb.sql.optimizer.plan;

import java.util.Collection;
import java.util.List;

/** Application of a Hash Table. */
public class HashTableLookup extends BaseJoinable
{
    private HashTable hashTable;
    private List<ExpressionNode> lookupExpressions;
    private List<ConditionExpression> conditions;
    private Collection<? extends ColumnSource> tables;

    public HashTableLookup(HashTable hashTable,
                           List<ExpressionNode> lookupExpressions,
                           List<ConditionExpression> conditions,
                           Collection<? extends ColumnSource> tables) {
        this.hashTable = hashTable;
        this.lookupExpressions = lookupExpressions;
        this.conditions = conditions;
        this.tables = tables;
    }

    public HashTable getHashTable() {
        return hashTable;
    }

    public List<ExpressionNode> getLookupExpressions() {
        return lookupExpressions;
    }

    public Collection<? extends ConditionExpression> getConditions() {
        return conditions;
    }

    public Collection<? extends ColumnSource> getTables() {
        return tables;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (tables != null) {
                for (ColumnSource table : tables) {
                    if (!table.accept(v))
                        break;
                }
            }
        }
        return v.visitLeave(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        tables = duplicateList(tables, map);
    }

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append("(");
        str.append(hashTable);
        str.append(", ");
        str.append(lookupExpressions);
        str.append(")");
        return str.toString();
    }

}
