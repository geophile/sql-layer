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

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TKeyComparable;

import java.util.List;

/** A join with some kind of hash table loading. */
public class HashJoinNode extends JoinNode
{
    private Joinable loader;
    private BaseHashTable hashTable;
    private List<ExpressionNode> hashColumns, matchColumns;
    private List<TKeyComparable> tKeyComparables;
    private List<AkCollator> collators;

    public HashJoinNode(Joinable loader, Joinable input, Joinable check, JoinType joinType, 
                        BaseHashTable hashTable, List<ExpressionNode> hashColumns, List<ExpressionNode> matchColumns,
                        List<TKeyComparable> tKeyComparables, List<AkCollator> collators) {
        super(input, check, joinType);
        this.loader = loader;
        loader.setOutput(this);
        this.hashTable = hashTable;
        this.hashColumns = hashColumns;
        this.matchColumns = matchColumns;
        this.tKeyComparables = tKeyComparables;
        this.collators = collators;
    }

    /**
     * This joinable produces the hash table
     */
    public Joinable getLoader() {
        return loader;
    }

    /**
     * This joinable is compared with the values in the hash table.
     * Note: the ordering of this determines the final ordering
     */
    public Joinable getInput() {
        return getLeft();
    }

    /**
     * This joinable is used to verify the results of the hash table lookup.
     */
    public Joinable getCheck() {
        return getRight();
    }

    public BaseHashTable getHashTable() {
        return hashTable;
    }
    public List<ExpressionNode> getHashColumns() {
        return hashColumns;
    }
    public List<ExpressionNode> getMatchColumns() {
        return matchColumns;
    }
    public List<TKeyComparable> getTKeyComparables() {
        return tKeyComparables;
    }
    public List<AkCollator> getCollators() {
        return collators;
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        if (loader == oldInput) {
            loader = (Joinable)newInput;
            loader.setOutput(this);
        }
        super.replaceInput(oldInput, newInput);
    }

    @Override
    protected boolean acceptPlans(PlanVisitor v) {
        return (loader.accept(v) && super.acceptPlans(v));
    }

    @Override
    protected void acceptConditions(PlanVisitor v) {
        super.acceptConditions(v);
        if (v instanceof ExpressionRewriteVisitor) {
            for (int i = 0; i < hashColumns.size(); i++) {
                hashColumns.set(i, hashColumns.get(i).accept((ExpressionRewriteVisitor)v));
                matchColumns.set(i, matchColumns.get(i).accept((ExpressionRewriteVisitor)v));
            }
        }
        else if (v instanceof ExpressionVisitor) {
            for (int i = 0; i < hashColumns.size(); i++) {
                if (!hashColumns.get(i).accept((ExpressionVisitor)v))
                    break;
                if (!matchColumns.get(i).accept((ExpressionVisitor)v))
                    break;
            }
        }
    }

    @Override
    protected void summarizeJoins(StringBuilder str) {
        super.summarizeJoins(str);
        str.append(hashColumns);
        str.append(" = ");
        str.append(matchColumns);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        loader = (Joinable)loader.duplicate(map);
        hashColumns = duplicateList(hashColumns, map);
        matchColumns = duplicateList(matchColumns, map);
    }

}
