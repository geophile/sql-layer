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

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.HKeyColumn;
import com.foundationdb.ais.model.HKeySegment;
import com.foundationdb.ais.model.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public abstract class MultiIndexEnumerator<C,N extends IndexIntersectionNode<C,N>,L extends N> implements Iterable<N> {

    protected abstract Collection<? extends C> getLeafConditions(L node);
    protected abstract N intersect(N first, N second, int comparisonCount);
    protected abstract List<Column> getComparisonColumns(N first, N second);

    // becomes null when we start enumerating
    private List<L> leaves = new ArrayList<>();
    private Set<C> conditions = new HashSet<>();
    
    public void addLeaf(L leaf) {
        leaves.add(leaf);
    }
    
    public Iterator<L> leavesIterator() {
        return leaves.iterator();
    }
    
    private class ComboIterator implements Iterator<N> {
        
        private boolean done = false;
        private List<N> current = new ArrayList<>();
        private Iterator<N> currentIter;

        // These are only used in advancePhase, but we cache them to save on allocations
        private List<N> previous = new ArrayList<>();
        private ConditionsCounter<C> outerCounter = new ConditionsCounter<>(conditions.size());
        private ConditionsCounter<C> innerCounter = new ConditionsCounter<>(conditions.size());
        private ConditionsCount<C> bothCount = new OverlayedConditionsCount<>(outerCounter, innerCounter);

        private ComboIterator() {
            current.addAll(leaves);
            advancePhase();
        }

        @Override
        public boolean hasNext() {
            if (done)
                return false;
            if (currentIter.hasNext())
                return true;
            advancePhase();
            return !done;
        }

        @Override
        public N next() {
            if (done)
                throw new NoSuchElementException();
            if (!currentIter.hasNext())
                advancePhase();
            return currentIter.next();
        }

        @Override
        public void remove() {
            currentIter.remove();
        }

        private void advancePhase() {
            assert (currentIter == null) || (!currentIter.hasNext()) : "internal iterator not exhausted";
            if (current.isEmpty()) {
                done = true;
                return;
            }

            previous.clear();
            previous.addAll(current);
            current.clear();
            int conditionsCount = conditions.size();
            for (N outer : previous) {
                outer.incrementConditionsCounter(outerCounter);
                int counted = outerCounter.conditionsCounted();
                // only try the leaves if the outer counted some conditions, but not all of them.
                if (counted > 0 && counted < conditionsCount) {
                    // at this point, "outer" satisfies some conditions, and more conditions are left
                    for (L inner : leaves) {
                        if (inner == outer)
                            continue; // fast path, we know there's full overlap
                        inner.incrementConditionsCounter(innerCounter);
                        if (inner.isUseful(bothCount) && outer.isUseful(bothCount)) {
                            emit(outer, inner, current);
                        }
                        innerCounter.clear();
                    }
                }
                outerCounter.clear();
            }
            if (current.isEmpty()) {
                done = true;
                currentIter = null;
            }
            else {
                currentIter = current.iterator();
            }
        }
    }

    @Override
    public Iterator<N> iterator() {
        filterLeaves();
        return new ComboIterator();
    }

    private void filterLeaves() {
        for (Iterator<L> iter = leaves.iterator(); iter.hasNext(); ) {
            L leaf = iter.next();
            Collection<? extends C> nodeConditions = getLeafConditions(leaf);
            if ( (nodeConditions != null) && (!nodeConditions.isEmpty()) ) {
                conditions.addAll(nodeConditions);
            }
            else {
                iter.remove();
            }
        }
    }

    private void emit(N first, N second, Collection<N> output)
    {
        Table firstTable = first.getLeafMostAisTable();
        Table secondTable = second.getLeafMostAisTable();
        List<Column> comparisonCols = getComparisonColumns(first, second);
        if (comparisonCols.isEmpty())
            return;
        int comparisonsLen = comparisonCols.size();
        // find the Table associated with the common N. This works for multi- as well as single-branch
        Table commonAncestor = first.findCommonAncestor(second);
        assert commonAncestor == second.findCommonAncestor(first) : first + "'s ancestor not reflexive with " + second;
        boolean isMultiBranch = true;
        if (firstTable != secondTable) {
            if (commonAncestor == firstTable) {
                isMultiBranch = false;
                if (includesHKey(firstTable, comparisonCols))
                    output.add(intersect(second, first, comparisonsLen));
            }
            else {
                // in single-branch cases, we only want to output the leafmost's index
                isMultiBranch = (commonAncestor != secondTable);
            }
        }
        if (isMultiBranch && includesHKey(commonAncestor, comparisonCols)) {
            output.add(intersect(first, second, comparisonsLen));
        }
    }

    private boolean includesHKey(Table table, List<Column> columns) {
        // TODO this seems horridly inefficient, but the data set is going to be quite small
        for (HKeySegment segment : table.hKey().segments()) {
            for (HKeyColumn hKeyCol : segment.columns()) {
                boolean found = false;
                for (Column equiv : hKeyCol.equivalentColumns()) {
                    if (columns.contains(equiv)) {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    return false;
            }
        }
        return true;
    }
}
