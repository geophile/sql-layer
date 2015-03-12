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
package com.foundationdb.qp.operator;

import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

// IndexScanSelector reflects USAGE of the index in a query.
// leftJoinAfter(Index, Table): query has inner joins down to the table then left (further down)
// rightJoinUntil(Index, Table): query has right joins down to the table then inner (further down)

public abstract class IndexScanSelector {

    public abstract boolean matchesAll();
    public abstract boolean matches(long map);

    /**
     * For testing.
     * @return the bitmask that will be compared against
     */
    abstract long getBitMask();

    public static IndexScanSelector leftJoinAfter(Index index, final Table leafmostRequired) {
        final int leafmostRequiredDepth = leafmostRequired.getDepth();
        return create(index, new SelectorCreationPolicy() {
            @Override
            public boolean include(Table table) {
                if (table.equals(leafmostRequired))
                    sawTable = true;
                return table.getDepth() <= leafmostRequiredDepth;
            }

            @Override
            public String description(GroupIndex index) {
                return index.leafMostTable().equals(leafmostRequired)
                        ? ""
                        : " INNER JOIN thru " + leafmostRequired.getName().getTableName() + ", then LEFT";
            }

            @Override
            public void validate(GroupIndex index) {
                if (!sawTable)
                    complain(index, leafmostRequired);
            }

            @Override
            public void validate(TableIndex index) {
                if (!index.getTable().equals(leafmostRequired))
                    complain(index, leafmostRequired);
            }

            private boolean sawTable = false;
        });
    }

    public static IndexScanSelector rightJoinUntil(Index index, final Table rootmostRequired) {
        final int leafmostRequiredDepth = rootmostRequired.getDepth();
        return create(index, new SelectorCreationPolicy() {
            @Override
            public boolean include(Table table) {
                if (table.equals(rootmostRequired))
                    sawTable = true;
                return table.getDepth() >= leafmostRequiredDepth;
            }

            @Override
            public String description(GroupIndex index) {
                return index.rootMostTable().equals(rootmostRequired)
                        ? ""
                        : " RIGHT JOIN thru " + rootmostRequired.getName().getTableName() + ", then INNER";
            }

            @Override
            public void validate(GroupIndex index) {
                if (!sawTable)
                    complain(index, rootmostRequired);
            }

            @Override
            public void validate(TableIndex index) {
                if (!index.getTable().equals(rootmostRequired))
                    complain(index, rootmostRequired);

            }

            private boolean sawTable = false;
        });

    }

    private static void complain(Index index, Table rootmostRequired) {
        throw new IllegalArgumentException(rootmostRequired + " not in " + index);
    }

    public static IndexScanSelector inner(Index index) {
        return create(index, new SelectorCreationPolicy() {
            @Override
            public boolean include(Table table) {
                return true;
            }

            @Override
            public String description(GroupIndex index) {
                return "";
            }

            @Override
            public void validate(GroupIndex index) {
            }

            @Override
            public void validate(TableIndex index) {
            }
        });
    }

    private static IndexScanSelector create(Index index, SelectorCreationPolicy policy) {
        if (index.isTableIndex()) {
            policy.validate((TableIndex)index);
            return ALLOW_ALL;
        }
        return create((GroupIndex)index, policy);
    }

    private static IndexScanSelector create(GroupIndex index, SelectorCreationPolicy policy) {
        Table giLeaf = index.leafMostTable();
        List<Table> requiredTables = new ArrayList<>(giLeaf.getDepth());
        for(Table table = giLeaf, end = index.rootMostTable().getParentTable();
            table != null && !table.equals(end);
            table = table.getParentTable()
        ) {
            if (policy.include(table))
                requiredTables.add(table);
        }
        policy.validate(index);
        return new SelectiveGiSelector(index, requiredTables, policy.description(index));
    }

    /**
     * A policy which tells which tables are required and which aren't.
     */
    private interface SelectorCreationPolicy {
        boolean include(Table table);
        String description(GroupIndex index);

        /**
         * Invoked <em>after all calls to {@linkplain #include}</em> to perform any final validations. Specifically,
         * lets policies make sure that they saw tables they expected to see.
         * @param index the index that triggered this policy
         */
        void validate(GroupIndex index);

        /**
         * Invoked if the index that triggered this policy was a table index
         * @param index the index that triggered this policy
         */
        void validate(TableIndex index);
    }

    private IndexScanSelector() {}

    private static final IndexScanSelector ALLOW_ALL = new AllSelector();

    public abstract String describe();

    private static class SelectiveGiSelector extends IndexScanSelector {
        @Override
        public boolean matchesAll() {
            return false;
        }

        @Override
        public boolean matches(long map) {
            return (map & requiredMap) == requiredMap;
        }

        @Override
        public String describe() {
            return description;
        }

        @Override
        long getBitMask() {
            return requiredMap;
        }

        private SelectiveGiSelector(GroupIndex index, Collection<? extends Table> tables, String description) {
            long tmpMap = 0;
            for (Table table : tables) {
                tmpMap |= (1 << table.getDepth());
            }
            requiredMap = tmpMap;
            this.description = description;
        }

        private final long requiredMap;
        private final String description;
    }

    private static class AllSelector extends IndexScanSelector {
        @Override
        public boolean matchesAll() {
            return true;
        }

        @Override
        public boolean matches(long map) {
            return true;
        }

        @Override
        public String describe() {
            return "";
        }

        @Override
        long getBitMask() {
            throw new UnsupportedOperationException();
        }
    }
}
