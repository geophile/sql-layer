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
package com.foundationdb.sql.optimizer.rule.cost;

import com.foundationdb.ais.model.*;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;

public abstract class TreeStatistics
{
    public abstract long rowCount();

    public abstract int rowWidth();

    public abstract RowType rowType();

    public static TreeStatistics forTable(TableRowType rowType, TableRowCounts tableRowCounts)
    {
        return new TableStatistics(rowType, tableRowCounts);
    }

    public static TreeStatistics forIndex(IndexRowType rowType, TableRowCounts tableRowCounts)
    {
        return 
            rowType.index().isGroupIndex()
            ? new GroupIndexStatistics(rowType, tableRowCounts)
            : new TableIndexStatistics(rowType, tableRowCounts);
    }

    int fieldWidth(Column column)
    {
        TClass tclass = column.getType().typeClass();
        if (tclass.hasFixedSerializationSize()) {
            if (tclass instanceof MNumeric) {
                return 8;       // TODO: For compatibility with existing tests.
            }
            return tclass.fixedSerializationSize();
        }
        if (tclass instanceof TString) {
            int length = ((TString)tclass).getFixedLength();
            if (length < 0) {
                return (int)(column.getAverageStorageSize() * PLAUSIBLE_AVERAGE_VAR_USAGE);
            }
            else {
                return PLAUSIBLE_AVERAGE_BLOB_SIZE;
            }
        }
        if (tclass instanceof TBinary) {
            int length = ((TBinary)tclass).getDefaultLength();
            if (length < 0) {
                return (int)(column.getAverageStorageSize() * PLAUSIBLE_AVERAGE_VAR_USAGE);
            }
            else {
                return PLAUSIBLE_AVERAGE_BLOB_SIZE;
            }
        }
        return (int)column.getAverageStorageSize();
    }

    private static final int PLAUSIBLE_AVERAGE_BLOB_SIZE = 100000;
    private static final double PLAUSIBLE_AVERAGE_VAR_USAGE = 0.3;

    protected TreeStatistics(TableRowCounts tableRowCounts) {
        this.tableRowCounts = tableRowCounts;
    }

    protected final TableRowCounts tableRowCounts;

    private static class TableStatistics extends TreeStatistics
    {
        public long rowCount()
        {
            return tableRowCounts.getTableRowCount(rowType.table());
        }

        @Override
        public int rowWidth()
        {
            // Columns
            int rowWidth = 0;
            for (Column column : rowType.table().getColumnsIncludingInternal()) {
                rowWidth += fieldWidth(column);
            }
            // Attempt to estimate hkey width
            for (HKeySegment segment : rowType().table().hKey().segments()) {
                // ordinal
                rowWidth += 1;
                for (HKeyColumn hKeyColumn : segment.columns()) {
                    rowWidth += fieldWidth(hKeyColumn.column());
                }
            }            
            return rowWidth;
        }

        public RowType rowType()
        {
            return rowType;
        }

        public TableStatistics(TableRowType rowType, TableRowCounts tableRowCounts)
        {
            super(tableRowCounts);
            this.rowType = rowType;
        }

        private final TableRowType rowType;
    }
    
    private static abstract class IndexStatistics extends TreeStatistics
    {
        @Override
        public final RowType rowType()
        {
            return rowType;
        }

        @Override
        public final int rowWidth()
        {
            int rowWidth = 0;
            Index index = rowType.index();
            for (IndexColumn indexColumn : index.getAllColumns()) {
                rowWidth += fieldWidth(indexColumn.getColumn());
            }
            return rowWidth;
        }

        protected IndexStatistics(IndexRowType rowType, TableRowCounts tableRowCounts)
        {
            super(tableRowCounts);
            this.rowType = rowType;
        }
        
        protected final IndexRowType rowType;
    }

    private static class TableIndexStatistics extends IndexStatistics
    {
        public long rowCount()
        {
            TableIndex index = (TableIndex) rowType.index();
            Table table = index.getTable();
            return tableRowCounts.getTableRowCount(table);
        }

        public TableIndexStatistics(IndexRowType rowType, TableRowCounts tableRowCounts)
        {
            super(rowType, tableRowCounts);
        }
    }

    private static class GroupIndexStatistics extends IndexStatistics
    {
        public long rowCount()
        {
            GroupIndex index = (GroupIndex) rowType.index();
            return tableRowCounts.getTableRowCount(index.leafMostTable());
        }

        public GroupIndexStatistics(IndexRowType rowType, TableRowCounts tableRowCounts)
        {
            super(rowType, tableRowCounts);
        }
    }
}
