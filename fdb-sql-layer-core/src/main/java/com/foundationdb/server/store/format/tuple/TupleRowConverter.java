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
package com.foundationdb.server.store.format.tuple;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.PrimaryKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.protobuf.FDBProtobuf.TupleUsage;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;
import com.foundationdb.server.types.aksql.aktypes.AkBlob;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleRowConverter
{
    private static final Logger LOG = LoggerFactory.getLogger(TupleRowConverter.class);

    static final Set<TClass> ALLOWED_CLASSES = new HashSet<>(Arrays.asList(
        MNumeric.BIGINT, MNumeric.BIGINT_UNSIGNED, MNumeric.INT, MNumeric.INT_UNSIGNED,
        MNumeric.MEDIUMINT, MNumeric.MEDIUMINT_UNSIGNED, MNumeric.SMALLINT,
        MNumeric.SMALLINT_UNSIGNED, MNumeric.TINYINT, MNumeric.TINYINT_UNSIGNED,
        MNumeric.DECIMAL, MNumeric.DECIMAL_UNSIGNED, MApproximateNumber.DOUBLE,
        MApproximateNumber.DOUBLE_UNSIGNED, MApproximateNumber.FLOAT, MApproximateNumber.FLOAT_UNSIGNED,
        MBinary.VARBINARY, MBinary.BINARY, MString.VARCHAR, MString.CHAR,
        MString.TINYTEXT, MString.TEXT,
        MString.MEDIUMTEXT, MString.LONGTEXT,
        MDateAndTime.TIMESTAMP, MDateAndTime.DATE, MDateAndTime.TIME, MDateAndTime.DATETIME,
        MDateAndTime.YEAR, AkGUID.INSTANCE, AkBool.INSTANCE, AkBlob.INSTANCE
    ));

    protected static void checkColumn(Column column, List<String> illegal) {
        if (!ALLOWED_CLASSES.contains(TInstance.tClass(column.getType()))) {
            illegal.add(column.toString());
        }
    }

    protected static void checkTable(Table table, TupleUsage usage, 
                                     List<String> illegal) {
        switch (usage) {
        case KEY_ONLY:
            PrimaryKey pkey = table.getPrimaryKeyIncludingInternal();
            for (Column column : pkey.getColumns()) {
                checkColumn(column, illegal);
            }
            break;
        case KEY_AND_ROW:
            for (Column column : table.getColumnsIncludingInternal()) {
                checkColumn(column, illegal);
            }
            break;
        }
        for (Join join : table.getChildJoins()) {
            checkTable(join.getChild(), usage, illegal);
        }
    }

    public static List<String> checkTypes(Group group, TupleUsage usage) {
        List<String> illegal = new ArrayList<>();
        checkTable(group.getRoot(), usage, illegal);
        return illegal;
    }

    public static List<String> checkTypes(Index index, TupleUsage usage) {
        List<String> illegal = new ArrayList<>();
        for (IndexColumn indexColumn : index.getKeyColumns()) {
            checkColumn(indexColumn.getColumn(), illegal);
        }
        return illegal;
    }

    public static Tuple2 tupleFromRow (Row row) {
        int nfields = row.rowType().nFields();
        assert nfields == row.rowType().table().getColumnsIncludingInternal().size() : 
             "Row Type: " + nfields + " Vs. table: " + row.rowType().table();
        Object[] objects = new Object[nfields];
        for (int i = 0; i < nfields ; i++) {
            objects[i] = ValueSources.toObject(row.value(i));
        }
        return Tuple2.from(objects);
    }
    
    public static Row tupleToRow (Tuple2 tuple, RowType rowType) {
        int nfields = rowType.nFields();
        Object[] objects = new Object[nfields];
        assert tuple.size() == nfields : "Row Type " + rowType + " does not match tuple size: " + tuple.size();
        for (int i = 0; i < nfields; i++) {
            objects[i] = tuple.get(i);
        }
        ValuesHolderRow newRow = new ValuesHolderRow (rowType, objects);
        return newRow;
    }
}
