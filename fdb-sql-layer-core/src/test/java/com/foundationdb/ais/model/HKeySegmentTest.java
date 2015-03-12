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

import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class HKeySegmentTest {

    @Test
    public void nonCascading() {
        AkibanInformationSchema ais = AISBBasedBuilder.create(SCHEMA, MTypesTranslator.INSTANCE)
                .table("c")
                    .colInt("cid")
                    .colString("name", 64)
                    .pk("cid")
                .table("o")
                    .colInt("oid")
                    .colInt("c_id")
                    .colString("date", 32)
                    .pk("oid")
                    .joinTo("c").on("c_id", "cid")
                .table("i")
                    .colInt("iid")
                    .colInt("o_id")
                    .colInt("sku")
                    .pk("iid")
                    .joinTo("o").on("o_id", "oid")
                .ais();

        checkHKeyColumn(
                ais, "c",
                expectedSegments(1).checkSegment(0).expectedCols(1).checkCol(0).expectAtIndex(0),
                c("c", "cid"), c("o", "c_id"));

        checkHKeyColumn(
                ais, "o",
                expectedSegments(2).checkSegment(0).expectedCols(1).checkCol(0).expectAtIndex(1),
                c("c", "cid"), c("o", "c_id"));
        checkHKeyColumn(
                ais, "o",
                expectedSegments(2).checkSegment(1).expectedCols(1).checkCol(0).expectAtIndex(0),
                c("o", "oid"), c("i", "o_id"));

        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(0).expectedCols(1).checkCol(0).expectAtIndex(1),
                c("c", "cid"), c("o", "c_id"));
        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(1).expectedCols(1).checkCol(0).expectAtIndex(1),
                c("o", "oid"), c("i", "o_id"));
        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(2).expectedCols(1).checkCol(0).expectAtIndex(0),
                c("i", "iid"));
    }

    @Test
    public void cascading() {
        AkibanInformationSchema ais = AISBBasedBuilder.create(SCHEMA, MTypesTranslator.INSTANCE)
                .table("c")
                    .colInt("cid")
                    .colString("name", 64)
                    .pk("cid")
                .table("o")
                    .colInt("c_id")
                    .colInt("oid")
                    .colString("date", 32)
                    .pk("c_id", "oid")
                    .joinTo("c").on("c_id", "cid")
                .table("i")
                    .colInt("c__id")
                    .colInt("o_id")
                    .colInt("iid")
                    .colInt("sku")
                    .pk("c__id", "o_id", "iid")
                    .joinTo("o").on("c__id","c_id").and("o_id", "oid")
                .ais();

        checkHKeyColumn(
                ais, "c",
                expectedSegments(1).checkSegment(0).expectedCols(1).checkCol(0).expectAtIndex(0),
                c("c", "cid"), c("o", "c_id"), c("i", "c__id"));

        checkHKeyColumn(
                ais, "o",
                expectedSegments(2).checkSegment(0).expectedCols(1).checkCol(0).expectAtIndex(1),
                c("c", "cid"), c("o", "c_id"), c("i", "c__id"));
        checkHKeyColumn(
                ais, "o",
                expectedSegments(2).checkSegment(1).expectedCols(1).checkCol(0).expectAtIndex(0),
                c("o", "oid"), c("i", "o_id"));

        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(0).expectedCols(1).checkCol(0).expectAtIndex(2),
                c("c", "cid"), c("o", "c_id"), c("i", "c__id"));
        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(1).expectedCols(1).checkCol(0).expectAtIndex(1),
                c("o", "oid"), c("i", "o_id"));
        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(2).expectedCols(1).checkCol(0).expectAtIndex(0),
                c("i", "iid"));
    }

    @Test
    public void multiColumnPkNoCascade() {
        AkibanInformationSchema ais = AISBBasedBuilder.create(SCHEMA, MTypesTranslator.INSTANCE)
                .table("c")
                    .colInt("cid1")
                    .colInt("cid2")
                    .colString("name", 64)
                    .pk("cid1", "cid2")
                .table("o")
                    .colInt("oid")
                    .colInt("c_id1")
                    .colInt("c_id2")
                    .colString("date", 32)
                    .pk("oid")
                    .joinTo("c").on("c_id1", "cid1").and("c_id2", "cid2")
                .table("i")
                    .colInt("iid")
                    .colInt("o_id")
                    .colInt("sku")
                    .pk("iid")
                    .joinTo("o").on("o_id", "oid")
                .ais();

        checkHKeyColumn(
                ais, "c",
                expectedSegments(1).checkSegment(0).expectedCols(2).checkCol(0).expectAtIndex(0),
                c("c", "cid1"), c("o", "c_id1"));
        checkHKeyColumn(
                ais, "c",
                expectedSegments(1).checkSegment(0).expectedCols(2).checkCol(1).expectAtIndex(0),
                c("c", "cid2"), c("o", "c_id2"));

        checkHKeyColumn(
                ais, "o",
                expectedSegments(2).checkSegment(0).expectedCols(2).checkCol(0).expectAtIndex(1),
                c("c", "cid1"), c("o", "c_id1"));
        checkHKeyColumn(
                ais, "o",
                expectedSegments(2).checkSegment(0).expectedCols(2).checkCol(1).expectAtIndex(1),
                c("c", "cid2"), c("o", "c_id2"));
        checkHKeyColumn(
                ais, "o",
                expectedSegments(2).checkSegment(1).expectedCols(1).checkCol(0).expectAtIndex(0),
                c("o", "oid"), c("i", "o_id"));

        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(0).expectedCols(2).checkCol(0).expectAtIndex(1),
                c("c", "cid1"), c("o", "c_id1"));
        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(0).expectedCols(2).checkCol(1).expectAtIndex(1),
                c("c", "cid2"), c("o", "c_id2"));
        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(1).expectedCols(1).checkCol(0).expectAtIndex(1),
                c("o", "oid"), c("i", "o_id"));
        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(2).expectedCols(1).checkCol(0).expectAtIndex(0),
                c("i", "iid"));
    }

    private void checkHKeyColumn(
                AkibanInformationSchema ais, String tableName,
                                 SegmentCheckParameters segmentParameters,
                                 ColumnName... equivalentColumns)
    {
        List<HKeySegment> segments = ais.getTable(SCHEMA, tableName).hKey().segments();
        assertEquals("segments size for " + segments, segmentParameters.expectedSegments, segments.size());
        List<HKeyColumn> hKeyColumns = segments.get(segmentParameters.checkSegment).columns();
        assertEquals("hKeyColumns size", segmentParameters.expectedColumns, hKeyColumns.size());
        HKeyColumn hKeyColumn = hKeyColumns.get(segmentParameters.checkColumn);
        checkColumnName(hKeyColumn.column(), equivalentColumns[segmentParameters.mainColumnIndex]);
        checkEquivalentColumns(Arrays.asList(equivalentColumns), hKeyColumn.equivalentColumns());
    }

    private void checkEquivalentColumns(List<ColumnName> expected, List<Column> actual) {
        List<ColumnName> actualNames = new ArrayList<>();
        for (Column column : actual) {
            actualNames.add(new ColumnName(column));
        }
        assertEquals("equivalent columns", expected, actualNames);
    }

    private static void checkColumnName(Column column, ColumnName expected) {
        ColumnName actual = new ColumnName(column);
        assertEquals("column name", expected,  actual);
    }

    private static ColumnName c(String table, String column) {
        return new ColumnName(SCHEMA, table, column);
    }
    
    private static SegmentCheckParameters expectedSegments(int expectedSegments) {
        return new SegmentCheckParameters().expectedSegments(expectedSegments);
    }

    // consts
    private static final String SCHEMA = "hkeytest";

    // nested classes

    private static class SegmentCheckParameters {
        /**
         * How many HKeySegments we expect in the HKey
         * @param expectedSegments expected size of {@link HKey#segments()}
         * @return this object (for chaining)
         */
        SegmentCheckParameters expectedSegments(int expectedSegments) {
            this.expectedSegments = expectedSegments;
            return this;
        }

        /**
         * Which segment to check ( must be between 0 and expectedSegments )
         * @param checkSegment the index of {@link HKey#segments()} to check
         * @return this object (for chaining)
         */
        SegmentCheckParameters checkSegment(int checkSegment) {
            this.checkSegment = checkSegment;
            return this;
        }

        /**
         * How many columns we expect in this segment
         * @param expectedColumns the expected size of {@link HKeySegment#columns}
         * @return this object (for chaining)
         */
        SegmentCheckParameters expectedCols(int expectedColumns) {
            this.expectedColumns = expectedColumns;
            return this;
        }

        /**
         * Which column to check
         * @param checkColumn the index of {@link HKeySegment#columns} to check
         * @return this object (for chaining)
         */
        SegmentCheckParameters checkCol(int checkColumn) {
            this.checkColumn = checkColumn;
            return this;
        }

        /**
         * Where in the equivalent columns list we expect the "main" column to be. In other words, we'll check that
         * {@link HKeyColumn#column()} is the same as the entry of the given index in
         * {@link HKeyColumn#equivalentColumns()}.
         * @param mainColumnIndex the index within {@linkplain HKeyColumn#equivalentColumns()} to expect to find
         * {@link HKeyColumn#column()}
         * @return this object (for chaining)
         */
        SegmentCheckParameters expectAtIndex(int mainColumnIndex) {
            this.mainColumnIndex = mainColumnIndex;
            return this;
        }

        private int expectedSegments = -1;
        private int checkSegment = -1;
        private int expectedColumns = -1;
        private int checkColumn = -1;
        private int mainColumnIndex = -1;
    }
}
