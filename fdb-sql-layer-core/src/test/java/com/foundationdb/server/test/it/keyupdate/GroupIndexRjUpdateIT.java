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
package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.row.Row;
import org.junit.Test;

public final class GroupIndexRjUpdateIT extends GIUpdateITBase {

    @Test
    public void placeholderNoOrphan() {
        final Row r1, r2;
        groupIndex("c.name", "o.when");
        writeAndCheck(
                r1 = row(c, 1L, "Bergy")
        );
        writeAndCheck(
                r2 = row(o, 10L, 1L, "01-01-2001"),
                "Bergy, 01-01-2001, 1, 10 => " + containing(c, o)
        );
        deleteAndCheck(r2);
        deleteAndCheck(r1);
    }

    @Test
    public void placeholderWithOrphan() {
        final Row r1, r2;
        groupIndex("c.name", "o.when");
        writeAndCheck(
                r1 = row(o, 10L, 1L, "01-01-2001"),
                "null, 01-01-2001, 1, 10 => " + containing(o)
        );
        writeAndCheck(
                r2 = row(c, 1L, "Bergy"),
                "Bergy, 01-01-2001, 1, 10 => " + containing(c, o)
        );
        deleteAndCheck(
                r2,
                "null, 01-01-2001, 1, 10 => " + containing(o)
        );
        deleteAndCheck(r1);
    }

    @Test
    public void coiNoOrphan() {
        groupIndex("c.name", "o.when", "i.sku");

        writeAndCheck(
                row(c, 1L, "Horton")
        );
        writeAndCheck(
                row(o, 11L, 1L, "01-01-2001")
        );
        writeAndCheck(
                row(i, 101L, 11L, 1111),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing(c, o, i)
        );
        writeAndCheck(
                row(i, 102L, 11L, 2222),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + containing(c, o, i)
        );
        writeAndCheck(
                row(i, 103L, 11L, 3333),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + containing(c, o, i),
                "Horton, 01-01-2001, 3333, 1, 11, 103 => " + containing(c, o, i)
        );
        writeAndCheck(
                row(o, 12L, 1L, "02-02-2002"),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + containing(c, o, i),
                "Horton, 01-01-2001, 3333, 1, 11, 103 => " + containing(c, o, i)
        );

        writeAndCheck(row(a, 10001L, 1L, "Causeway"),
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-2001, 2222, 1, 11, 102 => " + containing(c, o, i),
                "Horton, 01-01-2001, 3333, 1, 11, 103 => " + containing(c, o, i)
        );


        // update parent
        updateAndCheck(
                row(o, 11L, 1L, "01-01-2001"),
                row(o, 11L, 1L, "01-01-1999"), // party!
                "Horton, 01-01-1999, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-1999, 2222, 1, 11, 102 => " + containing(c, o, i),
                "Horton, 01-01-1999, 3333, 1, 11, 103 => " + containing(c, o, i)
        );
        // update child
        updateAndCheck(
                row(i, 102L, 11L, 2222),
                row(i, 102L, 11L, 2442),
                "Horton, 01-01-1999, 1111, 1, 11, 101 => " + containing(c, o, i),
                "Horton, 01-01-1999, 2442, 1, 11, 102 => " + containing(c, o, i),
                "Horton, 01-01-1999, 3333, 1, 11, 103 => " + containing(c, o, i)
        );

        // delete order
        deleteAndCheck(
                row(o, 11L, 1L, "01-01-1999"),
                "null, null, 1111, null, 11, 101 => " + containing(i),
                "null, null, 2442, null, 11, 102 => " + containing(i),
                "null, null, 3333, null, 11, 103 => " + containing(i)
        );
        // delete item
        deleteAndCheck(
                row(i, 102L, 11L, 222211),
                "null, null, 1111, null, 11, 101 => " + containing(i),
                "null, null, 3333, null, 11, 103 => " + containing(i)
        );
    }

    @Test
    public void createGIOnFullyPopulatedTables() {
        writeRows(
                row(c, 1L, "Horton"),
                row(o, 11L, 1L, "01-01-2001"),
                row(i, 101L, 11L, 1111)
        );
        groupIndexNamed("name_when_sku", "c.name", "o.when", "i.sku");
        checkIndex("name_when_sku",
                "Horton, 01-01-2001, 1111, 1, 11, 101 => " + containing("name_when_sku", c, o, i)
        );
    }

    @Test
    public void createGIOnPartiallyPopulatedTablesFromLeaf() {
        writeRows(
                row(i, 101L, 11L, 1111)
        );
        groupIndexNamed("name_when_sku", "c.name", "o.when", "i.sku");
        checkIndex("name_when_sku",
                "null, null, 1111, null, 11, 101 => " + containing("name_when_sku", i)
        );
    }

    @Test
    public void createGiOnPartiallyPopulatedTablesFromMiddle() {
        writeRows(
                row(o, 11L, 1L, "01-01-2001"),
                row(i, 101L, 11L, 1111)
        );
        groupIndexNamed("when_sku", "o.when", "i.sku");
        checkIndex("when_sku",
                "01-01-2001, 1111, 1, 11, 101 => " + containing("when_sku", o, i)
        );
    }

    @Test
    public void ihIndexNoOrphans() {
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(c, 1L, "Horton"),
                row(o, 11L, 1L, "01-01-2001"),
                row(i, 101L, 11L, 1111),
                row(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName, "1111, handle with care, 1, 11, 101, 1001 => " + containing(i, h));

        // delete from root on up
        deleteRow(c, 1L, "Horton");
        checkIndex(indexName, "1111, handle with care, 1, 11, 101, 1001 => " + containing(i, h));

        deleteRow(o, 11L, 1L, "01-01-2001 => " + containing(i, h));
        checkIndex(indexName, "1111, handle with care, null, 11, 101, 1001 => " + containing(i, h));

        deleteRow(i, 101L, 11L, 1111);
        checkIndex(indexName, "null, handle with care, null, null, 101, 1001 => " + containing(h));

        deleteRow(h, 1001L, 101L, "handle with care");
        checkIndex(indexName);
    }

    @Test
    public void adoptionChangesHKeyNoCustomer() {
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(i, 101L, 11L, 1111),
                row(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName,
                "1111, handle with care, null, 11, 101, 1001 => " + containing(i, h)
        );

        // bring an o that adopts the i
        final Row oRow;
        writeAndCheck(
                oRow = row(o, 11L, 1L, "01-01-2001"),
                "1111, handle with care, 1, 11, 101, 1001 => " + containing(i, h)
        );
        deleteAndCheck(
                oRow,
                "1111, handle with care, null, 11, 101, 1001 => " + containing(i, h)
        );
    }

    @Test
    public void adoptionChangesHKeyWithC() {
        String indexName = groupIndex("i.sku", "h.handling_instructions");
        writeRows(
                row(c, 1L, "Horton"),
                row(i, 101L, 11L, 1111),
                row(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName,
                "1111, handle with care, null, 11, 101, 1001 => " + containing(i, h)
        );

        // bring an o that adopts the i
        writeRow(o, 11L, 1L, "01-01-2001");
        checkIndex(indexName,
                "1111, handle with care, 1, 11, 101, 1001 => " + containing(i, h)
        );
    }
    @Test
    public void updateModifiesHKeyWithinBranch() {
        // branch is I-H, we're modifying the hkey of an H
        groupIndex("i.sku", "h.handling_instructions");
        writeAndCheck(row(c, 1L, "Horton"));

        writeAndCheck(row(o, 11L, 1L, "01-01-2001"));

        writeAndCheck(row(i, 101L, 11L, "1111"));

        writeAndCheck(
                row(h, 1001L, 101L, "don't break"),
                "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h)
        );

        writeAndCheck(
                row(c, 2L, "David"),
                "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h)
        );

        writeAndCheck(
                row(o, 12L, 2L, "02-02-2002"),
                "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h)
        );

        writeAndCheck(
                row(h, 1002L, 102L, "do break"),
                "null, do break, null, null, 102, 1002 => " + containing(h),
                "1111, don't break, 1, 11, 101, 1001 => " + containing(i, h)
        );

        updateAndCheck(
                row(i, 101L, 11L, "1111"),
                row(i, 102L, 12L, "2222"),
                "null, don't break, null, null, 101, 1001 => " + containing(h),
                "2222, do break, 2, 12, 102, 1002 => " + containing(i, h)
        );
    }

    public GroupIndexRjUpdateIT() {
        super(Index.JoinType.RIGHT);
    }
}
