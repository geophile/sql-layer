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

import com.foundationdb.qp.row.Row;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.ais.model.Index;

public class GroupIndexCascadeUpdateIT extends GIUpdateITBase {
    private static final Logger LOG = LoggerFactory.getLogger(GroupIndexCascadeUpdateIT.class);

    @Test
    public void deleteSecondOinCO() {
        groupIndex("c.name", "o.when");
        final Row customer, firstOrder, secondOrder;
        writeAndCheck(
                customer = row(c, 1L, "Joe"),
                "Joe, null, 1, null => " + containing(c)
        );
        writeAndCheck(
                firstOrder = row(o, 11L, 1L, "01-01-01"),
                "Joe, 01-01-01, 1, 11 => " + containing(c, o)
        );
        writeAndCheck(
                secondOrder = row(o, 12L, 1L, "02-02-02"),
                "Joe, 01-01-01, 1, 11 => " + containing(c, o),
                "Joe, 02-02-02, 1, 12 => " + containing(c, o)
        );
        deleteCascadeAndCheck(
                secondOrder,
                "Joe, 01-01-01, 1, 11 => " + containing(c, o)
        );
        deleteCascadeAndCheck(
                firstOrder,
                "Joe, null, 1, null => " + containing(c)
        );
        deleteCascadeAndCheck(
                customer
        );
    }
     
    @Test
    public void deleteFromRoot() {
        String indexName = groupIndex("c.name", "o.when", "i.sku");
        writeRows(
                row(c, 2L, "David"),
                row(o, 12L, 2L, "01-01-2001"),
                row(i, 102L, 12L, 1111),
                row(h, 1002L, 102L, "handle with 2 care")
        );
        checkIndex(indexName, "David, 01-01-2001, 1111, 2, 12, 102 => " + containing(c, o, i));
        deleteRow(row(c, 2L, "David"), true);
        checkIndex(indexName);
    }
    
    @Test
    public void deleteBelowIndex() {
        String indexName = groupIndex("c.name", "o.when");
        writeRows(
                row(c, 1L, "Horton"),
                row(o, 11L, 1L, "01-01-2001"),
                row(i, 101L, 11L, 1111),
                row(h, 1001L, 101L, "handle with care")
        );
        checkIndex(indexName, "Horton, 01-01-2001, 1, 11 => " + containing(c, o));
        deleteRow(row(i, 101L, 11L, 1111), true);
        checkIndex(indexName, "Horton, 01-01-2001, 1, 11 => " + containing(c, o));
        
    }
    
    @Test
    public void multipleIndexes() {
        createLeftGroupIndex(groupName, "gi1", "c.name", "o.when");
        createLeftGroupIndex(groupName, "gi2", "i.sku", "h.handling_instructions");
        writeRows(
                row(c, 1L, "Horton"),
                row(o, 11L, 1L, "01-01-2001"),
                row(i, 101L, 11L, 1111),
                row(h, 1001L, 101L, "handle with care")
        );
        checkIndex("gi1", "Horton, 01-01-2001, 1, 11 => " + containing("gi1", c, o));
        checkIndex("gi2", "1111, handle with care, 1, 11, 101, 1001 => " + containing("gi2", i, h));
        deleteRow(row(i, 101L, 11L, 1111), true);
        checkIndex("gi1", "Horton, 01-01-2001, 1, 11 => " + containing("gi1", c, o));
        checkIndex("gi2");
        
    }
    
    @Test
    public void branches () {
        createLeftGroupIndex(groupName, "gi1", "c.name", "o.when");
        createLeftGroupIndex(groupName, "gi2", "c.name", "a.street");
        writeRows(
                row(c, 4L, "Fred"),
                row(o, 14L, 4L, "01-01-2004"),
                row(i, 104L, 14L, 1111),
                row(h, 1004L, 104L, "handle with 4 care"),
                row(a, 21L, 4L, "street with no name")
        );
        checkIndex("gi1", "Fred, 01-01-2004, 4, 14 => " + containing("gi1", c, o));
        checkIndex("gi2", "Fred, street with no name, 4, 21 => " + containing("gi2", c, a));
        deleteRow(row(c, 4L, "Fred"), true);
        checkIndex("gi1");
        checkIndex("gi2");
    }
    
    @Test
    public void testPartial1Level() {
        String indexName = groupIndex("c.name", "o.when", "i.sku");
        writeRows(
                row(c, 5L, "James"),
                row(o, 15L, 5L, "01-01-2005"),
                row(i, 105L, 15L, 1111),
                row(h, 1005L, 105L, "handle with 5 care")
        );
        checkIndex(indexName, "James, 01-01-2005, 1111, 5, 15, 105 => " + containing(c, o, i));
        deleteRow(row(i, 105L, 15L, 1111), true);
        checkIndex(indexName, "James, 01-01-2005, null, 5, 15, null => " + containing(c, o));
        
    }

    @Test
    public void testPartial2Level() {
        String indexName = groupIndex("c.name", "o.when", "i.sku");
        writeRows(
                row(c, 6L, "Larry"),
                row(o, 16L, 6L, "01-01-2006"),
                row(i, 106L, 16L, 1111),
                row(h, 1006L, 106L, "handle with 6 care")
        );
        checkIndex(indexName, "Larry, 01-01-2006, 1111, 6, 16, 106 => " + containing(c, o, i));
        deleteRow(row(o, 16L, 6L, "01-01-2006"), true);
        checkIndex(indexName, "Larry, null, null, 6, null, null => " + containing(c));
        
    }

    
    public GroupIndexCascadeUpdateIT() {
        super(Index.JoinType.LEFT);
    }
}
