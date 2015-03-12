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
package com.foundationdb.qp.storeadapter.indexrow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.foundationdb.ais.CAOIBuilderFiller;
import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.collation.TestKeyCreator;
import com.foundationdb.server.service.tree.KeyCreator;
import com.persistit.Key;

public class FDBIndexRowTest {

    private static class OrdinalVisitor extends AbstractVisitor {
        public int maxOrdinal = 1;

        @Override
        public void visit(Table table) {
            Integer ordinal = table.getOrdinal();
            if (ordinal == null) {
                table.setOrdinal(maxOrdinal++);
            }
        }
    }

    @Before
    public void createAIS() {
        NewAISBuilder builder = CAOIBuilderFiller.createAndFillBuilder("Test");
        ais = builder.ais();
        Table customers = ais.getTable("Test", CAOIBuilderFiller.CUSTOMER_TABLE);
        assertNotNull(customers);
        Map<Table, Integer> ordinalMap = new HashMap<>();
        List<Table> remainingTables = new ArrayList<>();
        
        ais.visit(new OrdinalVisitor());
        
        // Add all roots
        for(Table table : ais.getTables().values()) {
            if(table.isRoot()) {
                remainingTables.add(table);
            }
        }
        while(!remainingTables.isEmpty()) {
            Table table = remainingTables.remove(remainingTables.size()-1);
            ordinalMap.put(table, table.getOrdinal());
            for(Index index : table.getIndexesIncludingInternal()) {
                index.computeFieldAssociations(ordinalMap);
            }
            // Add all immediate children
            for(Join join : table.getChildJoins()) {
                remainingTables.add(join.getChild());
            }
        }
        for(Group group : ais.getGroups().values()) {
            for(Index index : group.getIndexes()) {
                index.computeFieldAssociations(ordinalMap);
            }
        }
        schema = new Schema (ais);
        customerPK = schema.indexRowType(customers.getIndex(Index.PRIMARY));
        itemPK = schema.indexRowType(ais.getTable("Test", CAOIBuilderFiller.ITEM_TABLE).getIndex(Index.PRIMARY));
        testCreator = new TestKeyCreator(schema);
    }
    
    @Test
    public void testCreation () {
        FDBIndexRow row = new FDBIndexRow (testCreator, customerPK);
        assertNotNull (row);
        assertEquals(row.rowType(), customerPK);
    }

    @Test
    public void testCopyFromEmpty() {
        Table customers = ais.getTable("Test", CAOIBuilderFiller.CUSTOMER_TABLE);
        FDBIndexRow row = new FDBIndexRow (testCreator, customerPK);

       Key pkKey = testCreator.createKey();
       
       row.copyFrom(pkKey, null);
       
       assertTrue (row.keyEmpty());
       assertNotNull (row.ancestorHKey(customers));
    }

    @Test
    public void testCopyFromEntry() {
        Table customers = ais.getTable("Test", CAOIBuilderFiller.CUSTOMER_TABLE);
        FDBIndexRow row = new FDBIndexRow (testCreator, customerPK);

       Key pkKey = testCreator.createKey();
       pkKey.append (1L);
       pkKey.append(1);
       row.copyFrom(pkKey, null);
       
       assertTrue (!row.keyEmpty());
       HKey hKey = row.ancestorHKey(customers);
       assertNotNull (hKey);
       assertTrue(hKey.segments() == 1);
       assertTrue(hKey.value(0).getInt64() == 1);
    }
    
    @Test 
    public void testCopyFromItem() {
        Table items = ais.getTable("Test", CAOIBuilderFiller.ITEM_TABLE);
        FDBIndexRow row = new FDBIndexRow (testCreator, itemPK);
        
        Key pkKey = testCreator.createKey();
        pkKey.append(15L);
        pkKey.append(7L);
        pkKey.append(5L);
        row.copyFrom (pkKey, null);
        HKey hkey = row.ancestorHKey(items);
        
        assertNotNull (hkey);
        assertEquals(hkey.value(0).getInt64(), 7L);
        assertEquals(hkey.value(1).getInt64(), 5L);
        assertEquals(hkey.value(2).getInt64(), 15L);
    }
    
    
    Schema schema;
    AkibanInformationSchema ais;
    KeyCreator testCreator;
    IndexRowType customerPK; 
    IndexRowType itemPK;
    
}
