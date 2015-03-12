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
package com.foundationdb.qp.row;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.foundationdb.qp.rowtype.*;
import com.foundationdb.server.test.ApiTestBase;
import org.junit.Test;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.SchemaFactory;
import com.foundationdb.server.types.service.TestTypesRegistry;

public class CompoundRowTest {

    @Test
    public void testFlattenRow() {
        Schema schema = caoiSchema();
        
        Table customer = schema.ais().getTable("schema", "customer");
        Table order = schema.ais().getTable("schema", "order");
        
        RowType customerType = schema.tableRowType(customer);
        RowType orderType = schema.tableRowType(order);
        
        ValuesHolderRow customerRow = new ValuesHolderRow(customerType, new Integer(1), new String ("fred"));
        ValuesHolderRow orderRow = new ValuesHolderRow(orderType, new Integer (1000), new Integer(1), new Integer(45));

        FlattenedRowType flattenType = schema.newFlattenType(customerType, orderType);
        
        FlattenedRow flattenRow = new FlattenedRow(flattenType, customerRow, orderRow, null);
        
        assertTrue(flattenRow.containsRealRowOf(customer));
        assertTrue (flattenRow.containsRealRowOf(order));
        // Can't test this because ValuesHolderRow throws UnsupportedOperationException for this check.
        //assertFalse(flattenRow.containsRealRowOf(state));
        
        //assertEquals(ApiTestBase.getLong(flattenRow, 0), Long.valueOf(1));
        assertEquals(flattenRow.value(0).getInt32(), 1);
        assertEquals(flattenRow.value(1).getString(), "fred");
        assertEquals(ApiTestBase.getLong(flattenRow, 2), Long.valueOf(1000));
        assertEquals(ApiTestBase.getLong(flattenRow, 3), Long.valueOf(1));
        assertEquals(ApiTestBase.getLong(flattenRow, 4), Long.valueOf(45));
    }
    
    @Test
    public void testProductRow() {
        Schema schema = caoiSchema();
        
        Table customer = schema.ais().getTable("schema", "customer");
        Table order = schema.ais().getTable("schema", "order");
        
        RowType customerType = schema.tableRowType(customer);
        RowType orderType = schema.tableRowType(order);
        
        ValuesHolderRow customerRow = new ValuesHolderRow(customerType, new Integer(1), new String("Fred"));
        ValuesHolderRow ordersRow = new ValuesHolderRow(orderType, new Integer(1000), new Integer(1), new Integer(45));
        
        ProductRowType productType = schema.newProductType(customerType, (TableRowType)customerType, orderType);
        
        ProductRow productRow = new ProductRow (productType, customerRow, ordersRow);
        
        assertNotNull (productRow);
       
        assertEquals(ApiTestBase.getLong(productRow, 0), Long.valueOf(1));
        assertEquals(productRow.value(1).getString(), "Fred");
        assertEquals(ApiTestBase.getLong(productRow, 2), Long.valueOf(45));
        
    }

    private Schema caoiSchema() {
        TestAISBuilder builder = new TestAISBuilder(TestTypesRegistry.MCOMPAT);
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "MCOMPAT", "int", false);
        builder.column("schema", "customer", "customer_name", 1, "MCOMPAT", "varchar", 64L, null, false);
        builder.pk("schema", "customer");
        builder.indexColumn("schema", "customer", Index.PRIMARY, "customer_id", 0, true, null);
        builder.table("schema", "order");
        builder.column("schema", "order", "order_id", 0, "MCOMPAT", "int", false);
        builder.column("schema", "order", "customer_id", 1, "MCOMPAT", "int", false);
        builder.column("schema", "order", "order_date", 2, "MCOMPAT", "int", false);
        builder.pk("schema", "order");
        builder.indexColumn("schema", "order", Index.PRIMARY, "order_id", 0, true, null);
        builder.table("schema", "item");
        builder.column("schema", "item", "item_id", 0, "MCOMPAT", "int", false);
        builder.column("schema", "item", "order_id", 1, "MCOMPAT", "int", false);
        builder.column("schema", "item", "quantity", 2, "MCOMPAT", "int", false);
        builder.pk("schema", "item");
        builder.indexColumn("schema", "item", Index.PRIMARY, "item_id", 0, true, null);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "order_id", "schema", "item", "item_id");
        builder.table("schema", "state");
        builder.column("schema", "state", "code", 0, "MCOMPAT", "varchar", 2L, null, false);
        builder.column("schema", "state", "name", 1, "MCOMPAT", "varchar", 50L, null, false);
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema");
        builder.addJoinToGroup("group", "co", 0);
        builder.addJoinToGroup("group", "oi", 0);
        builder.createGroup("state", "schema");
        builder.addTableToGroup("state", "schema", "state");
        builder.groupingIsComplete();
        
        SchemaFactory factory = new SchemaFactory ("schema");
        factory.buildTableStatusAndFieldAssociations(builder.akibanInformationSchema());
        return new Schema(builder.akibanInformationSchema());
    }
    
}
