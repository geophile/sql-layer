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
package com.foundationdb.rest.dml;

import static org.junit.Assert.assertEquals;

import com.foundationdb.server.types.service.TypesRegistryService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.format.DefaultFormatter;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.test.it.ITBase;

public class DeleteGeneratorIT extends ITBase {

    private DeleteGenerator deleteGenerator; 
    public static final String SCHEMA = "test";

    @Before
    public void start() {
        Session session = this.session();
        this.txnService().beginTransaction(session);
    }
    
    @After
    public void commit() {
        this.txnService().commitTransaction(this.session());
    }

    @Test
    public void testCDelete() {
        createTable(SCHEMA, "c",
                "cid INT PRIMARY KEY NOT NULL",
                "name VARCHAR(32)");    

        TableName table = new TableName (SCHEMA, "c");
        this.deleteGenerator = new DeleteGenerator (this.ais());
        deleteGenerator.setTypesRegistry(this.serviceManager().getServiceByClass(TypesRegistryService.class));
        Operator delete = deleteGenerator.create(table);
        
        assertEquals(
                getExplain(delete, table.getSchemaName()),
                "\n  Delete_Returning()\n"+
                "    GroupLookup_Default(Index(c.PRIMARY) -> c)\n"+
                "      IndexScan_Default(Index(c.PRIMARY), cid = $1)");
    }

    @Test 
    public void testPKNotFirst() {
        createTable (SCHEMA, "c",
                "name varchar(32) not null",
                "address varchar(64) not null",
                "cid int not null primary key");
        TableName table = new TableName (SCHEMA, "c");
        this.deleteGenerator = new DeleteGenerator (this.ais());
        deleteGenerator.setTypesRegistry(this.serviceManager().getServiceByClass(TypesRegistryService.class));
        Operator delete = deleteGenerator.create(table);
        assertEquals(
                getExplain(delete, table.getSchemaName()),
                "\n  Delete_Returning()\n"+
                "    GroupLookup_Default(Index(c.PRIMARY) -> c)\n"+
                "      IndexScan_Default(Index(c.PRIMARY), cid = $1)");
    }

    @Test
    public void testPKMultiColumn() {
        createTable(SCHEMA, "o",
                "cid int not null",
                "oid int not null",
                "items int not null",
                "primary key (cid, oid)");
        TableName table = new TableName (SCHEMA, "o");
        this.deleteGenerator = new DeleteGenerator (this.ais());
        deleteGenerator.setTypesRegistry(this.serviceManager().getServiceByClass(TypesRegistryService.class));
        Operator delete = deleteGenerator.create(table);
        assertEquals(
                getExplain(delete, table.getSchemaName()),
                "\n  Delete_Returning()\n"+
                "    GroupLookup_Default(Index(o.PRIMARY) -> o)\n"+
                "      IndexScan_Default(Index(o.PRIMARY), cid = $1, oid = $2)");
    }
    
    @Test
    public void testNoPK() {
        createTable (SCHEMA, "c",
                "name varchar(32) not null",
                "address varchar(64) not null",
                "cid int not null");
        TableName table = new TableName (SCHEMA, "c");
        this.deleteGenerator = new DeleteGenerator (this.ais());
        deleteGenerator.setTypesRegistry(this.serviceManager().getServiceByClass(TypesRegistryService.class));
        Operator delete = deleteGenerator.create(table);
        assertEquals (
                getExplain(delete, table.getSchemaName()),
                "\n  Delete_Returning()\n"+
                "    GroupLookup_Default(Index(c.PRIMARY) -> c)\n"+
                "      IndexScan_Default(Index(c.PRIMARY), __row_id = $1)");
    }

    protected String getExplain (Operator plannable, String defaultSchemaName) {
        StringBuilder str = new StringBuilder();
        ExplainContext context = new ExplainContext(); // Empty
        DefaultFormatter f = new DefaultFormatter(defaultSchemaName);
        for (String operator : f.format(plannable.getExplainer(context))) {
            str.append("\n  ");
            str.append(operator);
        }
        return str.toString();
    }
}
