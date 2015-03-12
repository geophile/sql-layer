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
package com.foundationdb.sql.aisddl;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import com.foundationdb.server.error.ReferencedSchemaException;
import org.junit.Test;
import org.junit.Before;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.CreateSchemaNode;
import com.foundationdb.sql.parser.DropSchemaNode;
import com.foundationdb.server.error.DuplicateSchemaException;
import com.foundationdb.server.error.NoSuchSchemaException;
import com.foundationdb.server.types.service.TestTypesRegistry;
import com.foundationdb.server.types.service.TypesRegistry;

public class SchemaDDLTest {

    @Before
    public void before() throws Exception {
        parser = new SQLParser();
    }

    @Test
    public void createSchemaEmpty () throws Exception
    {
        String sql = "CREATE SCHEMA EMPTY";
        AkibanInformationSchema ais = new AkibanInformationSchema();
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateSchemaNode);
        
        SchemaDDL.createSchema(ais, null, (CreateSchemaNode)stmt, null);
    }
    
    @Test (expected=DuplicateSchemaException.class)
    public void createSchemaUsed() throws Exception
    {
        String sql = "CREATE SCHEMA S";
        AkibanInformationSchema ais = factory();
        
        assertNotNull(ais.getTable("s", "t"));
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateSchemaNode);
        
        SchemaDDL.createSchema(ais, null, (CreateSchemaNode)stmt, null);
    }
    
      
    @Test //(expected=DuplicateSchemaException.class)
    public void createNonDuplicateSchemaWithIfNotExist() throws Exception
    {
        String sql = "CREATE SCHEMA IF NOT EXISTS SS";
        AkibanInformationSchema ais = factory();
        
        assertNotNull(ais.getTable("s", "t"));
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateSchemaNode);
        
        SchemaDDL.createSchema(ais, null, (CreateSchemaNode)stmt, null);
    }
        
    @Test
    public void createDuplicateSchemaWithIfNotExist() throws Exception
    {
        String sql = "CREATE SCHEMA IF NOT EXISTS S";
        AkibanInformationSchema ais = factory();
        
        assertNotNull(ais.getTable("s", "t"));
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateSchemaNode);
        
        SchemaDDL.createSchema(ais, null, (CreateSchemaNode)stmt, null);
    }

    @Test (expected=NoSuchSchemaException.class)
    public void dropSchemaEmpty() throws Exception 
    {
        String sql = "DROP SCHEMA EMPTY RESTRICT";
        AkibanInformationSchema ais = new AkibanInformationSchema();
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropSchemaNode);
        
        DDLFunctions ddlFunctions = new TableDDLTest.DDLFunctionsMock(ais);
        
        SchemaDDL.dropSchema(ddlFunctions, null, (DropSchemaNode)stmt, null);
    }

    @Test
    public void dropNonExistingSchemaWithIfExists() throws Exception
    {
        String sql = "DROP SCHEMA IF EXISTS AA";
        AkibanInformationSchema ais = new AkibanInformationSchema();
        
        StatementNode node = parser.parseStatement(sql);
        assertTrue(node instanceof DropSchemaNode);
        
        DDLFunctions ddlFunctions = new TableDDLTest.DDLFunctionsMock(ais);
        
        SchemaDDL.dropSchema(ddlFunctions, null, (DropSchemaNode)node, null);
    }

    @Test
    public void dropSchemaEmptyIfExists() throws Exception 
    {
        String sql = "DROP SCHEMA IF EXISTS EMPTY RESTRICT";
        AkibanInformationSchema ais = new AkibanInformationSchema();
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropSchemaNode);
        
        DDLFunctions ddlFunctions = new TableDDLTest.DDLFunctionsMock(ais);
        
        SchemaDDL.dropSchema(ddlFunctions, null, (DropSchemaNode)stmt, null);
    }

    @Test(expected=ReferencedSchemaException.class)
    public void dropSchemaUsed() throws Exception
    {
        String sql = "DROP SCHEMA S RESTRICT";
        AkibanInformationSchema ais = factory();
        
        assertNotNull(ais.getTable("s", "t"));
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropSchemaNode);
        DDLFunctions ddlFunctions = new TableDDLTest.DDLFunctionsMock(ais);
        
        SchemaDDL.dropSchema(ddlFunctions, null, (DropSchemaNode)stmt, null);
    }

    protected SQLParser parser;

    private AkibanInformationSchema factory () throws Exception
    {
        TypesRegistry typesRegistry = TestTypesRegistry.MCOMPAT;
        TestAISBuilder builder = new TestAISBuilder(typesRegistry);
        builder.table("s", "t");
        builder.column ("s", "t", "c1", 0, "MCOMPAT", "int", false);
        builder.pk("s", "t");
        builder.indexColumn("s", "t", "PRIMARY", "c1", 0, true, 0);
        builder.basicSchemaIsComplete();
        return builder.akibanInformationSchema();
    }
}
