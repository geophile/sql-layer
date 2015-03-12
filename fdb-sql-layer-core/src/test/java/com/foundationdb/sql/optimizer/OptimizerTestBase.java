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
package com.foundationdb.sql.optimizer;

import com.foundationdb.server.types.service.TypesRegistryServiceImpl;
import com.foundationdb.server.SchemaFactory;
import com.foundationdb.sql.compiler.ASTTransformTestBase;
import com.foundationdb.sql.compiler.BooleanNormalizer;
import com.foundationdb.sql.parser.SQLParser;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.server.service.ServiceManager;

import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.service.text.FullTextIndexService;
import com.foundationdb.server.service.text.FullTextIndexServiceImpl;

import org.junit.Before;
import org.junit.Ignore;

import java.io.File;
import java.util.Collections;
import java.util.List;

@Ignore
public class OptimizerTestBase extends ASTTransformTestBase
{
    
    protected OptimizerTestBase(String caseName, String sql, 
                                String expected, String error) {
        super(caseName, sql, expected, error);
    }

    public static final File RESOURCE_DIR = 
        new File("src/test/resources/"
                 + OptimizerTestBase.class.getPackage().getName().replace('.', '/'));
    public static final String DEFAULT_SCHEMA = "test";

    // Base class has all possible transformers for convenience.
    protected AISBinder binder;
    protected TypeComputer typeComputer;
    protected BooleanNormalizer booleanNormalizer;
    protected SubqueryFlattener subqueryFlattener;
    protected DistinctEliminator distinctEliminator;


    @Before
    public void makeTransformers() throws Exception {
        parser = new SQLParser();
        parser.setNodeFactory(new BindingNodeFactory(parser.getNodeFactory()));
        unparser = new BoundNodeToString();
        typeComputer = new FunctionsTypeComputer(TypesRegistryServiceImpl.createRegistryService());
        booleanNormalizer = new BooleanNormalizer(parser);
        subqueryFlattener = new SubqueryFlattener(parser);
        distinctEliminator = new DistinctEliminator(parser);
    }

    public static AkibanInformationSchema parseSchema(List<File> ddls) throws Exception {
        StringBuilder ddlBuilder = new StringBuilder();
        for (File ddl : ddls)
            ddlBuilder.append(fileContents(ddl));
        String sql = ddlBuilder.toString();
        SchemaFactory schemaFactory = new SchemaFactory(DEFAULT_SCHEMA);
        AkibanInformationSchema ret = schemaFactory.ais(sql);
        return ret;
    }

    public static AkibanInformationSchema parseSchema(File ddl) throws Exception {
        return parseSchema(Collections.singletonList(ddl));
    }

    protected AkibanInformationSchema loadSchema(List<File> ddl) throws Exception {
        AkibanInformationSchema ais = parseSchema(ddl);
        binder = new AISBinder(ais, DEFAULT_SCHEMA);
        if (!ais.getViews().isEmpty())
            new TestBinderContext(parser, binder, typeComputer);
        return ais;
    }

    protected AkibanInformationSchema loadSchema(File ddl) throws Exception {
        return loadSchema(Collections.singletonList(ddl));
    }

    protected static class TestBinderContext extends AISBinderContext {
        public TestBinderContext(SQLParser parser, AISBinder binder, TypeComputer typeComputer) {
            this.parser = parser;
            this.defaultSchemaName = DEFAULT_SCHEMA;
            setBinderAndTypeComputer(binder, typeComputer);
        }
    }

     
    protected static ServiceManager createServiceManager()
    {
        return new GuicedServiceManager(serviceBindingsProvider());
    }

    protected static GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider()
    {
        return GuicedServiceManager
                    .testUrls()
                        .bindAndRequire(FullTextIndexService.class,
                                        FullTextIndexServiceImpl.class);
    }
}
