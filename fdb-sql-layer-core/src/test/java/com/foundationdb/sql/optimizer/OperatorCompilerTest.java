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


import com.foundationdb.junit.SelectedParameterizedRunner;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.sql.NamedParamsTestBase;
import com.foundationdb.sql.TestBase;

import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.SQLParser;

import com.foundationdb.sql.optimizer.plan.BasePlannable;
import com.foundationdb.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.foundationdb.sql.optimizer.plan.ResultSet.ResultField;
import com.foundationdb.sql.optimizer.rule.ExplainPlanContext;
import com.foundationdb.sql.optimizer.rule.RulesTestHelper;
import com.foundationdb.sql.optimizer.rule.PipelineConfiguration;
import com.foundationdb.sql.optimizer.rule.cost.TestCostEstimator;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.server.explain.format.DefaultFormatter;
import com.foundationdb.server.types.service.TypesRegistryServiceImpl;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;

@RunWith(SelectedParameterizedRunner.class)
public class OperatorCompilerTest extends NamedParamsTestBase 
                                  implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "operator");

    protected File schemaFile, statsFile, propertiesFile;
    protected SQLParser parser;
    protected OperatorCompiler compiler;

    @Before
    public void makeCompiler() throws Exception {
        parser = new SQLParser();
        AkibanInformationSchema ais = OptimizerTestBase.parseSchema(schemaFile);
        Properties properties = new Properties();
        if (propertiesFile != null) {
            FileInputStream fstr = new FileInputStream(propertiesFile);
            try {
                properties.load(fstr);
            }
            finally {
                fstr.close();
            }
        }
        compiler = TestOperatorCompiler.create(parser, ais, statsFile, properties);
    }

    static class TestResultColumn extends PhysicalResultColumn {
        private String type;

        public TestResultColumn(String name, String type) {
            super(name);
            this.type = type;
        }

        public String getType() {
            return type;
        }

        @Override
        public String toString() {
            return getName() + ":" + getType();
        }
    }
    
    public static class TestOperatorCompiler extends OperatorCompiler {
        private TestOperatorCompiler() {
        }

        public static TestOperatorCompiler create(SQLParser parser, 
                                                  AkibanInformationSchema ais, 
                                                  File statsFile,
                                                  Properties properties) 
                throws IOException {
            RulesTestHelper.ensureFieldAssociations(ais);
            TestOperatorCompiler compiler = new TestOperatorCompiler();
            compiler.initProperties(properties);
            compiler.initAIS(ais, OptimizerTestBase.DEFAULT_SCHEMA);
            compiler.initParser(parser);
            compiler.initCostEstimator(new TestCostEstimator(ais, compiler.getSchema(), statsFile, false, properties));
            compiler.initPipelineConfiguration(new PipelineConfiguration());

            TypesRegistryServiceImpl typesRegistry = new TypesRegistryServiceImpl();
            typesRegistry.start();
            compiler.initTypesRegistry(typesRegistry);
            TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;
            compiler.initTypesTranslator(typesTranslator);

            compiler.initDone();
            return compiler;
        }

        @Override
        public PhysicalResultColumn getResultColumn(ResultField field) {
            String type = String.valueOf(field.getSQLtype());
            if (field.getType() != null) {
                type = field.getType().toStringConcise(true);
            }
            Column column = field.getAIScolumn();
            if (column != null) {
                type = column.getTypeDescription();
            }
            return new TestResultColumn(field.getName(), type);
        }
    }

    @Parameterized.Parameters(name="{0}")
    public static Iterable<Object[]> statements() throws Exception {
        Collection<Object[]> result = new ArrayList<>();
        for (File subdir : RESOURCE_DIR.listFiles(new FileFilter() {
                public boolean accept(File file) {
                    return file.isDirectory();
                }
            })) {
            File schemaFile = new File(subdir, "schema.ddl");
            if (schemaFile.exists()) {
                File statsFile = new File(subdir, "stats.yaml");
                if (!statsFile.exists())
                    statsFile = null;
                File compilerPropertiesFile = new File(subdir, "compiler.properties");
                if (!compilerPropertiesFile.exists())
                    compilerPropertiesFile = null;
                for (Object[] args : sqlAndExpected(subdir)) {
                    File propertiesFile = new File(subdir, args[0] + ".properties");
                    if (!propertiesFile.exists())
                        propertiesFile = compilerPropertiesFile;
                    Object[] nargs = new Object[args.length+3];
                    nargs[0] = subdir.getName() + "/" + args[0];
                    nargs[1] = schemaFile;
                    nargs[2] = statsFile;
                    nargs[3] = propertiesFile;
                    System.arraycopy(args, 1, nargs, 4, args.length-1);
                    result.add(nargs);
                }
            }
        }
        return result;
    }

    public OperatorCompilerTest(String caseName, 
                                File schemaFile, File statsFile, File propertiesFile,
                                String sql, String expected, String error) {
        super(caseName, sql, expected, error);
        this.schemaFile = schemaFile;
        this.statsFile = statsFile;
        this.propertiesFile = propertiesFile;
    }

    @Test
    public void testOperator() throws Exception {
        generateAndCheckResult();
    }

    @Override
    public String generateResult() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        ExplainPlanContext context = new ExplainPlanContext(compiler, new SimpleQueryContext(null));
        BasePlannable result = compiler.compile((DMLStatementNode)stmt, 
                                                parser.getParameterList(), context);
        return result.explainToString(context.getExplainContext(), OptimizerTestBase.DEFAULT_SCHEMA, DefaultFormatter.LevelOfDetail.VERBOSE_WITHOUT_COST);
    }

    @Override
    public void checkResult(String result) throws IOException{
        assertEqualsWithoutHashes(caseName, expected, result);
    }
}
