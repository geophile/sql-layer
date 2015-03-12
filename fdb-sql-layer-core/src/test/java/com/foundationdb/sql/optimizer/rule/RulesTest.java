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
package com.foundationdb.sql.optimizer.rule;

import static com.foundationdb.util.FileTestUtils.printClickableFile;

import com.foundationdb.server.types.service.TypesRegistryServiceImpl;
import com.foundationdb.sql.TestBase;
import com.foundationdb.sql.optimizer.FunctionsTypeComputer;
import com.foundationdb.sql.optimizer.NestedResultSetTypeComputer;
import com.foundationdb.sql.optimizer.OptimizerTestBase;
import com.foundationdb.sql.optimizer.plan.AST;
import com.foundationdb.sql.optimizer.plan.PlanNode;
import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.junit.SelectedParameterizedRunner;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;

@RunWith(SelectedParameterizedRunner.class)
public class RulesTest extends OptimizerTestBase
                       implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "rule");

    protected File rulesFile, schemaFile, indexFile, statsFile, propertiesFile, extraDDL;

    @Parameters(name="{0}")
    public static Iterable<Object[]> statements() throws Exception {
        Collection<Object[]> result = new ArrayList<>();
        for (File subdir : RESOURCE_DIR.listFiles(new FileFilter() {
                public boolean accept(File file) {
                    return file.isDirectory();
                }
            })) {
            File rulesFile = new File (subdir, "rules.yml");
            File schemaFile = new File(subdir, "schema.ddl");
            if (rulesFile.exists() && schemaFile.exists()) {
                File defaultStatsFile = new File(subdir, "stats.yaml");
                File defaultPropertiesFile = new File(subdir, "compiler.properties");
                File defaultExtraDDL = new File(subdir, "schema-extra.ddl");
                if (!defaultStatsFile.exists())
                    defaultStatsFile = null;
                if (!defaultPropertiesFile.exists())
                    defaultPropertiesFile = null;
                if (!defaultExtraDDL.exists())
                    defaultExtraDDL = null;
                for (Object[] args : sqlAndExpected(subdir)) {
                    File statsFile = new File(subdir, args[0] + ".stats.yaml");
                    File propertiesFile = new File(subdir, args[0] + ".properties");
                    File extraDDL = new File(subdir, args[0] + ".ddl");
                    if (!statsFile.exists())
                        statsFile = defaultStatsFile;
                    if (!propertiesFile.exists())
                        propertiesFile = defaultPropertiesFile;
                    if (!extraDDL.exists())
                        extraDDL = defaultExtraDDL;
                    Object[] nargs = new Object[args.length+5];
                    nargs[0] = subdir.getName() + "/" + args[0];
                    nargs[1] = rulesFile;
                    nargs[2] = schemaFile;
                    nargs[3] = statsFile;
                    nargs[4] = propertiesFile;
                    nargs[5] = extraDDL;
                    System.arraycopy(args, 1, nargs, 6, args.length-1);
                    result.add(nargs);
                }
            }
        }
        return result;
    }

    public RulesTest(String caseName, 
                     File rulesFile, File schemaFile, File statsFile, File propertiesFile,
                     File extraDDL,
                     String sql, String expected, String error) {
        super(caseName, sql, expected, error);
        this.rulesFile = rulesFile;
        this.schemaFile = schemaFile;
        this.statsFile = statsFile;
        this.propertiesFile = propertiesFile;
        this.extraDDL = extraDDL;
    }

    protected Properties properties;
    protected RulesContext rules;

    @Before
    public void loadDDL() throws Exception {
        List<File> schemaFiles = new ArrayList<>(2);
        schemaFiles.add(schemaFile);
        if (extraDDL != null)
            schemaFiles.add(extraDDL);
        AkibanInformationSchema ais = loadSchema(schemaFiles);
        properties = new Properties();
        if (propertiesFile != null) {
            FileInputStream fstr = new FileInputStream(propertiesFile);
            try {
                properties.load(fstr);
            }
            finally {
                fstr.close();
            }
        }
        rules = RulesTestContext.create(ais, statsFile, extraDDL != null,
                                        RulesTestHelper.loadRules(rulesFile), 
                                        properties);
        // Normally set as a consequence of OutputFormat.
        if (Boolean.parseBoolean(properties.getProperty("allowSubqueryMultipleColumns",
                                                        "false"))) {
            binder.setAllowSubqueryMultipleColumns(true);
            typeComputer = new NestedResultSetTypeComputer(TypesRegistryServiceImpl.createRegistryService());
        }
        if (Boolean.parseBoolean(properties.getProperty("resultColumnsAvailableBroadly", "false"))) {
            binder.setResultColumnsAvailableBroadly(true);
        }
    }

    @Test
    public void testRules() throws Exception {
        try {
            generateAndCheckResult();
        } catch (Throwable e) {
            System.err.println("Failed Rules test (note: line number is always 1)");
            String filePathPrefix = RESOURCE_DIR + "/" + caseName;
            printClickableFile(filePathPrefix, "sql", 1);
            printClickableFile(filePathPrefix, "expected", 1);
            throw e;
        }
    }

    @Override
    public String generateResult() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        binder.bind(stmt);
        stmt = booleanNormalizer.normalize(stmt);
        typeComputer.compute(stmt);
        stmt = subqueryFlattener.flatten((DMLStatementNode)stmt);
        // Turn parsed AST into intermediate form as starting point.
        AST ast = new AST((DMLStatementNode)stmt, parser.getParameterList());
        PlanContext plan = new PlanContext(rules, ast);
        rules.applyRules(plan);
        PlanNode.SummaryConfiguration configuration = new PlanNode.SummaryConfiguration(
                Boolean.parseBoolean(properties.getProperty("showRowTypes", "false")),
                Boolean.parseBoolean(properties.getProperty("includeIndexTableNames", "false")));
        String result = plan.planString(configuration);
        if (Boolean.parseBoolean(properties.getProperty("showParameterTypes", "false")))
            result = ast.formatParameterTypes() + result;
        return result;
    }

    @Override
    public void checkResult(String result) throws IOException {
        assertEqualsWithoutHashes(caseName, expected, result);
    }

}
