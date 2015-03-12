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
package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.sql.NamedParamsTestBase;

import com.foundationdb.sql.optimizer.OptimizerTestBase;
import com.foundationdb.sql.optimizer.rule.ASTStatementLoader;
import com.foundationdb.sql.optimizer.rule.BaseRule;
import com.foundationdb.sql.optimizer.rule.PlanContext;
import com.foundationdb.sql.optimizer.rule.RulesTestContext;

import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.StatementNode;

import com.foundationdb.ais.model.AkibanInformationSchema;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import java.io.File;
import java.util.*;

@RunWith(NamedParameterizedRunner.class)
public class DuplicatePlanTest extends OptimizerTestBase
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "plan/duplicate");

    @TestParameters
    public static Collection<Parameterization> statements() throws Exception {
        Collection<Object[]> result = new ArrayList<>();
        for (File sqlFile : listSQLFiles(RESOURCE_DIR)) {
            String caseName = sqlFile.getName().replace(".sql", "");
            String sql = fileContents(sqlFile);
            result.add(new Object[] {
                           caseName, sql
                       });
        }
        return NamedParamsTestBase.namedCases(result);
    }

    private AkibanInformationSchema ais;

    public DuplicatePlanTest(String caseName, String sql) {
        super(caseName, sql, null, null);
    }

    @Before
    public void loadDDL() throws Exception {
        ais = loadSchema(new File(RESOURCE_DIR, "schema.ddl"));
    }

    @Test
    public void testDuplicatePlan() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        binder.bind(stmt);
        stmt = booleanNormalizer.normalize(stmt);
        typeComputer.compute(stmt);
        stmt = subqueryFlattener.flatten((DMLStatementNode)stmt);
        // Turn parsed AST into intermediate form.
        PlanNode plan = new AST((DMLStatementNode)stmt, parser.getParameterList());
        {
            RulesTestContext rulesContext = 
                RulesTestContext.create(ais, null, false,
                                        Collections.<BaseRule>singletonList(new ASTStatementLoader()), new Properties());
            PlanContext planContext = new PlanContext(rulesContext, plan);
            rulesContext.applyRules(planContext);
            plan = planContext.getPlan();
        }
        PlanNode duplicate = (PlanNode)plan.duplicate();
        assertFalse(plan == duplicate);
        assertEqualsWithoutHashes(caseName, 
                                  PlanToString.of(plan, PlanNode.SummaryConfiguration.DEFAULT),
                                  PlanToString.of(duplicate, PlanNode.SummaryConfiguration.DEFAULT));
    }

}
