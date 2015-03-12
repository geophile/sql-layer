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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.sql.optimizer.OptimizerTestBase;
import com.foundationdb.sql.optimizer.plan.AST;
import com.foundationdb.sql.optimizer.plan.ColumnExpression;
import com.foundationdb.sql.optimizer.plan.ExpressionNode;
import com.foundationdb.sql.optimizer.plan.ExpressionVisitor;
import com.foundationdb.sql.optimizer.plan.PlanNode;
import com.foundationdb.sql.optimizer.plan.PlanVisitor;
import com.foundationdb.sql.optimizer.rule.PlanContext;
import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.util.AssertUtils;
import com.foundationdb.util.Strings;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.foundationdb.util.AssertUtils.assertCollectionEquals;
import static com.foundationdb.util.Strings.stripr;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(NamedParameterizedRunner.class)
public final class ColumnEquivalenceTest extends OptimizerTestBase {

    public static final File RESOURCE_BASE_DIR =
            new File(OptimizerTestBase.RESOURCE_DIR, "rule");
    public static final File TESTS_RESOURCE_DIR = new File(RESOURCE_BASE_DIR, "column-equivalence");
    private static final Pattern SUBQUERY_DEPTH_PATTERN = Pattern.compile(
            "--\\s*subquery\\s+at\\s+depth\\s+(\\d+):",
            Pattern.CASE_INSENSITIVE
    );
    
    @TestParameters
    public static Collection<Parameterization> params() throws IOException {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        File schema = new File(TESTS_RESOURCE_DIR, "schema.ddl");
        for (File testFile : TESTS_RESOURCE_DIR.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isFile() && pathname.getName().endsWith(".test");
            }
        })) {
            List<String> testLines = Strings.dumpFile(testFile);
            Iterator<String> testLinesIter = testLines.iterator();
            String sql = testLinesIter.next();
            Map<Set<Map<String,Boolean>>,Integer> tmp = new HashMap<>();
            Set<Map<String,Boolean>> columnEquivalenceSets = new HashSet<>();
            int depth = 0;
            while (testLinesIter.hasNext()) {
                String columnEquivalenceLine = testLinesIter.next().trim();
                Matcher depthMatcher = SUBQUERY_DEPTH_PATTERN.matcher(columnEquivalenceLine);
                if (depthMatcher.matches()) {
                    tmp.put(columnEquivalenceSets, depth);
                    depth = Integer.parseInt(depthMatcher.group(1));
                    columnEquivalenceSets = new HashSet<>();
                    continue;
                }
                Map<String,Boolean> columnEquivalences = new HashMap<>();
                String[] columnNames = readEquivalences(columnEquivalenceLine);
                for (String columnName : columnNames)
                    columnEquivalences.put(columnName, columnNames.length == 1);
                columnEquivalenceSets.add(columnEquivalences);
            }
            tmp.put(columnEquivalenceSets, depth);
            pb.add(stripr(testFile.getName(), ".test"), schema, sql,  tmp);
        }
        
        return pb.asList();
    }

    private static String[] readEquivalences(String columnEquivalenceLine) {
        return columnEquivalenceLine.split("\\s+");
    }

    @Before
    public void loadDDL() throws Exception {
        AkibanInformationSchema ais = loadSchema(schemaFile);
        int columnEquivalenceRuleIndex = -1;
        for (int i = 0, max = DefaultRules.DEFAULT_RULES.size(); i < max; i++) {
            BaseRule rule = DefaultRules.DEFAULT_RULES.get(i);
            if (rule instanceof ColumnEquivalenceFinder) {
                columnEquivalenceRuleIndex = i;
                break;
            }
        }
        if (columnEquivalenceRuleIndex < 0)
            throw new RuntimeException(ColumnEquivalenceFinder.class.getSimpleName() + " not found");
        List<BaseRule> rulesSublist = DefaultRules.DEFAULT_RULES.subList(0, columnEquivalenceRuleIndex + 1);
        rules = RulesTestContext.create(ais, null, false, rulesSublist, new Properties());
    }

    @Test
    public void equivalences() throws Exception {
        Map<Set<Map<String,Boolean>>,Integer> actualEquivalentColumns = getActualEquivalentColumns();
        AssertUtils.assertMapEquals("for [ " + sql + " ]: ", equivalences, actualEquivalentColumns);
    }

    private Map<Set<Map<String,Boolean>>,Integer> getActualEquivalentColumns() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        binder.bind(stmt);
        stmt = booleanNormalizer.normalize(stmt);
        typeComputer.compute(stmt);
        stmt = subqueryFlattener.flatten((DMLStatementNode)stmt);
        // Turn parsed AST into intermediate form as starting point.
        PlanContext plan = new PlanContext(rules,
                new AST((DMLStatementNode)stmt,
                        parser.getParameterList()));
        rules.applyRules(plan);

        Map<Set<Map<String,Boolean>>,Integer> result = new HashMap<>();
        Collection<EquivalenceScope> scopes = new ColumnFinder().find(plan.getPlan());
        for (EquivalenceScope scope : scopes) {
            Collection<ColumnExpression> columnExpressions = scope.columns;
            int depth = scope.depth;
            Set<Map<String, Boolean>> byName = collectEquivalentColumns(columnExpressions, scope.equivs);
            Object old = result.put(byName, depth);
            assertNull("bumped: " + old, old);
            // anything in the equivs participants must also be in the scope's columns.
            HashSet<ColumnExpression> columnsSet = new HashSet<>(scope.columns);
            assertEquals("columns in equivalencies", columnsSet, new HashSet<>(scope.columns));
            Set<ColumnExpression> inScopeParticipants = Sets.intersection(scope.equivs.findParticipants(), columnsSet);
            assertCollectionEquals("columns in equivalencies", inScopeParticipants, scope.equivs.findParticipants());
        }
        return result;
    }

    private Set<Map<String, Boolean>> collectEquivalentColumns(Collection<ColumnExpression> columnExpressions,
                                                               EquivalenceFinder<ColumnExpression> equivs) {
        Set<Set<ColumnExpression>> set = new HashSet<>();
        for (ColumnExpression columnExpression : columnExpressions) {
            Set<ColumnExpression> belongsToSet = null;
            for (Set<ColumnExpression> equivalentExpressions : set) {
                Iterator<ColumnExpression> equivalentIters = equivalentExpressions.iterator();
                boolean isInSet = areEquivalent(equivalentIters.next(), columnExpression, equivs);
                // as a sanity check, ensure that this is consistent for the rest of them
                while (equivalentIters.hasNext()) {
                    ColumnExpression next = equivalentIters.next();
                    boolean bothEquivalent = areEquivalent(next, columnExpression, equivs)
                            && areEquivalent(columnExpression, next, equivs);
                    assertEquals(
                            "equivalence for " + columnExpression + " against " + next + " in " + equivalentExpressions,
                            isInSet,
                            bothEquivalent
                    );
                }
                if (isInSet) {
                    assertNull(columnExpression + " already in set: " + belongsToSet, belongsToSet);
                    belongsToSet = equivalentExpressions;
                }
            }
            if (belongsToSet == null) {
                belongsToSet = new HashSet<>();
                set.add(belongsToSet);
            }
            belongsToSet.add(columnExpression);
        }

        Set<Map<String,Boolean>> byName = new HashSet<>();
        for (Set<ColumnExpression> equivalenceSet : set) {
            Map<String,Boolean> nameAndNullability = new TreeMap<>();
            for (ColumnExpression columnExpression : equivalenceSet) {
                nameAndNullability.put(String.valueOf(columnExpression), columnExpression.getSQLtype().isNullable());
            }
            byName.add(nameAndNullability);
        }
        return byName;
    }

    private static boolean areEquivalent(ColumnExpression one, ColumnExpression two,
                                         EquivalenceFinder<ColumnExpression> equivs) {
        return equivs.areEquivalent(one, two) && equivs.areEquivalent(two, one);
    }

    public ColumnEquivalenceTest(File schemaFile, String sql, Map<Set<Map<String,Boolean>>,Integer> equivalences) {
        super(sql, sql, null, null);
        this.equivalences = equivalences;
        this.schemaFile = schemaFile;
    }
    
    private File schemaFile;
    private Map<Set<Map<String,Boolean>>,Integer> equivalences;
    private RulesContext rules;
    
    private static class EquivalenceScope {
        Collection<ColumnExpression> columns;
        EquivalenceFinder<ColumnExpression> equivs;
        int depth;

        private EquivalenceScope(int depth, Collection<ColumnExpression> columns,
                                 EquivalenceFinder<ColumnExpression> equivs) {
            this.depth = depth;
            this.columns = columns;
            this.equivs = equivs;
        }

        @Override
        public String toString() {
            return String.format("scope(cols=%s, depth=%d, %s", columns, depth, equivs);
        }
    }
    
    private static class ColumnFinder implements PlanVisitor, ExpressionVisitor {
        ColumnEquivalenceStack equivsStack = new ColumnEquivalenceStack();
        Deque<List<ColumnExpression>> columnExpressionsStack = new ArrayDeque<>();
        Collection<EquivalenceScope> results = new ArrayList<>();

        public Collection<EquivalenceScope> find(PlanNode root) {
            root.accept(this);
            assertTrue("stack isn't empty: " + columnExpressionsStack, columnExpressionsStack.isEmpty());
            assertTrue("equivs stack aren't empty", equivsStack.isEmpty());
            return results;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            if (equivsStack.enterNode(n))
                columnExpressionsStack.push(new ArrayList<ColumnExpression>());
            return visit(n);
        }
        @Override
        public boolean visitLeave(PlanNode n) {
            EquivalenceFinder<ColumnExpression> nodeEquivs = equivsStack.leaveNode(n);
            if (nodeEquivs != null) {
                Collection<ColumnExpression> collected = columnExpressionsStack.pop();
                int depth = columnExpressionsStack.size();
                EquivalenceScope scope = new EquivalenceScope(depth, collected, nodeEquivs);
                results.add(scope);
            }
            return true;
        }
        @Override
        public boolean visit(PlanNode n) {
            return true;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            return visit(n);
        }
        @Override
        public boolean visitLeave(ExpressionNode n) {
            return true;
        }
        @Override
        public boolean visit(ExpressionNode n) {
            if (n instanceof ColumnExpression)
                columnExpressionsStack.peek().add((ColumnExpression) n);
            return true;
        }
    }
}
