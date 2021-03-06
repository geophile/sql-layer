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

import com.foundationdb.sql.optimizer.plan.*;
import com.foundationdb.sql.optimizer.plan.Sort.OrderByExpression;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.server.error.InvalidOptimizerPropertyException;
import com.foundationdb.server.error.NoAggregateWithGroupByException;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.TableIndex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Resolve aggregate functions and group by expressions to output
 * columns of the "group table," that is, the result of aggregation.
 */
public class AggregateMapper extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(AggregateMapper.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        AggregateSourceAndFunctionFinder aggregateSourceFinder = new AggregateSourceAndFunctionFinder(plan);

        List<AggregateSourceState> sources = aggregateSourceFinder.find();
        List<AggregateFunctionExpression> functions = aggregateSourceFinder.getFunctions();
        if (sources.isEmpty() && !functions.isEmpty()) {
            // if there are AggregateFunctionExpressions but no AggregateSources
            throw new UnsupportedSQLException("Aggregate not allowed in WHERE",
                                              functions.get(0).getSQLsource());
        }

        // Step 1: look for outer aggregate references, and convert AggregateFunctionExpressions
        //         to AnnotatedAggregateFunctionExpressions
        Annotator annotator = new Annotator(plan.getPlan(),
                                            aggregateSourceFinder.getTablesToSources());
        annotator.run();

        // Step 2: Each run of FindHavingSources makes two passes. 
        //     Pass 1) Map AggregateFunctions that aren't references to outer aggregates
        //     Pass 2) Check that all the ColumnExpressions are okay 
        for (AggregateSourceState source : sources) {
            FindHavingSources findHavingSources = new FindHavingSources((SchemaRulesContext)plan.getRulesContext(),
                                                                        source.aggregateSource,
                                                                        source.containingQuery);
            findHavingSources.run(source.aggregateSource);
        }
        // Step 3: Add all aggregates to sources, or throw an error.
        AddAggregates addAggregates = new AddAggregates(plan.getPlan(),
                                                        aggregateSourceFinder.getTablesToSources());
        addAggregates.run();
    }

    static class AnnotatedAggregateFunctionExpression extends AggregateFunctionExpression {
        private AggregateSource source = null;

        public AnnotatedAggregateFunctionExpression(AggregateFunctionExpression aggregateFunc) {
            super(aggregateFunc.getFunction(),
                  aggregateFunc.getOperand(),
                  aggregateFunc.isDistinct(),
                  aggregateFunc.getSQLtype(),
                  aggregateFunc.getSQLsource(),
                  aggregateFunc.getType(),
                  aggregateFunc.getOption(),
                  aggregateFunc.getOrderBy());
        }

        public AnnotatedAggregateFunctionExpression(String function, ExpressionNode operand,
                boolean distinct, 
                DataTypeDescriptor sqlType, ValueNode sqlSource,
                TInstance type,
                Object option, List<OrderByExpression> orderBy, AggregateSource source) {
                    super(function, 
                          operand, 
                          distinct, 
                          sqlType, 
                          sqlSource, 
                          type, 
                          option, 
                          orderBy);
                    this.source = source;
        }

        public AnnotatedAggregateFunctionExpression setSource(AggregateSource source) {
            this.source = source;
            return this;
        }

        public AggregateSource getSource() {
            return source;
        }

        public AggregateFunctionExpression getWithoutAnnotation() {
            return new AggregateFunctionExpression(this.getFunction(),
                                                   this.getOperand(),
                                                   this.isDistinct(),
                                                   this.getSQLtype(),
                                                   this.getSQLsource(),
                                                   this.getType(),
                                                   this.getOption(),
                                                   this.getOrderBy());
        }

        @Override
        public ExpressionNode accept(ExpressionRewriteVisitor v) {
            ExpressionNode result = v.visit(this);
            return result;
        }
    }

    static class AggregateSourceFinder extends SubqueryBoundTablesTracker {
        List<AggregateSourceState> sources = new ArrayList<>();

        public AggregateSourceFinder(PlanContext planContext) {
            super(planContext);
        }

        public List<AggregateSourceState> find() {
            run();
            return sources;
        }

        @Override
        public boolean visit(PlanNode n) {
            super.visit(n);
            if (n instanceof AggregateSource)
                sources.add(new AggregateSourceState((AggregateSource)n, currentQuery()));
            return true;
        }
    }

    static class AggregateSourceAndFunctionFinder extends AggregateSourceFinder {
        List<AggregateFunctionExpression> functions = new ArrayList<>();
        Deque<AggregateFunctionExpression> functionsStack = new ArrayDeque<>();

        // collect this to use in AddAggregates
        Map<TableSource, AggregateSourceState> tablesToSources = new HashMap<>();

        public AggregateSourceAndFunctionFinder(PlanContext planContext) {
            super(planContext);
        }

        public List<AggregateFunctionExpression> getFunctions() {
            return functions;
        }

        public Map<TableSource, AggregateSourceState> getTablesToSources() {
            return tablesToSources;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            if (n instanceof AggregateFunctionExpression) {
                if (!functionsStack.isEmpty()) {
                    throw new UnsupportedSQLException("Cannot nest aggregate functions",
                                                      functionsStack.peek().getSQLsource());
                } else {
                    functionsStack.push((AggregateFunctionExpression)n);
                }
            }
            return visit(n);
        }

        @Override
        public boolean visit(PlanNode n) {
            super.visit(n);
            if (n instanceof TableSource) {
                if (!sources.isEmpty()) {
                    tablesToSources.put((TableSource)n, sources.get(sources.size()-1));
                }
            }
            return true;
        }

        @Override
        public boolean visit(ExpressionNode n) {
            super.visit(n);
            if (n instanceof AggregateFunctionExpression) {
                functions.add((AggregateFunctionExpression)n);
            }
            return true;
        }

        @Override
        public boolean visitLeave(ExpressionNode n) {
            if (n instanceof AggregateFunctionExpression) {
                functionsStack.pop();
            }
            return true;
        }
    }

    static class AggregateSourceState {
        AggregateSource aggregateSource;
        BaseQuery containingQuery;

        public AggregateSourceState(AggregateSource aggregateSource, 
                                    BaseQuery containingQuery) {
            this.aggregateSource = aggregateSource;
            this.containingQuery = containingQuery;
        }
    }

    static class Annotator implements PlanVisitor, ExpressionRewriteVisitor {
        PlanNode plan;
        Deque<BaseQuery> subqueries = new ArrayDeque<>();
        Map<TableSource, AggregateSourceState> tablesToSources;

        public Annotator(PlanNode plan,
                         Map<TableSource, AggregateSourceState> tablesToSources) {
            this.plan = plan;
            this.tablesToSources = tablesToSources;
        }

        public void run() {
            plan.accept(this);
        }

        public ExpressionNode annotateAggregate(AggregateFunctionExpression expr) {
            // look for a reference to an outer aggregate, and save that source in the annotated function if found
            AggregateSource source = null;
            if (expr.getOperand() instanceof ColumnExpression) {
                ColumnSource columnSource = ((ColumnExpression)expr.getOperand()).getTable();
                if (columnSource instanceof TableSource && tablesToSources.containsKey((TableSource)columnSource)) {
                    AggregateSourceState sourceState = 
                            tablesToSources.get((TableSource)columnSource);
                    if (sourceState.containingQuery != subqueries.peek() && 
                            subqueries.contains(sourceState.containingQuery)) {
                        source = sourceState.aggregateSource;
                    }
                }
            }
            return new AnnotatedAggregateFunctionExpression(expr).setSource(source);
        }

        @Override
        public boolean visitChildrenFirst(ExpressionNode n) {
            return false;
        }

        @Override
        public ExpressionNode visit(ExpressionNode n) {
            if (n instanceof AggregateFunctionExpression) {
                return annotateAggregate((AggregateFunctionExpression)n);
            }
            return n;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            if (n instanceof BaseQuery) {
                subqueries.push((BaseQuery)n);
            }
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            if (n instanceof BaseQuery) {
                subqueries.pop();
            }
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            return true;
        }
    }

    static abstract class Remapper implements ExpressionRewriteVisitor, PlanVisitor {

        public void remap(PlanNode n) {
            while (true) {
                // Keep going as long as we're feeding something we understand.
                n = n.getOutput();
                if (n instanceof Select) {
                    remap(((Select)n).getConditions());
                }
                else if (n instanceof Sort) {
                    remapA(((Sort)n).getOrderBy());
                }
                else if (n instanceof Project) {
                    Project p = (Project)n;
                    remap(p.getFields());
                }
                else if (n instanceof Limit) {
                    // Understood not but mapped.
                }
                else
                    break;
            }
        }

        @SuppressWarnings("unchecked")
        protected <T extends ExpressionNode> void remap(List<T> exprs) {
            for (int i = 0; i < exprs.size(); i++) {
                exprs.set(i, (T)exprs.get(i).accept(this));
            }
        }

        protected void remapA(List<? extends AnnotatedExpression> exprs) {
            for (AnnotatedExpression expr : exprs) {
                expr.setExpression(expr.getExpression().accept(this));
            }
        }

        @Override
        public boolean visitChildrenFirst(ExpressionNode expr) {
            return false;
        }

        @Override
        public boolean visit(PlanNode n) {
            return true;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }
    }

    static class FindHavingSources extends Remapper {
        private SchemaRulesContext rulesContext;
        private AggregateSource source;
        private BaseQuery query;
        private Deque<BaseQuery> subqueries = new ArrayDeque<>();
        private Set<ColumnSource> aggregated = new HashSet<>();
        private Map<ExpressionNode,ExpressionNode> map = 
            new HashMap<>();
        private enum State {
            FINDING_SOURCES, CHECKING_ERRORS
        };
        private State state;
        boolean hasAggregates;
        private enum ImplicitAggregateSetting {
            ERROR, FIRST, FIRST_IF_UNIQUE
        };
        private ImplicitAggregateSetting implicitAggregateSetting;
        private Set<TableSource> uniqueGroupedTables;

        protected ImplicitAggregateSetting getImplicitAggregateSetting() {
            if (implicitAggregateSetting == null) {
                String setting = rulesContext.getProperty("implicitAggregate", "error");
                if ("error".equals(setting))
                    implicitAggregateSetting = ImplicitAggregateSetting.ERROR;
                else if ("first".equals(setting))
                    implicitAggregateSetting = ImplicitAggregateSetting.FIRST;
                else if ("firstIfUnique".equals(setting))
                    implicitAggregateSetting = ImplicitAggregateSetting.FIRST_IF_UNIQUE;
                else
                    throw new InvalidOptimizerPropertyException("implicitAggregate", setting);
            }
            return implicitAggregateSetting;
        }

        public FindHavingSources(SchemaRulesContext rulesContext, AggregateSource source, BaseQuery query) {
            this.rulesContext = rulesContext;
            this.source = source;
            this.query = query;
            aggregated.add(source);
            // Map all the group by expressions at the start.
            // This means that if you GROUP BY x+1, you can ORDER BY
            // x+1, or x+1+1, but not x+2. Postgres is like that, too.
            List<ExpressionNode> groupBy = source.getGroupBy();
            for (int i = 0; i < groupBy.size(); i++) {
                ExpressionNode expr = groupBy.get(i);
                map.put(expr, new ColumnExpression(source, i, 
                                                   expr.getSQLtype(), expr.getSQLsource(), expr.getType()));
            }
        }

        public void run(PlanNode n) {
            state = State.FINDING_SOURCES;
            hasAggregates = false;
            remap(n);
            state = State.CHECKING_ERRORS;
            remap(n);
        }

        @Override
        public void remap(PlanNode n) {
            while (true) {
                // Keep going as long as we're feeding something we understand.
                n = n.getOutput();
                if (n instanceof Select) {
                    remap(((Select)n).getConditions());
                }
                else if (n instanceof Sort) {
                    remapA(((Sort)n).getOrderBy());
                }
                else if (n instanceof Project) {
                    Project p = (Project)n;
                    remap(p.getFields());
                    aggregated.add(p);
                }
                else if (n instanceof Limit) {
                    // Understood not but mapped.
                }
                else
                    break;
            }
        }

        @Override
        public ExpressionNode visit(ExpressionNode expr) {
            ExpressionNode nexpr = map.get(expr);
            if (nexpr != null)
                return nexpr;
            switch (state) {
            case FINDING_SOURCES:
                if (expr instanceof AnnotatedAggregateFunctionExpression) {
                    AnnotatedAggregateFunctionExpression a = (AnnotatedAggregateFunctionExpression)expr;
                    nexpr = rewrite(a);
                    if (nexpr == null) {
                        if (subqueries.isEmpty() && a.getSource() == null) {
                            a.setSource(source);
                            hasAggregates = true;
                        }
                        return a;
                    }
                    return nexpr.accept(this);
                }
            case CHECKING_ERRORS:
                if (expr instanceof ColumnExpression) {
                    ColumnExpression column = (ColumnExpression)expr;
                    ColumnSource table = column.getTable();
                    if ((!map.isEmpty() || hasAggregates) &&
                        !aggregated.contains(table) &&
                        !boundElsewhere(table)) {
                        return nonAggregate(column);
                    }
                }
            }
            return expr;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            if (n instanceof BaseQuery)
                subqueries.push((BaseQuery)n);
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            if (n instanceof BaseQuery)
                subqueries.pop();
            return true;
        }

        // Rewrite agregate functions that aren't well behaved wrt pre-aggregation.
        protected ExpressionNode rewrite(AnnotatedAggregateFunctionExpression expr) {
            String function = expr.getFunction().toUpperCase();
            if ("AVG".equals(function)) {
                ExpressionNode operand = expr.getOperand();
                List<ExpressionNode> noperands = new ArrayList<>(2);
                noperands.add(new AnnotatedAggregateFunctionExpression("SUM", operand, expr.isDistinct(),
                                                                       operand.getSQLtype(), null, 
                                                                       operand.getType(), null, null,
                                                                       expr.getSource()));
                DataTypeDescriptor intType = new DataTypeDescriptor(TypeId.INTEGER_ID, false);
                TInstance intInst = rulesContext.getTypesTranslator().typeForSQLType(intType);
                noperands.add(new AnnotatedAggregateFunctionExpression("COUNT", operand, expr.isDistinct(),
                                                                       intType, null, intInst, null, null, 
                                                                       expr.getSource()));
                return new FunctionExpression("divide",
                                              noperands,
                                              expr.getSQLtype(), expr.getSQLsource(), expr.getType());
            }
            if ("VAR_POP".equals(function) ||
                "VAR_SAMP".equals(function) ||
                "STDDEV_POP".equals(function) ||
                "STDDEV_SAMP".equals(function)) {
                ExpressionNode operand = expr.getOperand();
                List<ExpressionNode> noperands = new ArrayList<>(3);
                noperands.add(new AnnotatedAggregateFunctionExpression("_VAR_SUM_2", operand, expr.isDistinct(),
                                                                       operand.getSQLtype(), null,
                                                                       operand.getType(), null, null, 
                                                                       expr.getSource()));
                noperands.add(new AnnotatedAggregateFunctionExpression("_VAR_SUM", operand, expr.isDistinct(),
                                                                       operand.getSQLtype(), null,
                                                                       operand.getType(), null, null,
                                                                       expr.getSource()));
                DataTypeDescriptor intType = new DataTypeDescriptor(TypeId.INTEGER_ID, false);
                TInstance intInst = rulesContext.getTypesTranslator().typeForSQLType(intType);
                noperands.add(new AnnotatedAggregateFunctionExpression("COUNT", operand, expr.isDistinct(),
                                                                       intType, null, intInst, null, null,
                                                                       expr.getSource()));
                return new FunctionExpression("_" + function,
                                              noperands,
                                              expr.getSQLtype(), expr.getSQLsource(), expr.getType());
            }
            return null;
        }

        protected ExpressionNode addKey(ExpressionNode expr) {
            int position = source.addGroupBy(expr);
            ColumnExpression nexpr = new ColumnExpression(source, position,
                                                          expr.getSQLtype(), expr.getSQLsource(), expr.getType());
            map.put(expr, nexpr);
            return nexpr;
        }

        protected boolean boundElsewhere(ColumnSource table) {
            if (query.getOuterTables().contains(table))
                return true;    // Bound outside.
            BaseQuery subquery = subqueries.peek();
            if (subquery != null) {
                if (!subquery.getOuterTables().contains(table))
                    return true; // Must be introduced by subquery.
            }
            return false;
        }

        // Use of a column not in GROUP BY without aggregate function.
        protected ExpressionNode nonAggregate(ColumnExpression column) {
            boolean isUnique = isUniqueGroupedTable(column.getTable());
            ImplicitAggregateSetting setting = getImplicitAggregateSetting();
            if ((setting == ImplicitAggregateSetting.ERROR) ||
                ((setting == ImplicitAggregateSetting.FIRST_IF_UNIQUE) && !isUnique))
                throw new NoAggregateWithGroupByException(column.getSQLsource());
            if (isUnique && source.getAggregates().isEmpty())
                // Add unique as another key in hopes of turning the
                // whole things into a distinct.
                return addKey(column);
            else
                return new AnnotatedAggregateFunctionExpression("FIRST", column, false,
                                                                column.getSQLtype(), null, column.getType(), null, null,
                                                                source);
        }

        protected boolean isUniqueGroupedTable(ColumnSource columnSource) {
            if (!(columnSource instanceof TableSource))
                return false;
            TableSource table = (TableSource)columnSource;
            if (uniqueGroupedTables == null)
                uniqueGroupedTables = new HashSet<>();
            if (uniqueGroupedTables.contains(table))
                return true;
            Set<Column> columns = new HashSet<>();
            for (ExpressionNode groupBy : source.getGroupBy()) {
                if (groupBy instanceof ColumnExpression) {
                    ColumnExpression groupColumn = (ColumnExpression)groupBy;
                    if (groupColumn.getTable() == table) {
                        columns.add(groupColumn.getColumn());
                    }
                }
            }
            if (columns.isEmpty()) return false;
            // Find a unique index all of whose columns are in the GROUP BY.
            // TODO: Use column equivalences.
            find_index:
            for (TableIndex index : table.getTable().getTable().getIndexes()) {
                if (!index.isUnique()) continue;
                for (IndexColumn indexColumn : index.getKeyColumns()) {
                    if (!columns.contains(indexColumn.getColumn())) {
                        continue find_index;
                    }
                }
                uniqueGroupedTables.add(table);
                return true;
            }
            return false;
        }
    }

    static class AddAggregates implements PlanVisitor, ExpressionRewriteVisitor {
        PlanNode plan;
        Deque<BaseQuery> subqueries = new ArrayDeque<>();
        Map<TableSource, AggregateSourceState> tablesToSources;

        public AddAggregates(PlanNode plan,
                             Map<TableSource, AggregateSourceState> tablesToSources) {
            this.plan = plan;
            this.tablesToSources = tablesToSources;
        }

        public void run() {
            plan.accept(this);
        }

        public ExpressionNode addAggregate(AnnotatedAggregateFunctionExpression expr) {
            AggregateSource source = expr.getSource();
            if (source == null) {
                throw new UnsupportedSQLException("Aggregate not allowed in WHERE",
                        expr.getSQLsource());
            }
            int position;
            if (source.hasAggregate(expr)) {
                position = source.getPosition(expr.getWithoutAnnotation());
            } else {
                position = source.addAggregate(expr.getWithoutAnnotation());
            }
            ExpressionNode nexpr = new ColumnExpression(source, position,
                                         expr.getSQLtype(), expr.getSQLsource(), expr.getType());
            return nexpr;
        }

        @Override
        public boolean visitChildrenFirst(ExpressionNode n) {
            return false;
        }

        @Override
        public ExpressionNode visit(ExpressionNode n) {
            if (n instanceof AnnotatedAggregateFunctionExpression) {
                return addAggregate((AnnotatedAggregateFunctionExpression)n);
            }
            return n;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            if (n instanceof BaseQuery) {
                subqueries.push((BaseQuery)n);
            }
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            if (n instanceof BaseQuery) {
                subqueries.pop();
            }
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            return true;
        }
    }
}
