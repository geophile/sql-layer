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

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.server.types.texpressions.Comparison;
import com.foundationdb.sql.optimizer.plan.BaseQuery;
import com.foundationdb.sql.optimizer.plan.ColumnExpression;
import com.foundationdb.sql.optimizer.plan.ComparisonCondition;
import com.foundationdb.sql.optimizer.plan.ConditionExpression;
import com.foundationdb.sql.optimizer.plan.ExpressionNode;
import com.foundationdb.sql.optimizer.plan.ExpressionVisitor;
import com.foundationdb.sql.optimizer.plan.JoinNode;
import com.foundationdb.sql.optimizer.plan.Joinable;
import com.foundationdb.sql.optimizer.plan.PlanNode;
import com.foundationdb.sql.optimizer.plan.PlanVisitor;
import com.foundationdb.sql.optimizer.plan.Select;
import com.foundationdb.sql.optimizer.plan.TableNode;
import com.foundationdb.sql.optimizer.plan.TableSource;
import com.foundationdb.sql.optimizer.plan.TableTree;
import com.foundationdb.sql.optimizer.rule.JoinAndIndexPicker.JoinEnumerator;
import com.foundationdb.sql.optimizer.rule.JoinAndIndexPicker.JoinsFinder;
import com.foundationdb.sql.optimizer.rule.JoinAndIndexPicker.Picker;
import com.foundationdb.sql.types.DataTypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public final class ColumnEquivalenceFinder extends BaseRule {
    private static final Logger logger = LoggerFactory.getLogger(ColumnEquivalenceFinder.class);
    
    @Override
    protected Logger getLogger() {
        return logger;
    }
    
    private static class ColumnEquivalenceVisitor implements PlanVisitor, ExpressionVisitor {

        private ColumnEquivalenceStack equivs = new ColumnEquivalenceStack();

        @Override
        public boolean visitEnter(PlanNode n) {
            equivs.enterNode(n);
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            equivs.leaveNode(n);
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            if (n instanceof JoinNode) {
                JoinNode joinNode = (JoinNode)n;
                if (joinNode.isInnerJoin())
                    equivalenceConditions(joinNode.getJoinConditions());
            }
            else if (n instanceof Select) {
                Select select = (Select) n;
                equivalenceConditions(select.getConditions());
            }
            return true;
        }

        private void equivalenceConditions(List<ConditionExpression> conditions) {
            if (conditions != null) {
                for (ConditionExpression condition : conditions)
                    equivalenceCondition(condition);
            }
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
            return true;
        }

        private void equivalenceCondition(ConditionExpression condition) {
            if (condition instanceof ComparisonCondition) {
                ComparisonCondition comparison = (ComparisonCondition) condition;
                if (comparison.getOperation().equals(Comparison.EQ)
                        && (comparison.getLeft() instanceof ColumnExpression)
                        && (comparison.getRight() instanceof ColumnExpression)
                        ) {
                    ColumnExpression left = (ColumnExpression) comparison.getLeft();
                    ColumnExpression right = (ColumnExpression) comparison.getRight();
                    if (!left.equals(right)) {
                        markNotNull(left);
                        markNotNull(right);
                        equivs.get().markEquivalent(left, right); // also implies right.equivalentTo(left)
                    }
                }
            }
        }
    }

    private static void markNotNull(ColumnExpression columnExpression) {
        DataTypeDescriptor sqLtype = columnExpression.getSQLtype();
        if (sqLtype.isNullable()) {
            DataTypeDescriptor notNullable = sqLtype.getNullabilityType(false);
            columnExpression.setSQLtype(notNullable);
        }
    }

    @Override
    public void apply(PlanContext plan) {
        // Do basic column equivalence finding
        plan.getPlan().accept(new ColumnEquivalenceVisitor());
        
        // Do FK table equivalence finding
        // Loop through all the tables, and find any possible FK Parents
        // Add these to the possible Equivalences 
        BaseQuery basePlan = (BaseQuery)(plan.getPlan());
        List<Picker> pickers = new JoinsFinder(plan).find();
        for (Picker picker : pickers) {
            addFKEquivsFromJoins (picker.rootJoin(), picker.query.getFKEquivalencies());
            picker.query.getFKEquivalencies().copyEquivalences(picker.query.getColumnEquivalencies());
        }
    }

    private void addFKEquivsFromJoins(Joinable joins, EquivalenceFinder<ColumnExpression> equivalencies) {
        List<Joinable> tables = new ArrayList<>();
        JoinEnumerator.addTables(joins, tables);
        Map<Table, TableSource> sources = new HashMap<>();

        for (Joinable table: tables) {
            if (table instanceof TableSource) {
                TableSource tableSource = (TableSource)table;
                sources.put(tableSource.getTable().getTable(), tableSource);
            }
        }
        
        for (Joinable table: tables) {
            if (table instanceof TableSource) {
                TableSource tableSource = (TableSource) table;
                checkFKParents (tableSource.getTable().getTable(), tableSource, sources, equivalencies);
            }
        }
    }
    
    private void checkFKParents(Table child, TableSource tableSource,  Map<Table, TableSource> sources, EquivalenceFinder<ColumnExpression> equivelances) {
        for (ForeignKey key : child.getReferencingForeignKeys()) {
            if (checkParentFKIsPK (key, child)) {
                TableSource parentSource = sources.get(key.getReferencedTable());
                if (parentSource == null) {
                    parentSource = generateTableSource(key.getReferencedTable());
                    sources.put(key.getReferencedTable(), parentSource);
                }
                checkFKParents (key.getReferencedTable(), parentSource, sources, equivelances);
                for (int i = 0; i < key.getReferencedColumns().size(); i++) {
                    ColumnExpression one = expressionFromColumn(key.getReferencingColumns().get(i), tableSource);
                    ColumnExpression two = expressionFromColumn(key.getReferencedColumns().get(i), parentSource);
                    equivelances.markEquivalent(one, two);
                }
            }
        }
    }
    
    private boolean checkParentFKIsPK (ForeignKey key, Table child) {
        if (key.getReferencingTable().equals(child) && // TODO: This is temporary redundant. 
            key.getReferencedIndex().isPrimaryKey()) {

            // Check the child table FK columns are the child table PK as well. 
            // TODO: We may be able to relax this with further testing. 
            List<Column> fkColumns = key.getReferencingColumns();
            TableIndex childPKIndex = key.getReferencingTable().getIndex(Index.PRIMARY);
            // this can occur if the child table has no declared primary key. 
            if (childPKIndex == null) { return false; }
            List<IndexColumn> pkColumns = childPKIndex.getKeyColumns();
            if (fkColumns.size() != pkColumns.size()) return false;
            for (int i = 0; i < fkColumns.size(); i++) {
                if (!fkColumns.get(i).equals(pkColumns.get(i).getColumn())) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    
    private TableSource generateTableSource (Table table) {
        return new TableSource (new TableNode (table, new TableTree()), true, table.getName().toString());
    }
    private ColumnExpression expressionFromColumn(Column col, TableSource source) {
        
        if (source == null) {
            Table table = col.getTable();
            TableNode node = new TableNode(table, new TableTree());
            source = new TableSource(node, true, table.getName().toString());
        }
        return new ColumnExpression(source, col);
    }
    
}
