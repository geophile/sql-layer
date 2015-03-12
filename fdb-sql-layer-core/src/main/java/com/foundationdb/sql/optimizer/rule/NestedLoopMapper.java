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

import com.foundationdb.server.error.CorruptedPlanException;
import com.foundationdb.sql.optimizer.plan.*;
import com.foundationdb.sql.optimizer.plan.JoinNode.JoinType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Convert nested loop join into map.
 * This rule only does the immediate conversion to a Map. The map
 * still needs to be folded after conditions have moved down and so on.
 */
public class NestedLoopMapper extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(NestedLoopMapper.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    static class NestedLoopsJoinsFinder implements PlanVisitor, ExpressionVisitor {
        List<JoinNode> result = new ArrayList<>();

        public List<JoinNode> find(PlanNode root) {
            root.accept(this);
            return result;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            if (n instanceof JoinNode) {
                JoinNode j = (JoinNode)n;
                switch (j.getImplementation()) {
                case NESTED_LOOPS:
                case BLOOM_FILTER:
                case HASH_TABLE:
                    result.add(j);
                }
            }
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
            return true;
        }
    }

    @Override
    public void apply(PlanContext planContext) {
        BaseQuery query = (BaseQuery)planContext.getPlan();
        List<JoinNode> joins = new NestedLoopsJoinsFinder().find(query);
        for (JoinNode join : joins) {
            PlanNode outer = join.getLeft();
            PlanNode inner = join.getRight();
            if (join.hasJoinConditions()) {
                if (join.getJoinType().isInner() || join.getJoinType().isSemi()) {
                    outer = moveConditionsToOuterNode(outer, join.getJoinConditions(),
                                                      getQuery(join).getOuterTables());
                }
                if (join.hasJoinConditions())
                    inner = new Select(inner, join.getJoinConditions());
            }
            PlanNode map;
            switch (join.getImplementation()) {
            case NESTED_LOOPS:
                map = new MapJoin(join.getJoinType(), outer, inner);
                break;
            case BLOOM_FILTER:
                {
                    HashJoinNode hjoin = (HashJoinNode)join;
                    BloomFilter bf = (BloomFilter)hjoin.getHashTable();
                    map = new BloomFilterFilter(bf, hjoin.getMatchColumns(),
                                                outer, inner);
                    PlanNode loader = hjoin.getLoader();
                    loader = new Project(loader, hjoin.getHashColumns());
                    map = new UsingBloomFilter(bf, loader, map);
                }
                break;
            case HASH_TABLE:
                {
                    map = new MapJoin(join.getJoinType(), outer, inner);
                    HashJoinNode hjoin = (HashJoinNode)join;
                    HashTable ht = (HashTable)hjoin.getHashTable();
                    PlanNode loader = hjoin.getLoader();
                    map = new UsingHashTable(ht, loader, map,
                                             hjoin.getHashColumns(),
                                             hjoin.getTKeyComparables(),
                                             hjoin.getCollators());
                }
                break;
            default:
                assert false : join;
                map = join;
            }
            join.getOutput().replaceInput(join, map);
        }
    }

    private BaseQuery getQuery(PlanNode node) {
        PlanWithInput output = node.getOutput();
        while (output != null) {
            if (output instanceof BaseQuery) {
                return (BaseQuery) output;
            }
            output = output.getOutput();
        }
        throw new CorruptedPlanException("PlanNode did not have BaseQuery");
    }


    private PlanNode moveConditionsToOuterNode(PlanNode planNode, ConditionList conditions,
                                               Set<ColumnSource> outerSources) {
        ConditionList selectConditions = new ConditionList();
        Iterator<ConditionExpression> iterator = conditions.iterator();
        while (iterator.hasNext()) {
            ConditionExpression condition = iterator.next();
            Set<ColumnSource> columnSources = new ConditionColumnSourcesFinder().find(condition);
            columnSources.removeAll(outerSources);
            PlanNodeProvidesSourcesChecker checker = new PlanNodeProvidesSourcesChecker(columnSources, planNode);
            if (checker.run()) {
                selectConditions.add(condition);
                iterator.remove();
            }
        }
        return selectConditions.isEmpty() ? planNode : new Select(planNode, selectConditions);
    }

    private static class PlanNodeProvidesSourcesChecker implements PlanVisitor {

        private final Set<ColumnSource> columnSources;
        private final PlanNode planNode;

        private PlanNodeProvidesSourcesChecker(Set<ColumnSource> columnSources, PlanNode node) {
            this.columnSources = columnSources;
            this.planNode = node;
        }

        public boolean run() {
            planNode.accept(this);
            return columnSources.isEmpty();
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            if (columnSources.isEmpty()) {
                return false;
            }
            if (n instanceof ColumnSource) {
                columnSources.remove(n);
                // We want to go inside, because if you have a Group Join, the inner groups are nested nodes within
                // the outer table source
                return true;
            }
            if (n instanceof Subquery) {
                // subquery sources are the source you can see from outside, don't go into the inner subquery
                return false;
            }
            return true;
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            return false;
        }

        @Override
        public boolean visit(PlanNode n) {
            if (columnSources.isEmpty()) {
                return false;
            }
            if (n instanceof ColumnSource) {
                columnSources.remove(n);
                return true;
            }
            if (n instanceof Subquery) {
                return false;
            }
            return true;
        }
    }

}
