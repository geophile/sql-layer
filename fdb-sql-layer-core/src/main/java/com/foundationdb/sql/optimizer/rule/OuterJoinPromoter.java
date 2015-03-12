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
import com.foundationdb.sql.optimizer.plan.JoinNode.JoinType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Turn outer joins where the optional side of the join has a WHERE
 * condition on it (other that those that permit NULLs like IS NULL)
 * into inner joins.
 */
public class OuterJoinPromoter extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(OuterJoinPromoter.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    static class WhereFinder implements PlanVisitor, ExpressionVisitor {
        List<Select> result = new ArrayList<>();

        public List<Select> find(PlanNode root) {
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
            if (n instanceof Select) {
                Select f = (Select)n;
                if (f.getInput() instanceof Joinable) {
                    result.add(f);
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
    public void apply(PlanContext plan) {
        List<Select> wheres = new WhereFinder().find(plan.getPlan());
        for (Select select : wheres) {
            doJoins(select);
        }
    }

    static class RequiredSources implements ExpressionVisitor {
        private Set<ColumnSource> required = new HashSet<>();
        private Deque<ExpressionNode> stack = new ArrayDeque<>();
        
        public RequiredSources() {
        }

        public Set<ColumnSource> getRequired() {
            return required;
        }

        public void intersect(RequiredSources other) {
            required.retainAll(other.required);
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            stack.push(n);
            return visit(n);
        }

        @Override
        public boolean visitLeave(ExpressionNode n) {
            stack.pop();
            return true;
        }

        @Override
        public boolean visit(ExpressionNode n) {
            if (n instanceof ColumnExpression) {
                if (!insideDangerousFunction())
                    required.add(((ColumnExpression)n).getTable());
            }
            return true;
        }

        protected boolean insideDangerousFunction() {
            for (ExpressionNode inside : stack) {
                if ((inside instanceof FunctionExpression) &&
                    ((FunctionExpression)inside).anyNullTolerant()) {
                    // We could attempt to match a particular operand
                    // index by peeking at the next on the stack. Or
                    // to understand functions that are null-tolerant
                    // but cannot introduce a false positive condition
                    // with NULL, such as ISTRUE. This simpler check
                    // is more conservative.
                    return true;
                }
            }
            return false;
        }

        // Given a condition, collect all the column sources it implies aren't all null.
        protected void gatherRequired(ConditionExpression condition) {
            if (condition instanceof FunctionCondition) {
                FunctionCondition fcond = (FunctionCondition)condition;
                String fname = fcond.getFunction();
                if (fname.equals("and")) {
                    for (ExpressionNode operand : fcond.getOperands()) {
                        gatherRequired((ConditionExpression)operand);
                    }
                    return;
                }
                else if (fname.equals("or")) {
                    // Same table must be mentioned down every branch of the OR.
                    RequiredSources intersection = null;
                    for (ExpressionNode operand : fcond.getOperands()) {
                        RequiredSources opreq = new RequiredSources();
                        opreq.gatherRequired((ConditionExpression)operand);
                        if (intersection == null)
                            intersection = opreq;
                        else
                            intersection.intersect(opreq);
                    }
                    required.addAll(intersection.getRequired());
                    return;
                }
                else if (fname.equals("not") ||
                         fname.equals("isNull")) {
                    // These are too complicated to understand.
                    return;
                }
            }
            else if (condition instanceof AnyCondition) {
                Subquery subquery = ((AnyCondition)condition).getSubquery();
                if (subquery.getInput() instanceof Project) {
                    Project project = (Project)subquery.getInput();
                    gatherRequired((ConditionExpression)project.getFields().get(0));
                }
                return;
            }
            // Conditions, functions such as LIKE, etc.
            condition.accept(this);
        }
    }

    protected void doJoins(Select select) {
        RequiredSources required = new RequiredSources();
        for (ConditionExpression condition : select.getConditions()) {
            required.gatherRequired(condition);
        }
        Joinable joinable = (Joinable)select.getInput();
        gatherInnerJoinConditions(joinable, required);
        if (required.getRequired().isEmpty()) return;
        promoteOuterJoins(joinable, required);
    }

    // Predicates expressed as join conditions on an INNER join
    // reachable from the top are also promoting.
    protected void gatherInnerJoinConditions(Joinable joinable,
                                             RequiredSources required) {
        if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            switch (join.getJoinType()) {
            case INNER:
                if (join.getJoinConditions() != null) {
                    for (ConditionExpression condition : join.getJoinConditions()) {
                        required.gatherRequired(condition);
                    }
                }
                gatherInnerJoinConditions(join.getLeft(), required);
                gatherInnerJoinConditions(join.getRight(), required);
                break;
            case LEFT:
            case SEMI:
                gatherInnerJoinConditions(join.getLeft(), required);
                break;
            case RIGHT:
                gatherInnerJoinConditions(join.getRight(), required);
                break;
            }
        }
    }

    protected boolean promoteOuterJoins(Joinable joinable,
                                        RequiredSources required) {
        if (required.getRequired().contains(joinable))
            return true;
        if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            while (true) {
                boolean lp = promoteOuterJoins(join.getLeft(), required);
                boolean rp = promoteOuterJoins(join.getRight(), required);
                boolean promoted = false;
                switch (join.getJoinType()) {
                case LEFT:
                    promoted = rp;
                    break;
                case RIGHT:
                    promoted = lp;
                    break;
                }
                if (promoted) {
                    join.setJoinType(JoinType.INNER);
                    promotedOuterJoin(join);
                    int sizeBefore = required.getRequired().size();
                    for (ConditionExpression condition : join.getJoinConditions()) {
                        required.gatherRequired(condition);
                    }
                    if (sizeBefore < required.getRequired().size())
                        continue;
                }
                return lp || rp;
            }
        }
        return false;
    }

    // Walk back down recomputing the required/optional flag.
    protected void promotedOuterJoin(Joinable joinable) {
        if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            if (join.getJoinType() == JoinType.INNER) {
                promotedOuterJoin(join.getLeft());
                promotedOuterJoin(join.getRight());
            }
        }
        else if (joinable instanceof TableSource) {
            ((TableSource)joinable).setRequired(true);
        }
    }
    
}
