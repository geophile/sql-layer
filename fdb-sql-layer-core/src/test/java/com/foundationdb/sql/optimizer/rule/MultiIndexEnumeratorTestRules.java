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

import com.foundationdb.sql.optimizer.plan.Duplicatable;
import com.foundationdb.sql.optimizer.plan.DuplicateMap;
import com.foundationdb.sql.optimizer.plan.IndexScan;
import com.foundationdb.sql.optimizer.plan.PlanNode;
import com.foundationdb.sql.optimizer.plan.PlanToString;
import com.foundationdb.sql.optimizer.plan.PlanVisitor;
import com.foundationdb.sql.optimizer.plan.PlanWithInput;
import com.foundationdb.sql.optimizer.rule.PlanContext.DefaultWhiteboardMarker;
import com.foundationdb.sql.optimizer.rule.PlanContext.WhiteboardMarker;
import com.foundationdb.sql.optimizer.rule.join_enum.GroupIndexGoalHooks;
import com.foundationdb.util.Strings;
import com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused") // created by reflection from rules.yml
public final class MultiIndexEnumeratorTestRules {
    private static Logger logger = LoggerFactory.getLogger(MultiIndexEnumeratorTestRules.class);
    private static WhiteboardMarker<IntersectionViewer> intersectionViewerMarker = DefaultWhiteboardMarker.create();
    
    public static class InstallHooks extends BaseRule {
        @Override
        protected Logger getLogger() {
            return logger;
        }

        @Override
        public void apply(PlanContext plan) {
            IntersectionViewer hook = new IntersectionViewer();
            plan.putWhiteboard(intersectionViewerMarker, hook);
            GroupIndexGoalHooks.hookIntersectedIndexes(hook);
        }
    }
    
    public static class ResultHooks extends BaseRule {
        @Override
        protected Logger getLogger() {
            return logger;
        }

        @Override
        public void apply(PlanContext plan) {
            GroupIndexGoalHooks.unhookIntersectedIndexes();
            plan.setPlan(useHooks(plan.getPlan(), plan.getWhiteboard(intersectionViewerMarker)));
        }

        private PlanNode useHooks(final PlanNode plan, IntersectionViewer intersections) {
            List<String> scanDescriptions = new ArrayList<>(intersections.indexScans.size());
            for (IndexScan intersection : intersections.indexScans) {
                String str = intersection.summaryString(true, PlanNode.SummaryConfiguration.DEFAULT);
                scanDescriptions.add(str);
            }
            final String summary = Strings.join(scanDescriptions);
            return new PlanNode() {
                @Override
                public PlanWithInput getOutput() {
                    return plan.getOutput();
                }

                @Override
                public void setOutput(PlanWithInput output) {
                    plan.setOutput(output);
                }

                @Override
                public boolean accept(PlanVisitor v) {
                    v.visit(this);
                    return true;
                }

                @Override
                public String summaryString(SummaryConfiguration configuration) {
                    return summary;
                }

                @Override
                public String planString(SummaryConfiguration configuration) {
                    return PlanToString.of(this, configuration);
                }

                @Override
                public Duplicatable duplicate() {
                    return plan.duplicate();
                }

                @Override
                public Duplicatable duplicate(DuplicateMap map) {
                    return plan.duplicate(map);
                }
            };
        }
    }
    
    private static class IntersectionViewer implements Function<IndexScan,Void> {
        private List<IndexScan> indexScans = new ArrayList<>();
        @Override
        public Void apply(IndexScan input) {
            indexScans.add(input);
            return null;
        }
    }
}
