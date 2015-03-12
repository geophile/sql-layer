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

import com.foundationdb.sql.optimizer.plan.PlanNode;
import com.foundationdb.sql.optimizer.plan.PlanVisitor;

import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;

import java.util.Map;
import java.util.HashMap;

/** A plan and its common context while running rules. */
// TODO: Consider extending this to a inheritance tree of Scenarios
// to allow exploring alternatives efficiently.
public class PlanContext
{
    private RulesContext rulesContext;
    private PlanNode plan;

    public PlanContext(RulesContext rulesContext) {
        this.rulesContext = rulesContext;
    }
                       
    public PlanContext(RulesContext rulesContext, PlanNode plan) {
        this.rulesContext = rulesContext;
        this.plan = plan;
    }
                       
    public RulesContext getRulesContext () {
        return rulesContext;
    }

    public PlanNode getPlan() {
        return plan;
    }
    public void setPlan(PlanNode plan) {
        this.plan = plan;
    }
    
    public void accept(PlanVisitor visitor) {
        plan.accept(visitor);
    }
    
    /** Type safe tag for storing objects on the context whiteboard. */
    public interface WhiteboardMarker<T> {
    }

    /** A marker class if no other conveniently unique object exists. */
    public static final class DefaultWhiteboardMarker<T> implements WhiteboardMarker<T> {
        // poor man's substitute for diamond operator
        public static <T> WhiteboardMarker<T> create() {
            return new DefaultWhiteboardMarker<>();
        }
    }

    private Map<WhiteboardMarker<?>,Object> whiteboard = 
        new HashMap<>();

    /** Store information associated with the plan for use by more
     * than one rule, but not associated directly with any part of the
     * plan tree.
     */
    @SuppressWarnings("unchecked")
    public <T> T getWhiteboard(WhiteboardMarker<T> marker) {
        return (T)whiteboard.get(marker);
    }
    public <T> void putWhiteboard(WhiteboardMarker<T> marker, T value) {
        whiteboard.put(marker, value);
    }

    /** Get a {@link QueryContext} for evaluations performed during
     * compilation, issuing warnings, etc.
     */
    public QueryContext getQueryContext() {
        return new SimpleQueryContext(null);
    }


    /** Format a hierarchical view of the current plan.
     * @param configuration configuration options for how the plan should be printed
     */
    public String planString(PlanNode.SummaryConfiguration configuration) {
        return plan.planString(configuration);
    }
}
