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
import org.slf4j.Logger;

import java.util.List;
import java.util.Properties;

/** The context / owner of a {@link PlanContext}, shared among several of them. */
public class RulesContext
{
    // TODO: Need more much sophisticated invocation mechanism.
    private Properties properties;
    private List<? extends BaseRule> rules;

    protected RulesContext() {
    }

    protected void initProperties(Properties properties) {
        this.properties = properties;
    }

    protected void initRules(List<? extends BaseRule> rules) {
        this.rules = rules;
    }

    protected void initDone() {
        assert (properties != null) : "initProperties() not called";
        assert (rules != null) : "initRules() not called";
    }

    protected boolean rulesAre(List<? extends BaseRule> expected) {
        return rules == expected;
    }

    /** Make context with these rules. Just for testing. */
    public static RulesContext create(List<? extends BaseRule> rules,
                                      Properties properties) {
        RulesContext context = new RulesContext();
        context.initProperties(properties);
        context.initRules(rules);
        context.initDone();
        return context;
    }

    public void applyRules(PlanContext plan) {
        boolean logged = false;
        for (BaseRule rule : rules) {
            Logger logger = rule.getLogger();
            boolean debug = logger.isDebugEnabled();
            if (debug && !logged) {
                logger.debug("Before {}:\n{}", rule.getName(), plan.planString(PlanNode.SummaryConfiguration.DEFAULT));
            }
            beginRule(rule);
            try {
                rule.apply(plan);
            }
            catch (RuntimeException e) {
                if (debug) {
                    String msg = "error while applying " + rule.getName() + " to " + plan.planString(PlanNode.SummaryConfiguration.DEFAULT);
                    logger.debug(msg, e);
                }
                throw e;
            }
            finally {
                endRule(rule);
            }
            if (debug) {
                logger.debug("After {}:\n{}", rule.getName(), plan.planString(PlanNode.SummaryConfiguration.DEFAULT));
            }
            logged = debug;
        }
    }

    /** Extend this to implement tracing, etc. */
    public void beginRule(BaseRule rule) {
    }
    public void endRule(BaseRule rule) {
    }

    /** Get optimizer configuration. */
    public Properties getProperties() {
        return properties;
    }
    public String getProperty(String key) {
        return properties.getProperty(key);
    }
    public String getProperty(String key, String defval) {
        return properties.getProperty(key, defval);
    }
}
