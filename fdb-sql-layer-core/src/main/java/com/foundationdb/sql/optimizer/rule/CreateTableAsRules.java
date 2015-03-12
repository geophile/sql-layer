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

import com.foundationdb.server.error.UnsupportedCreateSelectException;
import com.foundationdb.sql.optimizer.plan.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** {@Create Table As Rules} takes in a planContext then visits all nodes that are
 * instances of TableSource and replaces them with CreateAs plan, these are used
 * later on to put EmitBoundRow_Nexted operators which will be used for insertion
 * and deletion from an online Create Table As query*/

/**
 * TODO in future versions this could take a specified table name or id
 * then only change these tablesources, this would be necessary if in future versions
 * we accept queries with union, intersect, except, join, etc
 */
 public class CreateTableAsRules extends BaseRule {
    private static final Logger logger = LoggerFactory.getLogger(SortSplitter.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {

        Results results =  new CreateTableAsFinder().find(plan.getPlan());
        CreateAs createAs = null;
        for (TableSource tableSource : results.tables) {
            createAs = transform(tableSource);
        }
        assert(createAs != null);
        for (Project project : results.projects) {
            transform(project, createAs);
        }
    }

    protected CreateAs transform(TableSource tableSource) {
        CreateAs createAs = new CreateAs();
        createAs.setOutput(tableSource.getOutput());
        (tableSource.getOutput()).replaceInput(tableSource, createAs);
        createAs.setTableSource(tableSource);
        return createAs;
    }

    protected void transform(Project project, CreateAs createAs){
        for (int i = 0; i < project.getFields().size(); i++){
            if(project.getFields().get(i) instanceof ColumnExpression) {
                ColumnExpression expression = (ColumnExpression) project.getFields().get(i);
                project.getFields().remove(i);
                project.getFields().add(i, new ColumnExpression(expression, createAs));
            }
        }
    }

    static class Results {

        public List<TableSource> tables = new ArrayList<>();
        public List<Project> projects = new ArrayList<>();
    }

    static class CreateTableAsFinder implements PlanVisitor {

        Results results;

        public Results find(PlanNode root) {
            results = new Results();
            root.accept(this);
            return results;
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
            if(isIllegalPlan(n)){
                throw new UnsupportedCreateSelectException();
            }
            if (n instanceof TableSource)
                results.tables.add((TableSource) n);
            else if (n instanceof Project) {
                results.projects.add((Project) n);
            }
            return true;
        }

        public boolean isIllegalPlan(PlanNode n) {
            // Only the simplest select from a single table is allowed.
            return !(n instanceof DMLStatement || n instanceof InsertStatement ||
                     n instanceof Project || n instanceof Select || n instanceof TableSource);
        }
    }
}