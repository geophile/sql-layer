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

import com.foundationdb.server.error.UnsupportedSQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Take a map join node and push enough into the inner loop that the
 * bindings can be evaluated properly. 
 * Or, looked at another way, what is before expressed through
 * data-flow is after expressed as control-flow.
 */
public class MapFolder extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(MapFolder.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    static class MapJoinsFinder implements PlanVisitor, ExpressionVisitor {
        List<MapJoin> result;

        public List<MapJoin> find(PlanNode root) {
            result = new ArrayList<>();
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
            if (n instanceof MapJoin) {
                result.add((MapJoin)n);
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

    static class ColumnSourceFinder implements PlanVisitor { // Not down to expressions.
        Set<ColumnSource> result;

        public Set<ColumnSource> find(PlanNode root) {
            result = new HashSet<>();
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
            if (n instanceof ColumnSource) {
                result.add((ColumnSource)n);
            } 
            else if (n instanceof IndexScan) {
                result.addAll(((IndexScan)n).getTables());
            }
            return true;
        }
    }

    @Override
    public void apply(PlanContext planContext) {
        BaseQuery query = (BaseQuery)planContext.getPlan();
        List<MapJoin> maps = new MapJoinsFinder().find(query);
        List<MapJoinProject> mapJoinProjects = new ArrayList<>(0);
        if (maps.isEmpty()) return;
        if (query instanceof DMLStatement) {
            DMLStatement update = (DMLStatement)query;
            switch (update.getType()) {
            case UPDATE:
            case DELETE:
                addUpdateInput(update);
                break;
            }
        }
        for (MapJoin map : maps)
            handleJoinType(map);
        for (MapJoin map : maps)
            foldOuterMap(map);
        for (MapJoin map : maps)
            fold(map, mapJoinProjects);
        for (MapJoinProject project : mapJoinProjects)
            fillProject(project);
    }

    // First pass: account for the join type by adding something at
    // the tip of the inner (fast) side of the loop.
    protected void handleJoinType(MapJoin map) {
        switch (map.getJoinType()) {
        case INNER:
            break;
        case LEFT:
            map.setInner(new NullIfEmpty(map.getInner()));
            break;
        case SEMI:
            map.setInner(new Limit(map.getInner(), 1));
            break;
        case ANTI:
            map.setInner(new OnlyIfEmpty(map.getInner()));
            break;
        default:
            throw new UnsupportedSQLException("complex join type " + map, null);
        }
        map.setJoinType(null);  // No longer special.
    }

    // Second pass: if one map has another on the outer (slow) side,
    // turn them inside out. Nesting must all be on the inner side to
    // be like regular loops. Conceptually, the two trade places, but
    // actually doing that would mess up the depth nesting for the
    // next pass.
    protected void foldOuterMap(MapJoin map) {
        if (map.getOuter() instanceof MapJoin) {
            MapJoin otherMap = (MapJoin)map.getOuter();
            foldOuterMap(otherMap);
            PlanNode inner = map.getInner();
            PlanNode outer = otherMap.getInner();
            map.setOuter(otherMap.getOuter());
            map.setInner(otherMap);
            otherMap.setOuter(outer);
            otherMap.setInner(inner);
        }
    }    

    // Third pass: move things upstream of the map down into the inner (fast) side.
    // Also add Project where the nesting still needs an actual join
    // on the outer side.
    protected void fold(MapJoin map, List<MapJoinProject> mapJoinProjects) {
        PlanWithInput parent = map;
        PlanNode child;
        UsingHashTable usingHashTable = null;

        do {
            child = parent;
            parent = child.getOutput();
            if(parent instanceof UsingHashTable)
                usingHashTable = (UsingHashTable)parent;
        } while (!((parent instanceof MapJoin) ||
                   // These need to be outside.
                   (parent instanceof Subquery) ||
                   (parent instanceof ResultSet) ||
                   (parent instanceof AggregateSource) ||
                   (parent instanceof Sort) ||
                   (parent instanceof NullIfEmpty) ||
                   (parent instanceof OnlyIfEmpty) ||
                   (parent instanceof Limit) ||
                   // Captures enough at the edge of the inside.
                   (child instanceof Project) ||
                   (child instanceof UpdateInput)));
        if (child != map) {
            PlanNode inner = map.getInner();
            // Add a Project to capture fields within the loop before
            // leaving the scope of its outer binding, that is,
            // materialize the join as a projected row. 
            // The cases where this is needed are:
            // (1) This loop is itself on the outer side of another loop.
            // (2) It implements the nullable side of an outer join.
            // (3) Nested inside and so feeding another instance of (2) or (3).
            if (parent instanceof MapJoin) {
                MapJoinProject nested = findAddedProject((MapJoin)parent, 
                                                         mapJoinProjects);
                if ((nested != null) ||
                    (child == ((MapJoin)parent).getOuter())) {
                    inner = addProject((MapJoin)parent, map, inner,
                                       nested, mapJoinProjects);
                }
            }
            else if (child instanceof Project) {
                MapJoinProject nested = findAddedProject((Project)child,
                                                         mapJoinProjects);
                if (nested != null) {
                    inner = addProject(null, map, inner,
                                       nested, mapJoinProjects);
                }
            }
            else if (parent instanceof NullIfEmpty) {
                // Even though we stop at the outer join, we still
                // need to find the map that it's contained in.
                PlanNode ancestor = parent;
                do {
                    ancestor = ancestor.getOutput();
                } while ((ancestor instanceof Select) ||
                         (ancestor instanceof Project));
                if (ancestor instanceof MapJoin) {
                    MapJoinProject nested = findAddedProject((MapJoin)ancestor, 
                                                             mapJoinProjects);
                    inner = addProject((MapJoin)ancestor, map, inner,
                                       nested, mapJoinProjects);
                }
            }
            if (usingHashTable != null){
                if(usingHashTable == child){
                    if(!(usingHashTable.getInput() == map)) {
                        //PlanNode usingChild = usingHashTable.getInput();
                        map.getOutput().replaceInput(map, inner);
                        usingHashTable.replaceInput(usingHashTable.getInput(), map);
                    } else {
                        inner.setOutput(map);
                        map.setInner(inner);
                    }
                } else {
                    usingHashTable.getOutput().replaceInput(usingHashTable, usingHashTable.getInput());
                    //remove using_hashTable from plan
                    parent.replaceInput(child, usingHashTable);
                    //now put it on very top
                    map.getOutput().replaceInput(map, inner);
                    //remove map from plan
                    usingHashTable.replaceInput(usingHashTable.getInput(), map);
                    //place map below using
                    map.setInner(child);
                }
            } else {
                map.getOutput().replaceInput(map, inner);
                parent.replaceInput(child, map);
                map.setInner(child);
            }
        }
    }

    // A pending Project used to capture bindings inside the loop.
    // When complete, we will walk the tree to determine which fields,
    // if any, from its scope are used downstream and capture them or
    // else throw the Project away.
    static class MapJoinProject implements PlanVisitor, ExpressionVisitor {
        MapJoin parentMap, childMap;
        MapJoinProject nested;
        Project project;
        Set<ColumnSource> allSources, innerSources;
        List<ColumnExpression> columns;
        boolean singleNodeMode, foundOuter;
        int nodeDepth;
        boolean inSubquery;
        int depthSubquery;

        @Override
        public String toString() {
            StringBuilder str = new StringBuilder(getClass().getSimpleName());
            str.append("(").append(childMap.summaryString(PlanNode.SummaryConfiguration.DEFAULT));
            for (ColumnSource source : allSources) {
                str.append(",");
                if (innerSources.contains(source))
                    str.append("*");
                str.append(source.getName());
            }
            if (project != null) {
                str.append(",").append(project.getFields());
            }
            str.append(")");
            return str.toString();
        }

        public MapJoinProject(MapJoin parentMap, MapJoin childMap, 
                              MapJoinProject nested, Project project,
                              Set<ColumnSource> allSources, 
                              Set<ColumnSource> innerSources) {
            this.parentMap = parentMap;
            this.childMap = childMap;
            this.nested = nested;
            this.project = project;
            this.allSources = allSources;
            this.innerSources = innerSources;
            this.inSubquery = false;
            this.depthSubquery = 0;
        }
        
        // Are any of the inner bindings used outer?
        public boolean find() {
            singleNodeMode = false;
            columns = new ArrayList<>();
            for (MapJoinProject loop = this; loop != null; loop = loop.nested) {
                if (loop.parentMap != null) {
                    // Check context within the bindings of any nested loops.
                    loop.parentMap.getInner().accept(this);
                }
            }
            if (foundOuter && (project != childMap.getInner())) {
                // If we will be using the project, it will cut off anything else coming
                // from the inside; check between project and inside of loop, if there is
                // anything there. Need to check one-by-one without any children to keep
                // spurious (although ultimately harmless) columns out.
                singleNodeMode = true;
                PlanNode node = project;
                do {
                    node = node.getOutput();
                    node.accept(this);
                } while (node != childMap.getInner());
            }
            return foundOuter;
        }

        // Actually install field expressions for this Project.
        public void install() {
            Set<ColumnExpression> seen = new HashSet<>();
            for (ColumnExpression column : columns) {
                if (seen.add(column)) {
                    project.getFields().add(column);
                }
            }
        }

        // Splice this unneeded Project out.
        public void remove() {
            project.getOutput().replaceInput(project, project.getInput());
            project = null;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            if (n instanceof SubquerySource) {
                if (!inSubquery) {
                    inSubquery = true;
                }
                depthSubquery++;
            }
            nodeDepth++;
            return true;
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            if (n instanceof SubquerySource) {
                depthSubquery--;
                if (depthSubquery == 0) {
                    inSubquery = false;
                }
            }
            nodeDepth--;
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
            if ((n instanceof ColumnExpression) &&
                // singleNodeMode: don't check columns from input nodes.
                (!singleNodeMode || (nodeDepth == 1)) && !inSubquery) {
                ColumnExpression column = (ColumnExpression)n;
                if (allSources.contains(column.getTable())) {
                    columns.add(column);
                    if (!innerSources.contains(column.getTable())) {
                        foundOuter = true;
                    }
                }
            }
            return true;
        }
     }

    protected Project addProject(MapJoin parentMap, MapJoin childMap, PlanNode inner, 
                                 MapJoinProject nested, List<MapJoinProject> into) {
        ColumnSourceFinder finder = new ColumnSourceFinder();
        Set<ColumnSource> outerSources = finder.find(childMap.getOuter());
        Set<ColumnSource> innerSources = finder.find(childMap.getInner());
        outerSources.addAll(innerSources);
        Project project = new Project(inner, new ArrayList<ExpressionNode>());
        into.add(new MapJoinProject(parentMap, childMap, 
                                    nested, project,
                                    outerSources, innerSources));
        return project;
    }

    protected MapJoinProject findAddedProject(MapJoin childMap, List<MapJoinProject> in) {
        for (MapJoinProject mapJoinProject : in) {
            if (mapJoinProject.childMap == childMap) {
                return mapJoinProject;
            }
        }
        return null;
    }
    
    protected MapJoinProject findAddedProject(Project project, List<MapJoinProject> in) {
        for (MapJoinProject mapJoinProject : in) {
            if (mapJoinProject.project == project) {
                return mapJoinProject;
            }
        }
        return null;
    }
    
    // Fourth pass: materialize join with a Project when there is no
    // other alternative.
    protected void fillProject(MapJoinProject project) {
        if (project.find()) {
            project.install();
            logger.debug("Added {}", project);
        }
        else {
            project.remove();   // Everything came from inner table(s) after all.
            logger.debug("Skipped {}", project);
        }
    }

    protected void addUpdateInput(DMLStatement update) {
        BasePlanWithInput node = update;
        while (true) {
            PlanNode input = node.getInput();
            if (!(input instanceof BasePlanWithInput))
                break;
            node = (BasePlanWithInput)input;
            if (node instanceof BaseUpdateStatement) {
                input = node.getInput();
                node.replaceInput(input, new UpdateInput(input, update.getSelectTable()));
                return;
            }
        }
    }

}
