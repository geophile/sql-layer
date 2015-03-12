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
package com.foundationdb.sql.optimizer;

import com.foundationdb.sql.parser.*;

import com.foundationdb.sql.StandardException;

import java.util.*;

/**
 * A <code>NodeFactory</code> that knows how to properly clone bindings.
 */
public class BindingNodeFactory extends NodeFactory
{
    private NodeFactory inner;

    private Map<QueryTreeNode,QueryTreeNode> nodeMap;
    private Collection<QueryTreeNode> bindingsToCopy;

    public static void wrap(SQLParser parser) {
        NodeFactory nodeFactory = parser.getNodeFactory();
        if (!(nodeFactory instanceof BindingNodeFactory))
            parser.setNodeFactory(new BindingNodeFactory(nodeFactory));
    }

    /** Construct BindingNodeFactory encapsulating original. */
    public BindingNodeFactory(NodeFactory inner) {
        this.inner = inner;
    }

    /** Create node: just passes along to encapsulated. */
    public QueryTreeNode getNode(int nodeType, SQLParserContext pc)
            throws StandardException {
        return inner.getNode(nodeType, pc);
    }

    /** Copy node: pass along, but keep track mappings for those kinds
     * of nodes that are used in bindings. */
    public QueryTreeNode copyNode(QueryTreeNode node, SQLParserContext pc)
            throws StandardException {
        boolean newMap = false;
        if (nodeMap == null) {
            nodeMap = new HashMap<>();
            bindingsToCopy = new ArrayList<>();
            newMap = true;
        }
        try {
            QueryTreeNode result = inner.copyNode(node, pc);
            if ((node instanceof FromTable) ||
                (node instanceof ResultColumn))
                nodeMap.put(node, result);
            return result;
        }
        finally {
            if (newMap) {
                copyBindings();
                nodeMap = null;
                bindingsToCopy = null;
            }
        }
    }

    /** Copy user data, which is known to be a appropriate binding object. */
    public Object copyUserData(QueryTreeNode node, Object userData)
            throws StandardException {
        if ((userData instanceof TableBinding) ||
            (userData instanceof ColumnBinding)) {
            // Remember to fix later after all copies done.
            bindingsToCopy.add(node);
        }
        // Just return same data for now.
        return userData;
    }
    
    /** Do the actual binding copies.
     * Deferred like this so that it does not matter what order the tree
     * is traversed during the node copy.
     */
    protected void copyBindings() throws StandardException {
        for (QueryTreeNode node : bindingsToCopy) {
            Object userData = node.getUserData();
            if (userData instanceof TableBinding)
                userData = new TableBinding((TableBinding)userData);
            else if (userData instanceof ColumnBinding) {
                ColumnBinding cb = (ColumnBinding)userData;
                FromTable oldFromTable = cb.getFromTable();
                FromTable newFromTable = (FromTable)nodeMap.get(oldFromTable);
                if (newFromTable == null)
                    newFromTable = oldFromTable; // Not cloned in this subtree.
                ResultColumn oldResultColumn = cb.getResultColumn();
                if (oldResultColumn == null) {
                    userData = new ColumnBinding(newFromTable,
                                                 cb.getColumn(), cb.isNullable());
                }
                else {
                    ResultColumn newResultColumn = (ResultColumn)nodeMap.get(oldResultColumn);
                    if (newResultColumn == null)
                        newResultColumn = oldResultColumn;
                    userData = new ColumnBinding(newFromTable, newResultColumn);
                }
            }
            else {
                continue;
            }
            node.setUserData(userData);
        }
    }

}
