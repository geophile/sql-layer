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

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.SetOperatorNode;
import com.foundationdb.sql.parser.Visitable;
import com.foundationdb.sql.parser.Visitor;
import com.foundationdb.server.error.SQLParserInternalException;

import java.util.ArrayList;
import java.util.List;

public class ParameterFinder implements Visitor
{
    private List<ParameterNode> parameters;

    public ParameterFinder() {
    }

    public List<ParameterNode> find(Visitable root) {
        parameters = new ArrayList<>();
        try {
            root.accept(this);
        } 
        catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }
        return parameters;
    }

    @Override
    public Visitable visit(Visitable node) {
        if (node instanceof ParameterNode)
            parameters.add((ParameterNode)node);
        return node;
    }

    @Override
    public boolean skipChildren(Visitable node) {
        if (node instanceof SetOperatorNode) {
            // visiting all children will result in duplicate ParameterNodes.
            // instead, just visit the LeftResultSet and RightResult set, skipping the resultColumns list
            try {
                ((SetOperatorNode)node).getLeftResultSet().accept(this);
                ((SetOperatorNode)node).getRightResultSet().accept(this);
            } catch (StandardException e) {
                throw new SQLParserInternalException(e);
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

}
