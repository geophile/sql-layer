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
package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.ais.model.Table;

import java.util.HashMap;
import java.util.Map;

public abstract class TableTreeBase<T extends TableSubTreeBase.TableNodeBase<T>>
                        extends TableSubTreeBase<T>
{
    private Map<Table,T> map = new HashMap<>();

    public TableTreeBase() {
        super(null);
    }

    public T getNode(Table table) {
        return map.get(table);
    }

    protected abstract T createNode(Table table) ;

    public T addNode(Table table) {
        T node = getNode(table);
        if (node != null)
            return node;
        node = createNode(table);
        map.put(table, node);
        if (root == null) {
            root = node;
            return node;
        }
        T toInsert = node;
        if (node.getDepth() < root.getDepth()) {
            toInsert = root;
            root = node;
        }
        Table parentTable = toInsert.getTable().getParentTable();
        assert (parentTable != null);
        T parent = addNode(parentTable);
        assert ((toInsert.getParent() == null) && // Brand new or old root.
                (toInsert.getNextSibling() == null));
        toInsert.setParent(parent);
        toInsert.setNextSibling(parent.getFirstChild());
        parent.setFirstChild(toInsert);
        return node;
    }

}
