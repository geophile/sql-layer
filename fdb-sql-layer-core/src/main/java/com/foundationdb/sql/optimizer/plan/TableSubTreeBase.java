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

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;

import java.util.Iterator;

public class TableSubTreeBase<T extends TableSubTreeBase.TableNodeBase<T>>
               implements Iterable<T>
{
    public static class TableNodeBase<T extends TableNodeBase<T>> {
        private Table table;
        private T parent, firstChild, nextSibling;

        public TableNodeBase(Table table) {
            this.table = table;
        }

        public Table getTable() {
            return table;
        }

        public Group getGroup() {
            return table.getGroup();
        }

        public int getNFields() {
            return table.getColumns().size();
        }

        public int getDepth() {
            return table.getDepth();
        }

        public int getOrdinal() {
            return table.getOrdinal();
        }

        /** Is <code>this</code> an ancestor of <code>other</code>? */
        public boolean isAncestor(T other) {
            if (getDepth() >= other.getDepth()) 
                return false;
            while (true) {
                if (other == this)
                    return true;
                other = other.getParent();
                if (other == null)
                    return false;
            }
        }

        public T getParent() {
            return parent;
        }

        public T getFirstChild() {
            return firstChild;
        }

        public T getNextSibling() {
            return nextSibling;
        }

        public void setParent(T parent) {
            this.parent = parent;
        }

        public void setFirstChild(T firstChild) {
            this.firstChild = firstChild;
        }

        public void setNextSibling(T nextSibling) {
            this.nextSibling = nextSibling;
        }

        public String toString() {
            return table.getName().getTableName();
        }
    }

    static class NodeIterator<T extends TableNodeBase<T>> implements Iterator<T> {
        private T root, next;

        NodeIterator(T root) {
            this.next = this.root = root;
        }

        public boolean hasNext() {
            return (next != null);
        }

        public T next() {
            T onext = next;
            next = onext.getFirstChild();
            if (next == null) {
                T node = onext;
                while (node != root) {
                    next = node.getNextSibling();
                    if (next != null) break;
                    node = node.getParent();
                }
            }
            return onext;
        }
        
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    protected T root;

    protected TableSubTreeBase(T root) {
        this.root = root;
    }

    public T getRoot() {
        return root;
    }
    
    public Iterator<T> iterator() {
        return new NodeIterator<>(root);
    }
}
