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
package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.rowtype.RowType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class HKey implements Comparable<HKey>
{
    // Object interface

    @Override
    public int hashCode()
    {
        int h = 0;
        for (Object element : elements) {
            h = h * 9987001 + (element instanceof Table ? ((Table) element).getTableId() : element.hashCode());
        }
        return h;
    }

    @Override
    public boolean equals(Object o)
    {
        boolean eq = o != null && o instanceof HKey;
        if (eq) {
            HKey that = (HKey) o;
            eq = this.elements.size() == that.elements.size();
            int i = 0;
            while (eq && i < elements.size()) {
                Object thisElement = this.elements.get(i);
                Object thatElement = that.elements.get(i);
                eq = thisElement == thatElement // Takes care of Tables
                     || thisElement.equals(thatElement);
                i++;
            }
        }
        return eq;
    }

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append("(");
        boolean first = true;
        for (Object element : elements) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            if (element instanceof Table) {
                Table table = (Table) element;
                buffer.append(table.getName().getTableName());
            } else {
                buffer.append(element);
            }
        }
        buffer.append(")");
        return buffer.toString();
    }

    // Comparable interface

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(HKey that)
    {
        int c = 0;
        Iterator<Object> i = this.elements.iterator();
        Iterator<Object> j = that.elements.iterator();
        while (c == 0 && i.hasNext() && j.hasNext()) {
            Object iElement = i.next();
            Object jElement = j.next();
            if (iElement == null && jElement == null) {
                // c is already 0
            } else if (iElement == null) {
                c = -1;
            } else if (jElement == null) {
                c = 1;
            } else {
                assertSame(iElement.getClass(), jElement.getClass());
                if (iElement instanceof Table) {
                    c = ((Table) iElement).getTableId() - ((Table) jElement).getTableId();
                } else {
                    c = ((Comparable)iElement).compareTo(jElement);
                }
            }
        }
        if (c == 0) {
            assertTrue((i.hasNext() ? 1 : 0) + (j.hasNext() ? 1 : 0) <= 1);
            c = i.hasNext() ? 1 : j.hasNext() ? -1 : 0;
        }
        return c;
    }


    // HKey interface

    public Object[] objectArray()
    {
        return elements.toArray();
    }

    public HKey copy()
    {
        return new HKey(elements.toArray());
    }

    public HKey(Object... elements)
    {
        this.elements = new ArrayList<>();
        for (Object element : elements) {
            if (element instanceof RowType) {
                element = ((RowType)element).table();
            }
            this.elements.add(element);
        }
    }

    // Object state

    private List<Object> elements;
}
