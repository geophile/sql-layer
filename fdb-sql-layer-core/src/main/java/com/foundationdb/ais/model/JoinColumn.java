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
package com.foundationdb.ais.model;

public class JoinColumn
{
    public static JoinColumn create(Join join, Column parent, Column child)
    {
        return join.addJoinColumn(parent, child);
    }

    @Override
    public String toString()
    {
        return "JoinColumn(" + child.getName() + " -> " + parent.getName() + ")";
    }

    protected JoinColumn(Join join, Column parent, Column child)
    {
        this.join = join;
        this.parent = parent;
        this.child = child;
    }

    public Join getJoin()
    {
        return join;
    }

    public Column getParent()
    {
        return parent;
    }

    public Column getChild()
    {
        return child;
    }

    private final Join join;
    private final Column parent;
    private final Column child;
}
