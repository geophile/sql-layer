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
package com.foundationdb.junit;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public final class Parameterization
{
    private String name;
    private final List<Object> args;
    private boolean expectedToPass;

    public static Parameterization create(String name, Object... args)
    {
        return new Parameterization(name, true, args);
    }

    public static Parameterization failing(String name, Object... args)
    {
        return new Parameterization(name, false, args);
    }

    public Parameterization(String name, boolean isPassing, Object... args)
    {
        if (name == null) {
            throw new IllegalArgumentException("name can't be null");
        }
        if (args == null) {
            throw new IllegalArgumentException("args can't be null");
        }
        this.name = name;
        this.args = new LinkedList<>(Arrays.asList(args));
        this.expectedToPass = isPassing;
    }

    /**
     * <p>Creates a copy of this parameterization.</p>
     *
     * <p>The original parameterization's arguments list is copied shallowly.</p>
     * @param copyFrom the original parameterization
     */
    public Parameterization(Parameterization copyFrom)
    {
        this.name = copyFrom.name;
        this.args = new LinkedList<>(copyFrom.args);
        this.expectedToPass = copyFrom.expectedToPass;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * Returns this parameterization's arguments as an Object array.
     *
     * This array is <em>not</em> backed by the parameterization, so changes in one are not reflected in the other.
     * However, the array is a shallow copy, so modifying an element of the parameterization in the list will be
     * reflected in the array, and the vice versa.
     * @return the arguments
     */
    public Object[] getArguments()
    {
        return args.toArray();
    }

    /**
     * Returns this parameterization's arguments as a list.
     *
     * This list is backed by the parameterization, so changes to one are reflected in the other.
     * @return a list of arguments, backed by the parameterization
     */
    public List<Object> getArgsAsList()
    {
        return args;
    }

    public boolean expectedToPass()
    {
        return expectedToPass;
    }

    public void setExpectedToPass(boolean expectedToPass)
    {
        this.expectedToPass = expectedToPass;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        Parameterization that = (Parameterization) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    public boolean equivalent(Parameterization other)
    {
        if (!this.name.equals(other.name))
            return false;
        if (this.expectedToPass != other.expectedToPass)
            return false;
        if (this.args.size() != other.args.size())
            return false;

        Iterator<Object> myIter = this.args.iterator();
        Iterator<Object> otherIter = other.args.iterator();

        while (myIter.hasNext())
        {
            Object mine = myIter.next();
            Object his = otherIter.next();

            if (mine == null)
            {
                if (his != null)
                {
                    return false;
                }
            }
            else if (!mine.equals(his))
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString()
    {
        return "Parameterization[" + (expectedToPass? "PASSING " : "FAILING ") + name + ": " + args + " ]";
    }
}
