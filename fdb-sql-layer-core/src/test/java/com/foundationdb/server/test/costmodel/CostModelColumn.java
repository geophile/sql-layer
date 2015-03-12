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
package com.foundationdb.server.test.costmodel;

public abstract class CostModelColumn
{
    public final String name()
    {
        return name;
    }
    
    public String declaration()
    {
        return String.format("%s %s", name, type.name());
    }
    
    public abstract String description();

    public abstract Object valueFor(int x);
    
    public static CostModelColumn intColumn(String name)
    {
        return new IntColumn(name);
    }

    public static CostModelColumn varcharColumnGoodPrefixCompression(String name, int width)
    {
        return new StringColumnGoodPrefixCompression(name, width);
    }

    public static CostModelColumn varcharColumnBadPrefixCompression(String name, int width)
    {
        return new StringColumnBadPrefixCompression(name, width);
    }

    protected CostModelColumn(String name, CostModelType type)
    {
        this.name = name;
        this.type = type;
    }
    
    private static final int MAX_INT_DIGITS = 10;
    private static final String ZEROS = "0000000000";

    private final String name;
    private final CostModelType type;
    
    private static class IntColumn extends CostModelColumn
    {
        @Override
        public String description()
        {
            return "int";
        }

        @Override
        public Object valueFor(int x)
        {
            return x;
        }

        public IntColumn(String name)
        {
            super(name, CostModelType.INT);
        }
    }
    
    private static abstract class StringColumn extends CostModelColumn
    {
        @Override
        public String declaration()
        {
            return String.format("%s(%s)", super.declaration(), width);
        }

        public StringColumn(String name, int width)
        {
            super(name, CostModelType.VARCHAR);
            this.width = width;
            StringBuilder buffer = new StringBuilder();
            for (int i = 0; i < width - MAX_INT_DIGITS; i++) {
                buffer.append('x');
            }
            this.filler = buffer.toString();
        }
        
        protected final int width;
        protected final String filler;
    }
    
    private static class StringColumnGoodPrefixCompression extends StringColumn
    {
        @Override
        public String description()
        {
            return String.format("varchar(%s) - good compression", width);
        }

        @Override
        public Object valueFor(int x)
        {
            String s = Integer.toString(x);
            return filler + ZEROS.substring(0, MAX_INT_DIGITS - s.length()) + s;
        }

        public StringColumnGoodPrefixCompression(String name, int length)
        {
            super(name, length);
        }
    }
    
    private static class StringColumnBadPrefixCompression extends StringColumn
    {
        @Override
        public String description()
        {
            return String.format("varchar(%s) - bad compression", width);
        }

        @Override
        public Object valueFor(int x)
        {
            String s = Integer.toString(x);
            return ZEROS.substring(0, MAX_INT_DIGITS - s.length()) + s + filler;
        }

        public StringColumnBadPrefixCompression(String name, int length)
        {
            super(name, length);
        }
    }
}
