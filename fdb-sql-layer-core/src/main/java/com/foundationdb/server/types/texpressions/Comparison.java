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
package com.foundationdb.server.types.texpressions;

public enum Comparison
{
    
    EQ("==", false, true, false),
    GE(">=", false, true, true),
    GT(">", false, false, true),
    LE("<=", true, true, false),
    LT("<", true, false, false),
    NE("!=", true, false, true)
    ;

    public boolean matchesCompareTo(int compareTo) {
        if (compareTo < 0)
            return includesLT;
        else if (compareTo > 0)
            return includesGT;
        return includesEQ;
    }

    public String toString()
    {
        return symbol;
    }

    private Comparison(String symbol, boolean lt, boolean eq, boolean gt)
    {
        this.symbol = symbol;
        this.includesLT = lt;
        this.includesEQ = eq;
        this.includesGT = gt;
    }

    private final String symbol;
    private final boolean includesLT;
    private final boolean includesEQ;
    private final boolean includesGT;
}
