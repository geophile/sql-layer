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

/** The basic unit of costing. Keep tracks of a number of rows that
 * result and the total cost to get them there. */
public class CostEstimate implements Comparable<CostEstimate>
{
    private final long rowCount;
    private final double cost;

    public CostEstimate(long rowCount, double cost) {
        this.rowCount = rowCount;
        this.cost = cost;
    }

    public long getRowCount() {
        return rowCount;
    }
    public double getCost() {
        return cost;
    }

    public int compareTo(CostEstimate other) {
        return Double.compare(cost, other.cost);
    }

    /** Cost of one operation after the other. */
    public CostEstimate sequence(CostEstimate next) {
        return new CostEstimate(next.rowCount, cost + next.cost);
    }

    /** Cost of one operation combined with another. */
    public CostEstimate union(CostEstimate other) {
        return new CostEstimate(rowCount + other.rowCount, cost + other.cost);
    }

    /** Cost of operation repeated. */
    public CostEstimate repeat(long count) {
        return new CostEstimate(rowCount * count, cost * count);
    }

    /** Cost of one operation nested within another. */
    public CostEstimate nest(CostEstimate inner) {
        return new CostEstimate(rowCount * inner.rowCount,
                                cost + rowCount * inner.cost);
    }

    @Override
    public String toString() {
        return String.format("rows = %d, cost = %g", rowCount, cost);
    }
}
