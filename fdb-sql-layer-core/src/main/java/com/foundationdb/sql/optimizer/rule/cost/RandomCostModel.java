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
package com.foundationdb.sql.optimizer.rule.cost;

import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.sql.optimizer.plan.CostEstimate;

import java.util.Random;

public class RandomCostModel extends CostModel
{
    // TODO: Need some kind of external control of Random seed, so
    // that this is reproducible when problems are detected. Should it
    // reset every query?

    private final Random random;

    public RandomCostModel(Schema schema, TableRowCounts tableRowCounts, long seed) {
        super(schema, tableRowCounts);
        random = new Random(seed);
    }

    @Override
    protected double treeScan(int rowWidth, long nRows) {
        return 10 + nRows * .5;
    }

    public synchronized CostEstimate adjustCostEstimate(CostEstimate costEstimate) {
        return new CostEstimate(costEstimate.getRowCount(),
                                random.nextDouble() * costEstimate.getCost());
    }
}
