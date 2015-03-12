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

/** A subtree from the AIS.
 * In other words, a group.
 */
public class TableTree extends TableTreeBase<TableNode> 
{
    private int nbranches;

    protected TableNode createNode(Table table) {
        return new TableNode(table, this);
    }

    /** Determine branch sharing.
     * @return the number of branches. */
    public int colorBranches() {
        if (nbranches == 0)
            nbranches = colorBranches(root, 0);
        return nbranches;
    }

    private int colorBranches(TableNode node, int nbranches) {
        long branches = 0;
        for (TableNode child = node.getFirstChild(); 
             child != null; 
             child = child.getNextSibling()) {
            nbranches = colorBranches(child, nbranches);
            // A parent is on the same branch as any child.
            branches |= child.getBranches();
        }
        if (branches == 0) {
            // The leaf of a new branch.
            branches = (1L << nbranches++);
        }
        node.setBranches(branches);
        return nbranches;
    }

}
