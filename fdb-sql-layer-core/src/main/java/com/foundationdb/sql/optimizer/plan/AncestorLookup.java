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

import java.util.List;
import java.util.ArrayList;

public class AncestorLookup extends BaseLookup
{
    private TableNode descendant;
    private List<TableNode> ancestors;

    public AncestorLookup(PlanNode input, TableNode descendant,
                          List<TableNode> ancestors,
                          List<TableSource> tables) {
        super(input, tables);
        this.descendant = descendant;
        this.ancestors = ancestors;
    }

    public AncestorLookup(PlanNode input, TableSource descendant,
                          List<TableSource> tables) {
        super(input, tables);
        this.descendant = descendant.getTable();
        this.ancestors = new ArrayList<>(tables.size());
        for (TableSource table : getTables()) {
            ancestors.add(table.getTable());
        }
    }

    public TableNode getDescendant() {
        return descendant;
    }

    public List<TableNode> getAncestors() {
        return ancestors;
    }

    @Override
    public String summaryString(SummaryConfiguration configuration) {
        return super.summaryString(configuration) + "(" + descendant + " -> " + ancestors + ")";
    }

}
