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
package com.foundationdb.qp.operator;

import com.foundationdb.qp.row.*;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.qp.util.HashTable;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

class HashTableLookup_Default extends Operator
{
    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, bindingsCursor);
    }

    public HashTableLookup_Default(RowType hashedRowType,
                                   List<TPreparedExpression> outerComparisonFields,
                                   int hashTableBindingPosition
    )
    {
        ArgumentValidation.notNull("hashedRowType", hashedRowType);
        ArgumentValidation.notNull("outerComparisonFields", outerComparisonFields);
        ArgumentValidation.isGTE("outerComparisonFields", outerComparisonFields.size(), 1);

        this.hashedRowType = hashedRowType;
        this.hashTableBindingPosition = hashTableBindingPosition;
        this.outerComparisonFields = outerComparisonFields;
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: HashTableLookup_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: HashTableLookup_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(Intersect_Ordered.class);

    // Object state

    private final int hashTableBindingPosition;
    private final RowType hashedRowType;
    List<TPreparedExpression> outerComparisonFields;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(hashTableBindingPosition));
        for (TPreparedExpression field : outerComparisonFields) {
            atts.put(Label.EXPRESSIONS, field.getExplainer(context));
        }
        return new CompoundExplainer(Type.HASH_JOIN, atts);
    }

    private class Execution extends LeafCursor
    {

        @Override
        public void open(){
            TAP_OPEN.in();
            try {
                super.open();
                hashTable = bindings.getHashTable(hashTableBindingPosition);
                assert (hashedRowType == hashTable.getRowType()) : hashTable;
                innerRowList = hashTable.getMatchingRows(null, evaluatableComparisonFields, bindings);
                innerRowListPosition = 0;
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                if (CURSOR_LIFECYCLE_ENABLED) {
                    CursorLifecycle.checkIdleOrActive(this);
                }
                Row next = null;
                if(innerRowListPosition < innerRowList.size()) {
                    next = innerRowList.get(innerRowListPosition++);
                    assert(next.rowType() == hashedRowType);
                }
                if (LOG_EXECUTION) {
                    LOG.debug("HashJoin: yield {}", next);
                }
                return next;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor)
        {
            super(context,  new MultipleQueryBindingsCursor(bindingsCursor));
            for (TPreparedExpression comparisonField : outerComparisonFields) {
                evaluatableComparisonFields.add(comparisonField.build());
            }
        }
        // Cursor interface
        protected HashTable hashTable;
        private List<Row> innerRowList;
        private int innerRowListPosition = 0;
        private final List<TEvaluatableExpression> evaluatableComparisonFields = new ArrayList<>();

    }
}