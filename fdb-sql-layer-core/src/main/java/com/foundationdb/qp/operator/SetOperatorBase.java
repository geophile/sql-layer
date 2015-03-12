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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.SetWrongNumColumns;
import com.foundationdb.server.error.SetWrongTypeColumns;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.Strings;

import com.google.common.base.Objects;

public abstract class SetOperatorBase extends Operator {
    SetOperatorBase (Operator left, RowType leftType, Operator right, RowType rightType, String name) {
        ArgumentValidation.notNull("left", left);
        ArgumentValidation.notNull("right", right);
        ArgumentValidation.notNull("leftRowType", leftType);
        ArgumentValidation.notNull("rightRowType", rightType);
        ArgumentValidation.notNull("name", name);
        if (leftType.nFields() != rightType.nFields()) {
            throw new SetWrongNumColumns (leftType.nFields(), rightType.nFields());
        }
        this.outputRowType = rowType(leftType, rightType);
        overlayRow = !(outputRowType == leftType);
        this.name = name;
        this.inputs = Arrays.asList(left, right);
        this.inputTypes = Arrays.asList(leftType, rightType);
        ArgumentValidation.isEQ("inputs.size", inputs.size(), "inputTypes.size", inputTypes.size());
    }

    private final RowType outputRowType;
    private boolean overlayRow = false;
    protected final List<? extends Operator> inputs;
    protected final List<? extends RowType> inputTypes;
    protected final String name;

    @Override
    public RowType rowType() {
        return outputRowType;
    }

    @Override
    public List<Operator> getInputOperators() {
        return Collections.unmodifiableList(inputs);
    }

    public List<RowType> getInputTypes() {
        return Collections.unmodifiableList(inputTypes);
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        for (Operator oper : inputs) {
            oper.findDerivedTypes(derivedTypes);
        }
    }

    public boolean useOverlayRow() {
        return overlayRow;
    }

    Operator left() {
        return inputs.get(0);
    }

    Operator right() {
        return inputs.get(1);
    }

    Operator operator(int i) {
        return inputs.get(i);
    }

    RowType inputRowType (int i) {
        return inputTypes.get(i);
    }

    int getInputSize() {
        return inputs.size();
    }

    public static RowType rowType(RowType rowType1, RowType rowType2) {
        if (rowType1 == rowType2)
            return rowType1;
        if (rowType1.nFields() != rowType2.nFields())
            throw notSameShape(rowType1, rowType2);
        return rowTypeNew(rowType1, rowType2);
    }

    private static RowType rowTypeNew(RowType rowType1, RowType rowType2) {
        TInstance[] types = new TInstance[rowType1.nFields()];
        for(int i=0; i<types.length; ++i) {
            TInstance type1 = rowType1.typeAt(i);
            TInstance type2 = rowType2.typeAt(i);
            if (Objects.equal(type1, type2))
                types[i] = type1;
            else if (type1 == null)
                types[i] = type2;
            else if (type2 == null)
                types[i] = type1;
            else if (type1.equalsExcludingNullable(type2)) {
                types[i] = type1.nullability() ? type1 : type2;
            }
            else
                throw notSameShape(rowType1, rowType2);
        }
        return rowType1.schema().newValuesType(types);
    }

    private static SetWrongTypeColumns notSameShape(RowType rt1, RowType rt2) {
        return new SetWrongTypeColumns (rt1, rt2);
    }

       @Override
    public String describePlan() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0, end = inputs.size(); i < end; ++i) {
            Operator input = inputs.get(i);
            sb.append(input);
            if (i + 1 < end)
                sb.append(Strings.nl()).append(name).append(Strings.nl());
        }//changed string to variable to type so universal
        return sb.toString();
    }

}